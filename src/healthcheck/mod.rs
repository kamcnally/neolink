//! Optional HTTP healthcheck / diagnostics endpoint.
//!
//! When a `[healthcheck]` section is present in the config, neolink starts a
//! small HTTP server (independent of the RTSP port) exposing a single endpoint:
//!
//! - `GET /health` — the status code is the verdict (`200` healthy / `503`
//!   unhealthy) and the JSON body carries the detail. `503` is returned when any
//!   *enabled* camera that should be connected is not (strict readiness). Suits
//!   both orchestrator probes (`curl -f`) and humans (`curl | jq`).
//!
//! Cameras that are intentionally disconnected (manual disconnect, or idle /
//! battery saving via `idle_disconnect`) report as `idle` and do **not** mark
//! the container unhealthy. Disabled cameras are reported but never affect
//! overall health.

use anyhow::Context;
use std::time::Duration;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::common::{DiagPhase, NeoReactor};
use crate::config::{CameraConfig, HealthcheckConfig, StreamConfig};
use crate::AnyResult;

/// Per-camera probe budget. Keeps one wedged camera from hanging the endpoint.
const CAMERA_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct AppState {
    reactor: NeoReactor,
    version: &'static str,
    profile: &'static str,
}

#[derive(Serialize)]
struct HealthResponse {
    healthy: bool,
    version: String,
    profile: String,
    cameras: Vec<CameraHealth>,
}

#[derive(Serialize)]
struct CameraHealth {
    name: String,
    enabled: bool,
    healthy: bool,
    /// `connected` | `connecting` | `reconnecting` | `idle` | `login_failed` |
    /// `stopped` | `disabled` | `unknown`
    state: &'static str,
    /// Whether the camera connection is currently live and usable.
    live: bool,
    /// The streams configured for this camera (`main`/`sub`/`extern`).
    streams: Vec<String>,
    /// Active uses (streams, motion, push notifications, ...).
    active_uses: u32,
    /// Seconds since the current connection was established, if connected.
    uptime_secs: Option<u64>,
    /// Failed connection attempts since the last stable connection.
    reconnect_attempts: u32,
    /// The most recent connection error, if any.
    last_error: Option<String>,
}

/// Names of the streams a camera is configured to expose.
fn stream_names(stream: &StreamConfig) -> Vec<String> {
    let names: &[&str] = match stream {
        StreamConfig::None => &[],
        StreamConfig::All => &["main", "sub", "extern"],
        StreamConfig::Both => &["main", "sub"],
        StreamConfig::Main => &["main"],
        StreamConfig::Sub => &["sub"],
        StreamConfig::Extern => &["extern"],
    };
    names.iter().map(|s| s.to_string()).collect()
}

/// Probe a single camera, never panicking and never hanging beyond
/// [`CAMERA_PROBE_TIMEOUT`].
async fn probe_camera(reactor: &NeoReactor, cam: &CameraConfig) -> CameraHealth {
    let streams = stream_names(&cam.stream);

    // Disabled cameras are reported but not instantiated (calling `reactor.get`
    // would spin up a connection) and never affect overall health.
    if !cam.enabled {
        return CameraHealth {
            name: cam.name.clone(),
            enabled: false,
            healthy: true,
            state: "disabled",
            live: false,
            streams,
            active_uses: 0,
            uptime_secs: None,
            reconnect_attempts: 0,
            last_error: None,
        };
    }

    match timeout(CAMERA_PROBE_TIMEOUT, gather(reactor, cam)).await {
        Ok(Ok(health)) => health,
        Ok(Err(e)) => unhealthy(cam, streams, format!("probe error: {e}")),
        Err(_) => unhealthy(cam, streams, "probe timed out".to_string()),
    }
}

/// Build an unhealthy result for a camera we could not probe.
fn unhealthy(cam: &CameraConfig, streams: Vec<String>, error: String) -> CameraHealth {
    CameraHealth {
        name: cam.name.clone(),
        enabled: true,
        healthy: false,
        state: "unknown",
        live: false,
        streams,
        active_uses: 0,
        uptime_secs: None,
        reconnect_attempts: 0,
        last_error: Some(error),
    }
}

async fn gather(reactor: &NeoReactor, cam: &CameraConfig) -> AnyResult<CameraHealth> {
    let instance = reactor.get(&cam.name).await?;
    let diag = instance.diagnostics().await?.borrow().clone();
    let live = instance.camera().borrow().upgrade().is_some();
    let active_uses = instance.use_count().await.unwrap_or(0);

    // Strict readiness: a connected camera is only healthy if it is also live
    // (actually transmitting); intentional states (idle/stopped) stay healthy.
    let (state, healthy) = match diag.phase {
        DiagPhase::Connected => ("connected", live),
        DiagPhase::Idle => ("idle", true),
        DiagPhase::Stopped => ("stopped", true),
        DiagPhase::Connecting => ("connecting", false),
        DiagPhase::Reconnecting => ("reconnecting", false),
        DiagPhase::LoginFailed => ("login_failed", false),
    };

    Ok(CameraHealth {
        name: cam.name.clone(),
        enabled: true,
        healthy,
        state,
        live,
        streams: stream_names(&cam.stream),
        active_uses,
        uptime_secs: diag.connected_since.map(|t| t.elapsed().as_secs()),
        reconnect_attempts: diag.reconnect_attempts,
        last_error: diag.last_error,
    })
}

async fn build_health(state: &AppState) -> HealthResponse {
    let mut config_ok = true;
    let cameras = match state.reactor.config().await {
        // Clone out of the watch borrow before any await points.
        Ok(rx) => {
            let cams = rx.borrow().cameras.clone();
            let mut out = Vec::with_capacity(cams.len());
            for cam in &cams {
                out.push(probe_camera(&state.reactor, cam).await);
            }
            out
        }
        Err(_) => {
            // An internal failure reading the config must not be reported as
            // healthy (an empty `cameras` would make `all()` vacuously true).
            config_ok = false;
            Vec::new()
        }
    };

    // Healthy only if we could read the config and every camera is healthy.
    let healthy = config_ok && cameras.iter().all(|c| c.healthy);

    HealthResponse {
        healthy,
        version: state.version.to_string(),
        profile: state.profile.to_string(),
        cameras,
    }
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let resp = build_health(&state).await;
    let code = if resp.healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(resp))
}

/// Run the healthcheck HTTP server until `cancel` is triggered.
pub(crate) async fn run(
    cfg: HealthcheckConfig,
    reactor: NeoReactor,
    cancel: CancellationToken,
) -> AnyResult<()> {
    let state = AppState {
        reactor,
        version: env!("NEOLINK_VERSION"),
        profile: env!("NEOLINK_PROFILE"),
    };

    let app = Router::new()
        .route("/health", get(health))
        .with_state(state);

    let addr = format!("{}:{}", cfg.bind_addr, cfg.port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind healthcheck server to {addr}"))?;
    log::info!("Healthcheck server listening on http://{addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .with_context(|| "Healthcheck server error")?;

    Ok(())
}
