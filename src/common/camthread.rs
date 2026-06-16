use std::sync::{Arc, Weak};
use tokio::{
    sync::watch::{Receiver as WatchReceiver, Sender as WatchSender},
    time::{interval, sleep, timeout, Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use crate::{config::CameraConfig, utils::connect_and_login, AnyResult};
use neolink_core::bc_protocol::BcCamera;

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum NeoCamThreadState {
    Connected,
    Disconnected,
}

/// Coarse connection phase published for diagnostics/healthcheck.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum DiagPhase {
    /// Currently attempting to (re)connect and log in.
    Connecting,
    /// Connected and advertised as live.
    Connected,
    /// Lost the connection after an error and waiting to retry.
    Reconnecting,
    /// Intentionally disconnected (manual disconnect or idle/battery saving).
    Idle,
    /// Login credentials were rejected; this is fatal and will not retry.
    LoginFailed,
    /// The camera thread shut down normally.
    Stopped,
}

/// A snapshot of a camera connection's health, published over a watch channel
/// by [`NeoCamThread`] and surfaced by the healthcheck endpoint.
#[derive(Debug, Clone)]
pub(crate) struct CameraDiagnostics {
    pub(crate) phase: DiagPhase,
    /// When the current connection was established (for uptime).
    pub(crate) connected_since: Option<Instant>,
    /// Number of failed connection attempts since the last stable connection.
    pub(crate) reconnect_attempts: u32,
    /// The most recent connection error, if any.
    pub(crate) last_error: Option<String>,
}

impl Default for CameraDiagnostics {
    fn default() -> Self {
        Self {
            phase: DiagPhase::Connecting,
            connected_since: None,
            reconnect_attempts: 0,
            last_error: None,
        }
    }
}

pub(crate) struct NeoCamThread {
    state: WatchReceiver<NeoCamThreadState>,
    config: WatchReceiver<CameraConfig>,
    cancel: CancellationToken,
    camera_watch: WatchSender<Weak<BcCamera>>,
    diag: WatchSender<CameraDiagnostics>,
}

impl NeoCamThread {
    pub(crate) async fn new(
        watch_state_rx: WatchReceiver<NeoCamThreadState>,
        watch_config_rx: WatchReceiver<CameraConfig>,
        camera_watch_tx: WatchSender<Weak<BcCamera>>,
        diag_tx: WatchSender<CameraDiagnostics>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            state: watch_state_rx,
            config: watch_config_rx,
            cancel,
            camera_watch: camera_watch_tx,
            diag: diag_tx,
        }
    }
    async fn run_camera(&mut self, config: &CameraConfig) -> AnyResult<()> {
        let name = config.name.clone();
        log::trace!("Attempting connection with config: {config:?}");
        let camera = Arc::new(connect_and_login(config).await?);
        log::trace!("  - Connected");

        sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up
        if let Err(e) = update_camera_time(&camera, &name, config.update_time).await {
            log::warn!("Could not set camera time, (perhaps missing on this camera of your login in not an admin): {e:?}");
        }
        sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up

        self.camera_watch.send_replace(Arc::downgrade(&camera));
        self.diag.send_modify(|d| {
            d.phase = DiagPhase::Connected;
            d.connected_since = Some(Instant::now());
            d.last_error = None;
        });

        let cancel_check = self.cancel.clone();
        // Now we wait for a disconnect
        tokio::select! {
            _ = cancel_check.cancelled() => {
                AnyResult::Ok(())
            }
            v = camera.join() => {
                v?;
                Ok(())
            },
            v = async {
                let mut interval = interval(Duration::from_secs(5));
                let mut missed_pings = 0;
                loop {
                    interval.tick().await;
                    log::trace!("Sending ping");
                    match timeout(Duration::from_secs(5), camera.get_linktype()).await {
                        Ok(Ok(_)) => {
                            log::trace!("Ping reply");
                            missed_pings = 0;
                            continue
                        },
                        Ok(Err(neolink_core::Error::UnintelligibleReply { _reply: reply, why })) => {
                            // Camera does not support pings just wait forever
                            log::trace!("Pings not supported: {reply:?}: {why}");
                            futures::future::pending().await
                        },
                        Ok(Err(e)) => {
                            break Err(e.into());
                        },
                        Err(_) => {
                            // Timeout
                            if missed_pings < 5 {
                                missed_pings += 1;
                                continue;
                            } else {
                                log::error!("Timed out waiting for camera ping reply");
                                break Err(anyhow::anyhow!("Timed out waiting for camera ping reply"));
                            }
                        }
                    }
                }
            } => v,
        }?;

        let _ = camera.logout().await;
        let _ = camera.shutdown().await;

        Ok(())
    }

    // Will run and attempt to maintain the connection
    //
    // A watch sender is used to send the new camera
    // whenever it changes
    pub(crate) async fn run(&mut self) -> AnyResult<()> {
        const MAX_BACKOFF: Duration = Duration::from_secs(5);
        const MIN_BACKOFF: Duration = Duration::from_millis(50);

        let mut backoff = MIN_BACKOFF;

        loop {
            // While the desired state is Disconnected we are intentionally idle
            // (manual disconnect or idle/battery saving), not in an error state.
            if !matches!(*self.state.borrow(), NeoCamThreadState::Connected) {
                self.diag.send_modify(|d| {
                    d.phase = DiagPhase::Idle;
                    d.connected_since = None;
                });
            }
            self.state
                .clone()
                .wait_for(|state| matches!(state, NeoCamThreadState::Connected))
                .await?;
            self.diag.send_modify(|d| {
                if !matches!(d.phase, DiagPhase::Connected) {
                    d.phase = DiagPhase::Connecting;
                }
            });
            let mut config_rec = self.config.clone();

            let config = config_rec.borrow_and_update().clone();
            let now = Instant::now();
            let name = config.name.clone();

            let mut state = self.state.clone();

            let res = tokio::select! {
                Ok(_) = config_rec.changed() => {
                    None
                }
                Ok(_) = state.wait_for(|state| matches!(state, NeoCamThreadState::Disconnected)) => {
                    log::trace!("State changed to disconnect");
                    None
                }
                v = self.run_camera(&config) => {
                    Some(v)
                }
            };
            self.camera_watch.send_replace(Weak::new());

            if res.is_none() {
                // If None go back and reload NOW
                //
                // This occurs if there was a config change
                log::trace!("Config change or Manual disconnect");
                continue;
            }

            // Else we see what the result actually was
            let result = res.unwrap();

            if now.elapsed() > Duration::from_secs(60) {
                // Command ran long enough to be considered a success
                backoff = MIN_BACKOFF;
                self.diag.send_modify(|d| d.reconnect_attempts = 0);
            }
            if backoff > MAX_BACKOFF {
                backoff = MAX_BACKOFF;
            }

            match result {
                Ok(()) => {
                    // Normal shutdown
                    log::trace!("Normal camera shutdown");
                    self.diag.send_modify(|d| {
                        d.phase = DiagPhase::Stopped;
                        d.connected_since = None;
                    });
                    self.cancel.cancel();
                    return Ok(());
                }
                Err(e) => {
                    // An error
                    // Check if it is non-retry
                    let e_inner = e.downcast_ref::<neolink_core::Error>();
                    match e_inner {
                        Some(neolink_core::Error::CameraLoginFail) => {
                            // Fatal
                            log::error!("{name}: Login credentials were not accepted");
                            self.diag.send_modify(|d| {
                                d.phase = DiagPhase::LoginFailed;
                                d.connected_since = None;
                                d.last_error = Some(format!("{e:?}"));
                            });
                            self.cancel.cancel();
                            return Err(e);
                        }
                        _ => {
                            // Non fatal
                            log::warn!("{name}: Connection Lost: {:?}", e);
                            log::info!("{name}: Attempt reconnect in {:?}", backoff);
                            self.diag.send_modify(|d| {
                                d.phase = DiagPhase::Reconnecting;
                                d.connected_since = None;
                                d.reconnect_attempts = d.reconnect_attempts.saturating_add(1);
                                d.last_error = Some(format!("{e:?}"));
                            });
                            sleep(backoff).await;
                            backoff *= 2;
                        }
                    }
                }
            }
        }
    }
}

impl Drop for NeoCamThread {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn update_camera_time(
    camera: &BcCamera,
    name: &str,
    update_time: bool,
) -> AnyResult<()> {
    let cam_time = camera.get_time().await?;
    let mut update = false;
    if let Some(time) = cam_time {
        log::info!("{}: Camera time is already set: {}", name, time);
        if update_time {
            update = true;
        }
    } else {
        update = true;
        log::warn!("{}: Camera has no time set, Updating", name);
    }
    if update {
        use time::{OffsetDateTime, UtcOffset};

        let utc_now = OffsetDateTime::now_utc();
        let offset = UtcOffset::local_offset_at(utc_now).unwrap_or(UtcOffset::UTC);
        let local = utc_now.to_offset(offset);
        log::info!(
            "{}: Setting camera time to local time: {} {}",
            name,
            local,
            offset
        );
        // Strip the offset so the camera stores wall-clock local time as-is
        let new_time = local.replace_offset(UtcOffset::UTC);

        match camera.set_time(new_time).await {
            Ok(_) => {
                let cam_time = camera.get_time().await?;
                if let Some(time) = cam_time {
                    log::info!("{}: Camera time is now set: {}", name, time);
                }
            }
            Err(e) => {
                log::error!(
                    "{}: Camera did not accept new time (is user an admin?): Error: {:?}",
                    name,
                    e
                );
            }
        }
    }
    Ok(())
}
