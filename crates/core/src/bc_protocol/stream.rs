use super::{BcCamera, Error, Result};
use crate::{
    bc::{model::*, xml::*},
    bcmedia::model::*,
};
use futures::stream::StreamExt;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task::{self, JoinHandle};
use tokio_util::sync::CancellationToken;

/// The stream names supported by BC
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// This is the HD stream
    Main,
    /// This is the SD stream
    Sub,
    /// This stream represents a balance between SD and HD
    ///
    /// It is only available on some camera. If the camera doesn't
    /// support it the stream will be the same as the SD stream
    Extern,
}

impl std::fmt::Display for StreamKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StreamKind::Main => write!(f, "mainStream"),
            StreamKind::Sub => write!(f, "subStream"),
            StreamKind::Extern => write!(f, "externStream"),
        }
    }
}

/// A handle on currently streaming data
///
/// The data can be pulled using `get_data` which returns raw BcMedia packets
///
/// When this object is dropped the streaming is stopped
pub struct StreamData {
    handle: Option<JoinHandle<Result<()>>>,
    rx: Receiver<Result<BcMedia>>,
    abort_handle: CancellationToken,
    /// Human-readable label for logging (e.g. "mainStream", "replay").
    label: String,
}

impl StreamData {
    /// Create stream data from a task handle and receiver (e.g. for replay streams).
    pub(crate) fn from_parts(
        handle: JoinHandle<Result<()>>,
        rx: Receiver<Result<BcMedia>>,
        abort_handle: CancellationToken,
        label: impl Into<String>,
    ) -> Self {
        Self {
            handle: Some(handle),
            rx,
            abort_handle,
            label: label.into(),
        }
    }

    /// Pull data from the camera's buffer
    /// This returns raw BcMedia packets
    pub async fn get_data(&mut self) -> Result<Result<BcMedia>> {
        if let Some(handle) = self.handle.as_mut() {
            if handle.is_finished() {
                self.abort_handle.cancel();
                handle.await??;
                return Err(Error::StreamFinished);
            }
        } else {
            self.abort_handle.cancel();
            return Err(Error::StreamFinished);
        }
        match self.rx.recv().await {
            Some(data) => Ok(data),
            None => {
                self.abort_handle.cancel();
                Err(Error::StreamFinished)
            }
        }
    }

    /// Attempts to gracefully shutdown this will cancel the background task and send
    /// the Stop command to the camera
    pub async fn shutdown(&mut self) -> Result<()> {
        self.abort_handle.cancel();
        if let Some(handle) = self.handle.take() {
            // The task runs a bounded best-effort STOP_VIDEO handshake after
            // cancellation (see start_video), so it normally finishes in ~2s.
            // Cap the wait anyway: this is called while the caller holds the
            // per-stream-kind semaphore, and a wedged task must never pin that
            // permit and block the next session for this stream from starting.
            match tokio::time::timeout(tokio::time::Duration::from_secs(3), handle).await {
                Ok(join_res) => {
                    let _ = join_res?;
                }
                Err(_) => {
                    log::warn!("{}: stream task did not stop within 3s; detaching", self.label);
                }
            }
        }
        Ok(())
    }
}

impl Drop for StreamData {
    fn drop(&mut self) {
        self.abort_handle.cancel();
        match self.handle.as_ref() {
            Some(handle) if handle.is_finished() => {
                log::trace!("{}: stream task finished before drop", self.label);
            }
            Some(_) => {
                // Expected on abrupt teardown: the task is still running but has
                // been cancelled and runs a bounded (~2s) best-effort STOP, so it
                // self-terminates shortly. Detaching here is safe.
                log::trace!(
                    "{}: stream task still running after cancel; will self-terminate within ~2s",
                    self.label
                );
            }
            None => {
                // shutdown() already took and awaited the handle.
                log::trace!("{}: stream already shut down", self.label);
            }
        }
        // Drop (detach) the handle. We've already cancelled via abort_handle and
        // the task's STOP handshake is bounded, so it finishes soon on its own.
        // This avoids spawning a task that keeps the runtime alive.
        self.handle.take();
    }
}

// Second from_parts removed — already defined above

impl BcCamera {
    ///
    /// Starts the video stream
    ///
    /// The returned object manages the data stream, when it is dropped
    /// the video stop signal is sent to the camera
    ///
    /// To pull frames from the camera's buffer use `recv_data` on the returned object
    ///
    /// The buffer_size represents number of compete messages so 1 would be one complete message
    /// which may be a single audio frame or a whole video key frame. If 0 a default of 100 is used
    ///
    /// A value of scrict=true will mean that the stream will error if the underlying stream is not
    /// as expected
    pub async fn start_video(
        &self,
        stream: StreamKind,
        mut buffer_size: usize,
        strict: bool,
    ) -> Result<StreamData> {
        if let Err(e) = self.has_ability_rw("preview").await {
            if self.has_ability_ro("streamTable").await.is_err() {
                return Err(e);
            }
        }

        let connection = self.get_connection();
        let msg_num = self.new_message_num();

        let abort_handle = CancellationToken::new();
        let abort_handle_thread = abort_handle.clone();

        if buffer_size == 0 {
            buffer_size = 100;
        }
        let (tx, rx) = channel(buffer_size);
        let channel_id = self.channel_id;

        let handle = task::spawn(async move {
            // On an E1 and swann cameras:
            //  - mainStream always has a value of 0
            //  - subStream always has a value of 1
            //  - There is no externStram
            // On a B800:
            //  - mainStream is 0
            //  - subStream is 0
            //  - externStream is 0
            let stream_code = match stream {
                StreamKind::Main => 0,
                StreamKind::Sub => 1,
                StreamKind::Extern => 0,
            };

            // Theses are the numbers used with the official client
            // On an E1 and swann cameras:
            //  - mainStream always has a value of 0
            //  - subStream always has a value of 1
            //  - There is no externStram
            // On a B800:
            //  - mainStream is 0
            //  - subStream is 256
            //  - externStram is 1024
            let handle = match stream {
                StreamKind::Main => 0,
                StreamKind::Sub => 256,
                StreamKind::Extern => 1024,
            };

            let stream_name = match stream {
                StreamKind::Main => "mainStream",
                StreamKind::Sub => "subStream",
                StreamKind::Extern => "externStream",
            }
            .to_string();

            let start_video = Bc::new_from_xml(
                BcMeta {
                    msg_id: MSG_ID_VIDEO,
                    channel_id,
                    msg_num,
                    stream_type: stream_code,
                    response_code: 0,
                    class: 0x6414, // IDK why
                },
                BcXml {
                    preview: Some(Preview {
                        version: xml_ver(),
                        channel_id,
                        handle,
                        stream_type: Some(stream_name),
                    }),
                    ..Default::default()
                },
            );

            // Guard the ENTIRE setup+stream phase with the cancellation token.
            // Previously only the media-read loop below was guarded, so an abort
            // that arrived while the task was still subscribing or waiting for the
            // START_VIDEO 200 reply was ignored. On a dead/half-dead connection
            // that reply never arrives (the poller only forwards an error when the
            // socket actually yields one), so the task hung forever and the
            // detached task kept the old connection alive. Now cancellation is
            // honoured at every await point and the task always finishes promptly.
            let stream_result: Result<()> = tokio::select! {
                _ = abort_handle_thread.cancelled() => Ok(()),
                r = async {
                    let mut sub_video = connection.subscribe(MSG_ID_VIDEO, msg_num).await?;
                    sub_video.send(start_video).await?;

                    let msg = sub_video.recv().await?;
                    if let BcMeta {
                        response_code: 200, ..
                    } = msg.meta
                    {
                    } else {
                        return Err(Error::UnintelligibleReply {
                            _reply: std::sync::Arc::new(Box::new(msg)),
                            why: "The camera did not accept the stream start command.",
                        });
                    }

                    let mut media_sub = sub_video.bcmedia_stream(strict);
                    while let Some(bc_media) = media_sub.next().await {
                        // Complete interesting packet — forward it to the receiver
                        if tx.send(bc_media).await.is_err() {
                            break; // receiver gone / connection dropped
                        }
                    }
                    Ok(())
                } => r,
            };

            let stop_video = Bc::new_from_xml(
                BcMeta {
                    msg_id: MSG_ID_VIDEO_STOP,
                    channel_id,
                    msg_num,
                    stream_type: stream_code,
                    response_code: 0,
                    class: 0x6414, // IDK why
                },
                BcXml {
                    preview: Some(Preview {
                        version: xml_ver(),
                        channel_id,
                        handle,
                        stream_type: None,
                    }),
                    ..Default::default()
                },
            );

            // Best-effort STOP_VIDEO handshake.
            //
            // We only reach here during teardown (the cancellation token fired
            // or the media stream ended). The connection may already be dead
            // (camera reboot, network drop, go2rtc forcing a reconnect). On a
            // dead socket `subscribe`/`send` can block until the OS TCP timeout
            // (minutes) because the writer task's channel backs up.
            //
            // The caller holds the per-stream-kind semaphore until this task
            // finishes, so a hang here blocks the *next* session for this stream
            // from ever starting (only some streams recover after a reconnect).
            // Bound the entire handshake with one timeout so the task always
            // finishes promptly after cancellation, releasing the semaphore and
            // not leaving a detached task wedged on a dead connection. Errors are
            // ignored — we are shutting down regardless.
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
                let mut sub_stop = connection.subscribe(MSG_ID_VIDEO_STOP, msg_num).await?;
                sub_stop.send(stop_video).await?;
                loop {
                    let msg = sub_stop.recv().await?;
                    if let BcMeta {
                        response_code: 200,
                        msg_id: MSG_ID_VIDEO_STOP,
                        ..
                    } = msg.meta
                    {
                        return Ok::<(), Error>(());
                    } else if let BcMeta {
                        msg_id: MSG_ID_VIDEO_STOP,
                        ..
                    } = msg.meta
                    {
                        return Err(Error::CameraServiceUnavailable {
                            id: msg.meta.msg_id,
                            code: msg.meta.response_code,
                        });
                    }
                }
            })
            .await;

            log::trace!("{stream}: stream task exited ({stream_result:?})");
            stream_result
        });

        Ok(StreamData {
            handle: Some(handle),
            rx,
            abort_handle,
            label: stream.to_string(),
        })
    }

    /// Stop a camera from sending more stream data.
    pub async fn stop_video(&self, stream: StreamKind) -> Result<()> {
        if let Err(e) = self.has_ability_rw("preview").await {
            if self.has_ability_ro("streamTable").await.is_err() {
                return Err(e);
            }
        }
        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut sub_video = connection.subscribe(MSG_ID_VIDEO_STOP, msg_num).await?;

        // On an E1 and swann cameras:
        //  - mainStream always has a value of 0
        //  - subStream always has a value of 1
        //  - There is no externStram
        // On a B800:
        //  - mainStream is 0
        //  - subStream is 0
        //  - externStream is 0
        let stream_code = match stream {
            StreamKind::Main => 0,
            StreamKind::Sub => 1,
            StreamKind::Extern => 0,
        };

        // Theses are the numbers used with the official client
        // On an E1 and swann cameras:
        //  - mainStream always has a value of 0
        //  - subStream always has a value of 1
        //  - There is no externStram
        // On a B800:
        //  - mainStream is 0
        //  - subStream is 256
        //  - externStram is 1024
        let handle = match stream {
            StreamKind::Main => 0,
            StreamKind::Sub => 256,
            StreamKind::Extern => 1024,
        };

        let stop_video = Bc::new_from_xml(
            BcMeta {
                msg_id: MSG_ID_VIDEO_STOP,
                channel_id: self.channel_id,
                msg_num,
                stream_type: stream_code,
                response_code: 0,
                class: 0x6414, // IDK why
            },
            BcXml {
                preview: Some(Preview {
                    version: xml_ver(),
                    channel_id: self.channel_id,
                    handle,
                    stream_type: None,
                }),
                ..Default::default()
            },
        );

        sub_video.send(stop_video).await?;

        let reply = sub_video.recv().await?;
        if reply.meta.response_code != 200 {
            return Err(Error::CameraServiceUnavailable {
                id: reply.meta.msg_id,
                code: reply.meta.response_code,
            });
        }

        Ok(())
    }
}
