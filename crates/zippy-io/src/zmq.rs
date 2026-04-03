#[cfg(feature = "zmq-publisher")]
mod implementation {
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use zippy_core::{Result, ZippyError};

    use crate::Publisher;

    pub struct ZmqPublisher {
        _context: zmq::Context,
        socket: zmq::Socket,
    }

    impl ZmqPublisher {
        pub fn bind(endpoint: &str) -> Result<Self> {
            let context = zmq::Context::new();
            let socket = context.socket(zmq::PUB).map_err(|error| ZippyError::Io {
                reason: format!("failed to create zmq pub socket error=[{}]", error),
            })?;
            socket.bind(endpoint).map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to bind zmq endpoint endpoint=[{}] error=[{}]",
                    endpoint, error
                ),
            })?;

            Ok(Self {
                _context: context,
                socket,
            })
        }

        pub fn last_endpoint(&self) -> Result<String> {
            self.socket
                .get_last_endpoint()
                .map_err(|error| ZippyError::Io {
                    reason: format!("failed to get zmq last endpoint error=[{}]", error),
                })?
                .map_err(|endpoint| ZippyError::Io {
                    reason: format!(
                        "failed to decode zmq last endpoint bytes=[{:?}]",
                        endpoint
                    ),
                })
        }
    }

    impl Publisher for ZmqPublisher {
        fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
            let mut payload = Vec::new();
            {
                let mut writer =
                    StreamWriter::try_new(&mut payload, &batch.schema()).map_err(|error| {
                        ZippyError::Io {
                            reason: format!("failed to create arrow ipc writer error=[{}]", error),
                        }
                    })?;
                writer.write(batch).map_err(|error| ZippyError::Io {
                    reason: format!("failed to write arrow ipc payload error=[{}]", error),
                })?;
                writer.finish().map_err(|error| ZippyError::Io {
                    reason: format!("failed to finish arrow ipc payload error=[{}]", error),
                })?;
            }

            self.socket.send(payload, 0).map_err(|error| ZippyError::Io {
                reason: format!("failed to send zmq payload error=[{}]", error),
            })?;
            Ok(())
        }
    }
}

#[cfg(not(feature = "zmq-publisher"))]
mod implementation {
    use arrow::record_batch::RecordBatch;
    use zippy_core::{Result, ZippyError};

    use crate::Publisher;

    pub struct ZmqPublisher;

    impl ZmqPublisher {
        pub fn bind(endpoint: &str) -> Result<Self> {
            Err(ZippyError::Io {
                reason: format!(
                    "failed to bind zmq endpoint because feature is disabled endpoint=[{}]",
                    endpoint
                ),
            })
        }

        pub fn last_endpoint(&self) -> Result<String> {
            Err(ZippyError::Io {
                reason: "failed to inspect zmq endpoint because feature is disabled".to_string(),
            })
        }
    }

    impl Publisher for ZmqPublisher {
        fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq payload because feature is disabled".to_string(),
            })
        }
    }
}

pub use implementation::ZmqPublisher;
