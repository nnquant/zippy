use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::bus_protocol::{
    AcquireSegmentReaderLeaseRequest, ControlEnvelopeRequest, ControlRequest, ControlResponse,
    DropTableRequest, DropTableResult, GetConfigRequest, GetSegmentDescriptorRequest,
    GetStreamRequest, HeartbeatRequest, ListStreamsRequest, PublishPersistEventRequest,
    PublishPersistedFileRequest, PublishSegmentDescriptorRequest, RegisterEngineRequest,
    RegisterProcessRequest, RegisterSinkRequest, RegisterSourceRequest, RegisterStreamRequest,
    ReleaseSegmentReaderLeaseRequest, ReplacePersistedFilesRequest, StreamInfo,
    UnregisterProcessRequest, UnregisterSourceRequest, UpdateRecordStatusRequest, WatchResource,
};
use crate::{
    canonical_schema_hash, resolve_control_endpoint, schema_metadata, send_control_line_request,
    ControlEndpoint, Result, SchemaRef, ZippyConfig, ZippyError,
};

static CONTROL_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_control_request_id() -> String {
    let request_id = CONTROL_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    format!("req_{request_id}")
}

/// Synchronous control-plane client for zippy-master.
#[derive(Debug, Clone)]
pub struct MasterClient {
    endpoint: ControlEndpoint,
    process_id: Option<String>,
    process_token: Option<String>,
    process_app: Option<String>,
    token: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentDescriptorUpdate {
    pub descriptor_generation: u64,
    pub descriptor: serde_json::Value,
}

impl MasterClient {
    pub fn connect(socket_path: impl Into<PathBuf>) -> Result<Self> {
        Self::connect_endpoint(ControlEndpoint::Unix(socket_path.into()))
    }

    pub fn connect_uri(uri: impl AsRef<str>) -> Result<Self> {
        Self::connect_endpoint(resolve_control_endpoint(uri.as_ref())?)
    }

    pub fn connect_endpoint(endpoint: ControlEndpoint) -> Result<Self> {
        Ok(Self {
            endpoint,
            process_id: None,
            process_token: None,
            process_app: None,
            token: None,
        })
    }

    pub fn endpoint(&self) -> &ControlEndpoint {
        &self.endpoint
    }

    pub fn process_id(&self) -> Option<&str> {
        self.process_id.as_deref()
    }

    pub fn process_token(&self) -> Option<&str> {
        self.process_token.as_deref()
    }

    pub fn process_app(&self) -> Option<&str> {
        self.process_app.as_deref()
    }

    pub fn set_token(&mut self, token: impl Into<String>) {
        self.token = Some(token.into());
    }

    pub fn register_process(&mut self, app: &str) -> Result<String> {
        let response =
            self.send_request(ControlRequest::RegisterProcess(RegisterProcessRequest {
                app: app.to_string(),
            }))?;

        match response {
            ControlResponse::ProcessRegistered {
                process_id,
                process_token,
            } => {
                self.process_id = Some(process_id.clone());
                self.process_token = Some(process_token);
                self.process_app = Some(app.to_string());
                Ok(process_id)
            }
            other => Err(unexpected_response("ProcessRegistered", other)),
        }
    }

    pub fn reregister_process(&mut self) -> Result<String> {
        let app = self.process_app.clone().ok_or(ZippyError::InvalidState {
            status: "master client process app missing",
        })?;
        self.process_id = None;
        self.process_token = None;
        self.register_process(&app)
    }

    pub fn heartbeat(&self) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response = self.send_request(ControlRequest::Heartbeat(HeartbeatRequest {
            process_id,
            process_token,
        }))?;

        match response {
            ControlResponse::HeartbeatAccepted { .. } => Ok(()),
            ControlResponse::ShutdownRequested { process_id, reason } => {
                Err(ZippyError::MasterShutdownRequested { process_id, reason })
            }
            other => Err(unexpected_response("HeartbeatAccepted", other)),
        }
    }

    pub fn wait_shutdown(&self, timeout: Duration) -> Result<bool> {
        match self.watch_resource(WatchResource::Shutdown, 0, timeout)? {
            None => Ok(false),
            Some(event) => match event.resource {
                WatchResource::Shutdown => {
                    let reason = event
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("master shutdown requested")
                        .to_string();
                    let process_id = self.require_process_id()?;
                    Err(ZippyError::MasterShutdownRequested { process_id, reason })
                }
                other => Err(unexpected_watch_resource("Shutdown", other)),
            },
        }
    }

    pub fn unregister_process(&mut self) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response = self.send_request(ControlRequest::UnregisterProcess(
            UnregisterProcessRequest {
                process_id: process_id.clone(),
                process_token,
            },
        ))?;

        match response {
            ControlResponse::ProcessUnregistered { .. } => {
                self.process_id = None;
                self.process_token = None;
                self.process_app = None;
                Ok(())
            }
            other => Err(unexpected_response("ProcessUnregistered", other)),
        }
    }

    pub fn register_stream(
        &mut self,
        stream_name: &str,
        schema: SchemaRef,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<()> {
        let schema_hash = canonical_schema_hash(&schema);
        let schema = schema_metadata(&schema);
        let (process_id, process_token) = self.optional_process_capability_or_token()?;
        let response =
            self.send_request(ControlRequest::RegisterStream(RegisterStreamRequest {
                process_id,
                process_token,
                token: self.token.clone(),
                stream_name: stream_name.to_string(),
                schema,
                schema_hash,
                buffer_size,
                frame_size,
            }))?;

        match response {
            ControlResponse::StreamRegistered { .. } => Ok(()),
            other => Err(unexpected_response("StreamRegistered", other)),
        }
    }

    pub fn register_source(
        &mut self,
        source_name: &str,
        source_type: &str,
        output_stream: &str,
        config: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response =
            self.send_request(ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: source_name.to_string(),
                source_type: source_type.to_string(),
                process_id,
                process_token,
                output_stream: output_stream.to_string(),
                config,
            }))?;

        match response {
            ControlResponse::SourceRegistered { .. } => Ok(()),
            other => Err(unexpected_response("SourceRegistered", other)),
        }
    }

    pub fn unregister_source(&mut self, source_name: &str) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response =
            self.send_request(ControlRequest::UnregisterSource(UnregisterSourceRequest {
                source_name: source_name.to_string(),
                process_id,
                process_token,
            }))?;

        match response {
            ControlResponse::SourceUnregistered { .. } => Ok(()),
            other => Err(unexpected_response("SourceUnregistered", other)),
        }
    }

    pub fn register_engine(
        &mut self,
        engine_name: &str,
        engine_type: &str,
        input_stream: &str,
        output_stream: &str,
        sink_names: Vec<String>,
        config: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response =
            self.send_request(ControlRequest::RegisterEngine(RegisterEngineRequest {
                engine_name: engine_name.to_string(),
                engine_type: engine_type.to_string(),
                process_id,
                process_token,
                input_stream: input_stream.to_string(),
                output_stream: output_stream.to_string(),
                sink_names,
                config,
            }))?;

        match response {
            ControlResponse::EngineRegistered { .. } => Ok(()),
            other => Err(unexpected_response("EngineRegistered", other)),
        }
    }

    pub fn register_sink(
        &mut self,
        sink_name: &str,
        sink_type: &str,
        input_stream: &str,
        config: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response = self.send_request(ControlRequest::RegisterSink(RegisterSinkRequest {
            sink_name: sink_name.to_string(),
            sink_type: sink_type.to_string(),
            process_id,
            process_token,
            input_stream: input_stream.to_string(),
            config,
        }))?;

        match response {
            ControlResponse::SinkRegistered { .. } => Ok(()),
            other => Err(unexpected_response("SinkRegistered", other)),
        }
    }

    pub fn update_status(
        &self,
        kind: &str,
        name: &str,
        status: &str,
        metrics: Option<serde_json::Value>,
    ) -> Result<()> {
        let (process_id, process_token) = self.optional_process_capability_or_token()?;
        let response =
            self.send_request(ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
                process_id,
                process_token,
                token: self.token.clone(),
                kind: kind.to_string(),
                name: name.to_string(),
                status: status.to_string(),
                metrics,
            }))?;

        match response {
            ControlResponse::StatusUpdated { .. } => Ok(()),
            other => Err(unexpected_response("StatusUpdated", other)),
        }
    }

    pub fn list_streams(&self) -> Result<Vec<StreamInfo>> {
        let response = self.send_request(ControlRequest::ListStreams(ListStreamsRequest {}))?;

        match response {
            ControlResponse::StreamsListed(response) => Ok(response.streams),
            other => Err(unexpected_response("StreamsListed", other)),
        }
    }

    pub fn get_stream(&self, stream_name: &str) -> Result<StreamInfo> {
        let response = self.send_request(ControlRequest::GetStream(GetStreamRequest {
            stream_name: stream_name.to_string(),
        }))?;

        match response {
            ControlResponse::StreamFetched(response) => Ok(response.stream),
            other => Err(unexpected_response("StreamFetched", other)),
        }
    }

    pub fn get_config(&self) -> Result<ZippyConfig> {
        let response = self.send_request(ControlRequest::GetConfig(GetConfigRequest {}))?;

        match response {
            ControlResponse::ConfigFetched { config } => {
                serde_json::from_value(config).map_err(|error| ZippyError::InvalidConfig {
                    reason: format!("failed to decode zippy config error=[{}]", error),
                })
            }
            other => Err(unexpected_response("ConfigFetched", other)),
        }
    }

    pub fn drop_table(&self, table_name: &str, drop_persisted: bool) -> Result<DropTableResult> {
        let response = self.send_request(ControlRequest::DropTable(DropTableRequest {
            process_id: self.process_id.clone(),
            process_token: self.process_token.clone(),
            token: self.token.clone(),
            table_name: table_name.to_string(),
            drop_persisted,
        }))?;

        match response {
            ControlResponse::TableDropped(result) => Ok(result),
            other => Err(unexpected_response("TableDropped", other)),
        }
    }

    pub fn publish_segment_descriptor(
        &self,
        stream_name: &str,
        descriptor: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let descriptor = self.with_stream_writer_epoch_if_missing(stream_name, descriptor)?;
        let response = self.send_request(ControlRequest::PublishSegmentDescriptor(
            PublishSegmentDescriptorRequest {
                stream_name: stream_name.to_string(),
                process_id,
                process_token,
                descriptor,
            },
        ))?;

        match response {
            ControlResponse::SegmentDescriptorPublished { .. } => Ok(()),
            other => Err(unexpected_response("SegmentDescriptorPublished", other)),
        }
    }

    pub fn publish_segment_descriptor_bytes(
        &self,
        stream_name: &str,
        descriptor_envelope: &[u8],
    ) -> Result<()> {
        let descriptor =
            serde_json::from_slice::<serde_json::Value>(descriptor_envelope).map_err(json_error)?;
        self.publish_segment_descriptor(stream_name, descriptor)
    }

    pub fn publish_persisted_file(
        &self,
        stream_name: &str,
        persisted_file: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let persisted_file =
            self.with_stream_writer_epoch_if_missing(stream_name, persisted_file)?;
        let response = self.send_request(ControlRequest::PublishPersistedFile(
            PublishPersistedFileRequest {
                stream_name: stream_name.to_string(),
                process_id,
                process_token,
                persisted_file,
            },
        ))?;

        match response {
            ControlResponse::PersistedFilePublished { .. } => Ok(()),
            other => Err(unexpected_response("PersistedFilePublished", other)),
        }
    }

    pub fn replace_persisted_files(
        &self,
        stream_name: &str,
        persisted_files: Vec<serde_json::Value>,
    ) -> Result<()> {
        let persisted_files = persisted_files
            .into_iter()
            .map(|persisted_file| {
                self.with_stream_writer_epoch_if_missing(stream_name, persisted_file)
            })
            .collect::<Result<Vec<_>>>()?;
        let response = self.send_request(ControlRequest::ReplacePersistedFiles(
            ReplacePersistedFilesRequest {
                stream_name: stream_name.to_string(),
                process_id: self.process_id.clone(),
                process_token: self.process_token.clone(),
                token: self.token.clone(),
                persisted_files,
            },
        ))?;

        match response {
            ControlResponse::PersistedFilesReplaced { .. } => Ok(()),
            other => Err(unexpected_response("PersistedFilesReplaced", other)),
        }
    }

    pub fn publish_persist_event(
        &self,
        stream_name: &str,
        persist_event: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let persist_event = self.with_stream_writer_epoch_if_missing(stream_name, persist_event)?;
        let response = self.send_request(ControlRequest::PublishPersistEvent(
            PublishPersistEventRequest {
                stream_name: stream_name.to_string(),
                process_id,
                process_token,
                persist_event,
            },
        ))?;

        match response {
            ControlResponse::PersistEventPublished { .. } => Ok(()),
            other => Err(unexpected_response("PersistEventPublished", other)),
        }
    }

    pub fn acquire_segment_reader_lease(
        &self,
        stream_name: &str,
        source_segment_id: u64,
        source_generation: u64,
    ) -> Result<String> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response = self.send_request(ControlRequest::AcquireSegmentReaderLease(
            AcquireSegmentReaderLeaseRequest {
                stream_name: stream_name.to_string(),
                process_id,
                process_token,
                source_segment_id,
                source_generation,
            },
        ))?;

        match response {
            ControlResponse::SegmentReaderLeaseAcquired { lease_id, .. } => Ok(lease_id),
            other => Err(unexpected_response("SegmentReaderLeaseAcquired", other)),
        }
    }

    pub fn release_segment_reader_lease(&self, stream_name: &str, lease_id: &str) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        self.release_segment_reader_lease_for_process(
            stream_name,
            lease_id,
            &process_id,
            process_token.as_deref(),
        )
    }

    pub fn release_segment_reader_lease_for_process(
        &self,
        stream_name: &str,
        lease_id: &str,
        process_id: &str,
        process_token: Option<&str>,
    ) -> Result<()> {
        let response = self.send_request(ControlRequest::ReleaseSegmentReaderLease(
            ReleaseSegmentReaderLeaseRequest {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
                process_token: process_token.map(ToOwned::to_owned),
                lease_id: lease_id.to_string(),
            },
        ))?;

        match response {
            ControlResponse::SegmentReaderLeaseReleased { .. } => Ok(()),
            other => Err(unexpected_response("SegmentReaderLeaseReleased", other)),
        }
    }

    pub fn get_segment_descriptor(&self, stream_name: &str) -> Result<Option<serde_json::Value>> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response = self.send_request(ControlRequest::GetSegmentDescriptor(
            GetSegmentDescriptorRequest {
                stream_name: stream_name.to_string(),
                process_id,
                process_token,
            },
        ))?;

        match response {
            ControlResponse::SegmentDescriptorFetched { descriptor, .. } => Ok(descriptor),
            other => Err(unexpected_response("SegmentDescriptorFetched", other)),
        }
    }

    pub fn wait_segment_descriptor(
        &self,
        stream_name: &str,
        after_descriptor_generation: u64,
        timeout: Duration,
    ) -> Result<Option<SegmentDescriptorUpdate>> {
        let event = self.watch_resource(
            WatchResource::SegmentDescriptor {
                stream_name: stream_name.to_string(),
            },
            after_descriptor_generation,
            timeout,
        )?;

        match event {
            None => Ok(None),
            Some(event) => match event.resource {
                WatchResource::SegmentDescriptor { .. } => {
                    let descriptor = event
                        .payload
                        .get("descriptor")
                        .cloned()
                        .filter(|value| !value.is_null());
                    Ok(descriptor.map(|descriptor| SegmentDescriptorUpdate {
                        descriptor_generation: event.revision,
                        descriptor,
                    }))
                }
                other => Err(unexpected_watch_resource("SegmentDescriptor", other)),
            },
        }
    }

    pub fn watch_resource(
        &self,
        resource: WatchResource,
        after_revision: u64,
        timeout: Duration,
    ) -> Result<Option<crate::ResourceEvent>> {
        let process_id = self.require_process_id()?;
        let process_token = self.require_process_token()?;
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        let response = self.send_request(ControlRequest::Envelope(ControlEnvelopeRequest {
            version: crate::CONTROL_PROTOCOL_VERSION,
            request_id: next_control_request_id(),
            process_id: Some(process_id),
            process_token: Some(process_token),
            token: self.token.clone(),
            verb: Some("watch".to_string()),
            resource: Some(resource),
            revision: Some(after_revision),
            timeout_ms: Some(timeout_ms),
            payload: None,
            inner: None,
        }))?;

        match response {
            ControlResponse::Envelope(envelope) => match *envelope.inner {
                ControlResponse::ResourceChanged { event } => Ok(event),
                other => Err(unexpected_response("ResourceChanged", other)),
            },
            ControlResponse::ResourceChanged { event } => Ok(event),
            other => Err(unexpected_response("ResourceChanged", other)),
        }
    }

    fn require_process_id(&self) -> Result<String> {
        self.process_id.clone().ok_or(ZippyError::InvalidState {
            status: "master client process not registered",
        })
    }

    fn require_process_token(&self) -> Result<String> {
        self.process_token.clone().ok_or(ZippyError::InvalidState {
            status: "master client process token missing",
        })
    }

    fn optional_process_capability_or_token(&self) -> Result<(Option<String>, Option<String>)> {
        match self.process_id.as_ref() {
            Some(process_id) => Ok((
                Some(process_id.clone()),
                Some(self.require_process_token()?),
            )),
            None if self.token.is_some() => Ok((None, None)),
            None => Err(ZippyError::InvalidState {
                status: "master client process not registered",
            }),
        }
    }

    fn with_stream_writer_epoch_if_missing(
        &self,
        stream_name: &str,
        mut metadata: serde_json::Value,
    ) -> Result<serde_json::Value> {
        if metadata.get("writer_epoch").is_some() {
            return Ok(metadata);
        }

        let writer_epoch = self.get_stream(stream_name)?.writer_epoch;
        if let Some(object) = metadata.as_object_mut() {
            object.insert(
                "writer_epoch".to_string(),
                serde_json::Value::from(writer_epoch),
            );
        }

        Ok(metadata)
    }

    fn send_request(&self, request: ControlRequest) -> Result<ControlResponse> {
        send_control_line_request(&self.endpoint, request)
    }
}

fn json_error(error: serde_json::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn unexpected_response(expected: &str, response: ControlResponse) -> ZippyError {
    ZippyError::Io {
        reason: format!(
            "unexpected control response expected=[{}] actual=[{}]",
            expected, response
        ),
    }
}

fn unexpected_watch_resource(expected: &str, resource: WatchResource) -> ZippyError {
    ZippyError::Io {
        reason: format!(
            "unexpected watch resource expected=[{}] actual=[{:?}]",
            expected, resource
        ),
    }
}
