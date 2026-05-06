#[cfg(feature = "zmq-publisher")]
mod implementation {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::{self, RecvTimeoutError, Sender};
    use std::sync::Arc;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use arrow::ipc::convert::{fb_to_schema, IpcSchemaEncoder};
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use zippy_core::{
        Publisher as CorePublisher, Result, SchemaRef, SegmentTableView, Source, SourceEvent,
        SourceHandle, SourceMode, SourceSink, StreamHello, ZippyError,
    };

    const PROTOCOL_VERSION: u16 = 1;
    const ZMQ_RECV_TIMEOUT_MS: i32 = 100;
    const CONTROL_REPEAT_COUNT: usize = 3;
    const HELLO_STARTUP_DELAY_MS: u64 = 200;
    const HELLO_REPEAT_INTERVAL_MS: u64 = 250;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum MessageKind {
        Hello,
        Data,
        Flush,
        Stop,
        Error,
    }

    impl MessageKind {
        fn as_str(self) -> &'static str {
            match self {
                Self::Hello => "HELLO",
                Self::Data => "DATA",
                Self::Flush => "FLUSH",
                Self::Stop => "STOP",
                Self::Error => "ERROR",
            }
        }

        fn parse(value: &[u8]) -> Result<Self> {
            match value {
                b"HELLO" => Ok(Self::Hello),
                b"DATA" => Ok(Self::Data),
                b"FLUSH" => Ok(Self::Flush),
                b"STOP" => Ok(Self::Stop),
                b"ERROR" => Ok(Self::Error),
                _ => Err(ZippyError::Io {
                    reason: format!("failed to decode zmq message kind bytes=[{:?}]", value),
                }),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct EnvelopeMeta {
        protocol_version: u16,
        stream_name: String,
        seq_no: Option<u64>,
        schema_hash: Option<String>,
        message: Option<String>,
    }

    struct Envelope {
        kind: MessageKind,
        meta: EnvelopeMeta,
        payload: Option<Vec<u8>>,
    }

    impl EnvelopeMeta {
        fn encode(&self) -> Vec<u8> {
            let mut lines = vec![
                format!("protocol_version:{}", self.protocol_version),
                format!("stream_name:{}", self.stream_name),
            ];

            if let Some(seq_no) = self.seq_no {
                lines.push(format!("seq_no:{seq_no}"));
            }
            if let Some(schema_hash) = self.schema_hash.as_ref() {
                lines.push(format!("schema_hash:{schema_hash}"));
            }
            if let Some(message) = self.message.as_ref() {
                lines.push(format!("message:{message}"));
            }

            lines.join("\n").into_bytes()
        }

        fn decode(bytes: &[u8]) -> Result<Self> {
            let text = std::str::from_utf8(bytes).map_err(|error| ZippyError::Io {
                reason: format!("failed to decode zmq envelope meta utf8 error=[{}]", error),
            })?;

            let mut protocol_version = None;
            let mut stream_name = None;
            let mut seq_no = None;
            let mut schema_hash = None;
            let mut message = None;

            for line in text.lines() {
                let (key, value) = line.split_once(':').ok_or_else(|| ZippyError::Io {
                    reason: format!("failed to parse zmq envelope meta line=[{}]", line),
                })?;

                match key {
                    "protocol_version" => {
                        protocol_version =
                            Some(value.parse::<u16>().map_err(|error| ZippyError::Io {
                                reason: format!(
                                    "failed to parse protocol version value=[{}] error=[{}]",
                                    value, error
                                ),
                            })?);
                    }
                    "stream_name" => {
                        if value.is_empty() {
                            return Err(ZippyError::InvalidConfig {
                                reason: "stream name must not be empty".to_string(),
                            });
                        }
                        stream_name = Some(value.to_string());
                    }
                    "seq_no" => {
                        seq_no = Some(value.parse::<u64>().map_err(|error| ZippyError::Io {
                            reason: format!(
                                "failed to parse sequence number value=[{}] error=[{}]",
                                value, error
                            ),
                        })?);
                    }
                    "schema_hash" => {
                        schema_hash = Some(value.to_string());
                    }
                    "message" => {
                        message = Some(value.to_string());
                    }
                    _ => {
                        return Err(ZippyError::Io {
                            reason: format!("failed to parse unknown meta key key=[{}]", key),
                        });
                    }
                }
            }

            Ok(Self {
                protocol_version: protocol_version.ok_or_else(|| ZippyError::Io {
                    reason: "failed to parse missing protocol version".to_string(),
                })?,
                stream_name: stream_name.ok_or_else(|| ZippyError::Io {
                    reason: "failed to parse missing stream name".to_string(),
                })?,
                seq_no,
                schema_hash,
                message,
            })
        }
    }

    fn encode_schema(schema: &SchemaRef) -> Vec<u8> {
        let mut encoder = IpcSchemaEncoder::new();
        encoder
            .schema_to_fb(schema.as_ref())
            .finished_data()
            .to_vec()
    }

    fn decode_schema(payload: &[u8]) -> Result<SchemaRef> {
        let schema = arrow::ipc::root_as_schema(payload).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to decode arrow ipc schema payload error=[{}]",
                error
            ),
        })?;
        Ok(Arc::new(fb_to_schema(schema)))
    }

    fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
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

        Ok(payload)
    }

    fn decode_batch(payload: &[u8]) -> Result<RecordBatch> {
        let mut reader =
            StreamReader::try_new(Cursor::new(payload), None).map_err(|error| ZippyError::Io {
                reason: format!("failed to decode arrow ipc payload error=[{}]", error),
            })?;

        reader
            .next()
            .transpose()
            .map_err(|error| ZippyError::Io {
                reason: format!("failed to read arrow ipc batch error=[{}]", error),
            })?
            .ok_or_else(|| ZippyError::Io {
                reason: "received empty arrow ipc payload".to_string(),
            })
    }

    fn decode_envelope(frames: Vec<Vec<u8>>) -> Result<Envelope> {
        if !(2..=3).contains(&frames.len()) {
            return Err(ZippyError::Io {
                reason: format!(
                    "failed to decode zmq multipart frame count count=[{}]",
                    frames.len()
                ),
            });
        }

        let mut frames = frames.into_iter();
        let kind = MessageKind::parse(&frames.next().ok_or_else(|| ZippyError::Io {
            reason: "failed to decode missing message kind frame".to_string(),
        })?)?;
        let meta = EnvelopeMeta::decode(&frames.next().ok_or_else(|| ZippyError::Io {
            reason: "failed to decode missing meta frame".to_string(),
        })?)?;
        let payload = frames.next();

        Ok(Envelope {
            kind,
            meta,
            payload,
        })
    }

    fn require_payload(kind: MessageKind, payload: Option<Vec<u8>>) -> Result<Vec<u8>> {
        payload.ok_or_else(|| ZippyError::Io {
            reason: format!("missing payload for message kind=[{}]", kind.as_str()),
        })
    }

    fn require_seq_no(meta: &EnvelopeMeta, kind: MessageKind) -> Result<u64> {
        meta.seq_no.ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!("message kind=[{}] requires seq_no", kind.as_str()),
        })
    }

    fn require_schema_hash(meta: &EnvelopeMeta, kind: MessageKind) -> Result<&str> {
        meta.schema_hash
            .as_deref()
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: format!("message kind=[{}] requires schema_hash", kind.as_str()),
            })
    }

    fn ensure_no_payload(kind: MessageKind, payload: Option<Vec<u8>>) -> Result<()> {
        match payload {
            Some(payload) if !payload.is_empty() => Err(ZippyError::Io {
                reason: format!(
                    "unexpected payload for control message kind=[{}] size=[{}]",
                    kind.as_str(),
                    payload.len()
                ),
            }),
            _ => Ok(()),
        }
    }

    fn socket_error(reason: &str, error: zmq::Error) -> ZippyError {
        ZippyError::Io {
            reason: format!("{reason} error=[{}]", error),
        }
    }

    fn socket_last_endpoint(socket: &zmq::Socket) -> Result<String> {
        socket
            .get_last_endpoint()
            .map_err(|error| socket_error("failed to get zmq last endpoint", error))?
            .map_err(|endpoint| ZippyError::Io {
                reason: format!("failed to decode zmq last endpoint bytes=[{:?}]", endpoint),
            })
    }

    type CommandAck = mpsc::Sender<Result<()>>;

    enum StreamPublisherCommand {
        PublishHello { ack: CommandAck },
        PublishData { payload: Vec<u8>, ack: CommandAck },
        PublishFlush { ack: CommandAck },
        PublishStop { ack: CommandAck },
        PublishError { message: String, ack: CommandAck },
        Shutdown,
    }

    struct StreamPublisherState {
        hello: StreamHello,
        next_hello_seq_no: u64,
        next_seq_no: u64,
        hello_repeat_interval: Duration,
        last_hello_sent_at: Option<Instant>,
    }

    impl StreamPublisherState {
        fn new(hello: StreamHello) -> Self {
            Self {
                hello,
                next_hello_seq_no: 1,
                next_seq_no: 1,
                hello_repeat_interval: Duration::from_millis(HELLO_REPEAT_INTERVAL_MS),
                last_hello_sent_at: None,
            }
        }

        fn allocate_seq_no(&mut self) -> u64 {
            let seq_no = self.next_seq_no;
            self.next_seq_no += 1;
            seq_no
        }
    }

    fn stream_send_message(
        socket: &zmq::Socket,
        hello: &StreamHello,
        kind: MessageKind,
        seq_no: Option<u64>,
        message: Option<String>,
        payload: Option<Vec<u8>>,
    ) -> Result<()> {
        let meta = EnvelopeMeta {
            protocol_version: hello.protocol_version,
            stream_name: hello.stream_name.clone(),
            seq_no,
            schema_hash: Some(hello.schema_hash.clone()),
            message,
        };

        let mut frames = vec![kind.as_str().as_bytes().to_vec(), meta.encode()];
        if let Some(payload) = payload {
            frames.push(payload);
        }

        socket
            .send_multipart(frames, 0)
            .map_err(|error| socket_error("failed to send zmq multipart payload", error))
    }

    fn stream_send_hello(socket: &zmq::Socket, state: &mut StreamPublisherState) -> Result<()> {
        let seq_no = state.next_hello_seq_no;
        stream_send_message(
            socket,
            &state.hello,
            MessageKind::Hello,
            Some(seq_no),
            None,
            Some(encode_schema(&state.hello.schema)),
        )?;
        state.next_hello_seq_no += 1;
        state.last_hello_sent_at = Some(Instant::now());
        Ok(())
    }

    fn stream_send_repeated_control(
        socket: &zmq::Socket,
        state: &mut StreamPublisherState,
        kind: MessageKind,
        seq_no: u64,
    ) -> Result<()> {
        for _ in 0..CONTROL_REPEAT_COUNT {
            stream_send_message(socket, &state.hello, kind, Some(seq_no), None, None)?;
        }
        Ok(())
    }

    fn stream_maybe_publish_hello_for_command(
        socket: &zmq::Socket,
        state: &mut StreamPublisherState,
    ) -> Result<()> {
        let should_publish = state
            .last_hello_sent_at
            .map(|instant| instant.elapsed() >= state.hello_repeat_interval)
            .unwrap_or(true);

        if should_publish {
            stream_send_hello(socket, state)?;
        }

        Ok(())
    }

    fn stream_next_hello_timeout(
        state: &StreamPublisherState,
        startup_deadline: Instant,
    ) -> Duration {
        let deadline = state
            .last_hello_sent_at
            .map(|instant| instant + state.hello_repeat_interval)
            .unwrap_or(startup_deadline);
        deadline.saturating_duration_since(Instant::now())
    }

    fn stream_publisher_worker(
        endpoint: String,
        hello: StreamHello,
        command_rx: mpsc::Receiver<StreamPublisherCommand>,
        init_tx: mpsc::Sender<Result<String>>,
    ) -> Result<()> {
        let context = zmq::Context::new();
        let socket = context
            .socket(zmq::PUB)
            .map_err(|error| socket_error("failed to create zmq pub socket", error))?;
        socket.bind(&endpoint).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to bind zmq endpoint endpoint=[{}] error=[{}]",
                endpoint, error
            ),
        })?;

        let last_endpoint = socket_last_endpoint(&socket);
        let _ = init_tx.send(last_endpoint.clone());
        let last_endpoint = last_endpoint?;

        let _ = last_endpoint;
        let mut state = StreamPublisherState::new(hello);
        let startup_deadline = Instant::now() + Duration::from_millis(HELLO_STARTUP_DELAY_MS);

        loop {
            match command_rx.recv_timeout(stream_next_hello_timeout(&state, startup_deadline)) {
                Ok(StreamPublisherCommand::PublishHello { ack }) => {
                    let result = stream_send_hello(&socket, &mut state);
                    let should_stop = result.is_err();
                    let _ = ack.send(result.clone());
                    if should_stop {
                        return result;
                    }
                }
                Ok(StreamPublisherCommand::PublishData { payload, ack }) => {
                    let result = (|| {
                        stream_maybe_publish_hello_for_command(&socket, &mut state)?;
                        let seq_no = state.allocate_seq_no();
                        stream_send_message(
                            &socket,
                            &state.hello,
                            MessageKind::Data,
                            Some(seq_no),
                            None,
                            Some(payload),
                        )
                    })();
                    let should_stop = result.is_err();
                    let _ = ack.send(result.clone());
                    if should_stop {
                        return result;
                    }
                }
                Ok(StreamPublisherCommand::PublishFlush { ack }) => {
                    let result = (|| {
                        stream_maybe_publish_hello_for_command(&socket, &mut state)?;
                        let seq_no = state.allocate_seq_no();
                        stream_send_repeated_control(
                            &socket,
                            &mut state,
                            MessageKind::Flush,
                            seq_no,
                        )
                    })();
                    let should_stop = result.is_err();
                    let _ = ack.send(result.clone());
                    if should_stop {
                        return result;
                    }
                }
                Ok(StreamPublisherCommand::PublishStop { ack }) => {
                    let result = (|| {
                        stream_maybe_publish_hello_for_command(&socket, &mut state)?;
                        let seq_no = state.allocate_seq_no();
                        stream_send_repeated_control(&socket, &mut state, MessageKind::Stop, seq_no)
                    })();
                    let _ = ack.send(result.clone());
                    return result;
                }
                Ok(StreamPublisherCommand::PublishError { message, ack }) => {
                    let result = (|| {
                        stream_maybe_publish_hello_for_command(&socket, &mut state)?;
                        let seq_no = state.allocate_seq_no();
                        stream_send_message(
                            &socket,
                            &state.hello,
                            MessageKind::Error,
                            Some(seq_no),
                            Some(message),
                            None,
                        )
                    })();
                    let should_stop = result.is_err();
                    let _ = ack.send(result.clone());
                    if should_stop {
                        return result;
                    }
                }
                Ok(StreamPublisherCommand::Shutdown) => {
                    return Ok(());
                }
                Err(RecvTimeoutError::Timeout) => {
                    stream_send_hello(&socket, &mut state)?;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Ok(());
                }
            }
        }
    }

    /// Publish bare Arrow IPC batches over a ZMQ PUB socket.
    pub struct ZmqPublisher {
        _context: zmq::Context,
        socket: zmq::Socket,
    }

    /// Subscribe to bare Arrow IPC batches from a ZMQ SUB socket.
    pub struct ZmqSubscriber {
        _context: zmq::Context,
        socket: zmq::Socket,
    }

    /// Publish multipart stream envelopes and Arrow batches over ZMQ.
    pub struct ZmqStreamPublisher {
        hello: StreamHello,
        last_endpoint: String,
        command_tx: Option<Sender<StreamPublisherCommand>>,
        worker_handle: Option<JoinHandle<Result<()>>>,
        closed: bool,
    }

    /// Consume multipart stream envelopes from ZMQ and emit `SourceEvent`s.
    pub struct ZmqSource {
        name: String,
        endpoint: String,
        expected_schema: SchemaRef,
        mode: SourceMode,
        stop_requested: Arc<AtomicBool>,
    }

    impl ZmqPublisher {
        pub fn bind(endpoint: &str) -> Result<Self> {
            let context = zmq::Context::new();
            let socket = context
                .socket(zmq::PUB)
                .map_err(|error| socket_error("failed to create zmq pub socket", error))?;
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
            socket_last_endpoint(&self.socket)
        }
    }

    impl ZmqSubscriber {
        pub fn connect(endpoint: &str, timeout_ms: i32) -> Result<Self> {
            let context = zmq::Context::new();
            let socket = context
                .socket(zmq::SUB)
                .map_err(|error| socket_error("failed to create zmq sub socket", error))?;
            socket.set_subscribe(b"").map_err(|error| ZippyError::Io {
                reason: format!("failed to subscribe zmq endpoint error=[{}]", error),
            })?;
            socket
                .set_rcvtimeo(timeout_ms)
                .map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to set zmq receive timeout timeout_ms=[{}] error=[{}]",
                        timeout_ms, error
                    ),
                })?;
            socket.connect(endpoint).map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to connect zmq endpoint endpoint=[{}] error=[{}]",
                    endpoint, error
                ),
            })?;

            Ok(Self {
                _context: context,
                socket,
            })
        }

        pub fn recv(&mut self) -> Result<RecordBatch> {
            let payload = self.socket.recv_bytes(0).map_err(|error| ZippyError::Io {
                reason: format!("failed to receive zmq payload error=[{}]", error),
            })?;
            decode_batch(&payload)
        }
    }

    impl ZmqStreamPublisher {
        pub fn bind(endpoint: &str, stream_name: &str, schema: SchemaRef) -> Result<Self> {
            let hello = StreamHello::new(stream_name, schema, PROTOCOL_VERSION)?;
            let (command_tx, command_rx) = mpsc::channel();
            let (init_tx, init_rx) = mpsc::channel();
            let endpoint = endpoint.to_string();
            let worker_hello = hello.clone();
            let worker_handle = thread::spawn(move || {
                stream_publisher_worker(endpoint, worker_hello, command_rx, init_tx)
            });

            let last_endpoint = match init_rx.recv() {
                Ok(result) => result?,
                Err(_) => {
                    return match worker_handle.join() {
                        Ok(Ok(())) => Err(ZippyError::ChannelReceive),
                        Ok(Err(error)) => Err(error),
                        Err(_) => Err(ZippyError::Io {
                            reason: "zmq stream publisher thread panicked".to_string(),
                        }),
                    };
                }
            };

            Ok(Self {
                hello,
                last_endpoint,
                command_tx: Some(command_tx),
                worker_handle: Some(worker_handle),
                closed: false,
            })
        }

        pub fn last_endpoint(&self) -> Result<String> {
            Ok(self.last_endpoint.clone())
        }

        pub fn publish_data(&mut self, batch: &RecordBatch) -> Result<()> {
            if batch.schema() != self.hello.schema {
                return Err(ZippyError::SchemaMismatch {
                    reason: "stream publisher batch schema does not match hello schema".to_string(),
                });
            }
            let payload = encode_batch(batch)?;
            self.send_command_wait(|ack| StreamPublisherCommand::PublishData { payload, ack })
        }

        pub fn publish_flush(&mut self) -> Result<()> {
            self.send_command_wait(|ack| StreamPublisherCommand::PublishFlush { ack })
        }

        pub fn publish_stop(&mut self) -> Result<()> {
            if self.closed {
                return Ok(());
            }
            let result = self.send_command_wait(|ack| StreamPublisherCommand::PublishStop { ack });
            self.closed = true;
            self.command_tx.take();
            let join_result = self.join_worker();
            result.and(join_result)
        }

        pub fn publish_error(&mut self, message: &str) -> Result<()> {
            self.send_command_wait(|ack| StreamPublisherCommand::PublishError {
                message: message.to_string(),
                ack,
            })
        }

        pub fn publish_hello(&mut self) -> Result<()> {
            self.send_command_wait(|ack| StreamPublisherCommand::PublishHello { ack })
        }

        fn send_command_wait<F>(&mut self, build_command: F) -> Result<()>
        where
            F: FnOnce(CommandAck) -> StreamPublisherCommand,
        {
            self.ensure_open()?;
            let command_tx = self
                .command_tx
                .as_ref()
                .ok_or(ZippyError::InvalidState { status: "closed" })?;
            let (ack_tx, ack_rx) = mpsc::channel();
            command_tx
                .send(build_command(ack_tx))
                .map_err(|_| ZippyError::ChannelSend)?;
            self.wait_for_ack(ack_rx)
        }

        fn wait_for_ack(&mut self, ack_rx: mpsc::Receiver<Result<()>>) -> Result<()> {
            match ack_rx.recv() {
                Ok(result) => result,
                Err(_) => match self.join_worker() {
                    Ok(()) => Err(ZippyError::ChannelReceive),
                    Err(error) => Err(error),
                },
            }
        }

        fn ensure_open(&self) -> Result<()> {
            if self.closed {
                return Err(ZippyError::InvalidState { status: "closed" });
            }
            Ok(())
        }

        fn join_worker(&mut self) -> Result<()> {
            match self.worker_handle.take() {
                Some(worker_handle) => match worker_handle.join() {
                    Ok(result) => result,
                    Err(_) => Err(ZippyError::Io {
                        reason: "zmq stream publisher thread panicked".to_string(),
                    }),
                },
                None => Ok(()),
            }
        }
    }

    impl Drop for ZmqStreamPublisher {
        fn drop(&mut self) {
            if self.worker_handle.is_none() {
                return;
            }

            if let Some(command_tx) = self.command_tx.take() {
                let _ = command_tx.send(StreamPublisherCommand::Shutdown);
            }
            let _ = self.join_worker();
        }
    }

    impl ZmqSource {
        pub fn connect(
            name: &str,
            endpoint: &str,
            expected_schema: SchemaRef,
            mode: SourceMode,
        ) -> Result<Self> {
            if name.is_empty() {
                return Err(ZippyError::InvalidConfig {
                    reason: "source name must not be empty".to_string(),
                });
            }
            if endpoint.is_empty() {
                return Err(ZippyError::InvalidConfig {
                    reason: "zmq endpoint must not be empty".to_string(),
                });
            }

            Ok(Self {
                name: name.to_string(),
                endpoint: endpoint.to_string(),
                expected_schema,
                mode,
                stop_requested: Arc::new(AtomicBool::new(false)),
            })
        }

        fn run(self, sink: Arc<dyn SourceSink>) -> Result<()> {
            let context = zmq::Context::new();
            let socket = context
                .socket(zmq::SUB)
                .map_err(|error| socket_error("failed to create zmq sub socket", error))?;
            socket.set_subscribe(b"").map_err(|error| ZippyError::Io {
                reason: format!("failed to subscribe zmq endpoint error=[{}]", error),
            })?;
            socket
                .set_rcvtimeo(ZMQ_RECV_TIMEOUT_MS)
                .map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to set zmq receive timeout timeout_ms=[{}] error=[{}]",
                        ZMQ_RECV_TIMEOUT_MS, error
                    ),
                })?;
            socket
                .connect(&self.endpoint)
                .map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to connect zmq endpoint endpoint=[{}] error=[{}]",
                        self.endpoint, error
                    ),
                })?;

            let mut accepted_hello: Option<StreamHello> = None;

            loop {
                if self.stop_requested.load(Ordering::SeqCst) {
                    if accepted_hello.is_some() {
                        sink.emit(SourceEvent::Stop)?;
                    }
                    return Ok(());
                }

                let frames = match socket.recv_multipart(0) {
                    Ok(frames) => frames,
                    Err(zmq::Error::EAGAIN) => continue,
                    Err(error) => {
                        return Err(socket_error(
                            "failed to receive zmq multipart payload",
                            error,
                        ));
                    }
                };

                let envelope = decode_envelope(frames)?;
                if envelope.meta.protocol_version != PROTOCOL_VERSION {
                    return Err(ZippyError::InvalidConfig {
                        reason: format!(
                            "unsupported stream protocol version version=[{}]",
                            envelope.meta.protocol_version
                        ),
                    });
                }

                match envelope.kind {
                    MessageKind::Hello => {
                        let meta_schema_hash =
                            require_schema_hash(&envelope.meta, MessageKind::Hello)?;
                        let schema =
                            decode_schema(&require_payload(MessageKind::Hello, envelope.payload)?)?;
                        let hello = StreamHello::new(
                            &envelope.meta.stream_name,
                            schema,
                            envelope.meta.protocol_version,
                        )?;
                        if hello.schema_hash != meta_schema_hash {
                            return Err(ZippyError::SchemaMismatch {
                                reason: "incoming hello schema_hash does not match payload"
                                    .to_string(),
                            });
                        }
                        if hello.schema != self.expected_schema {
                            return Err(ZippyError::SchemaMismatch {
                                reason: "incoming hello schema does not match expected schema"
                                    .to_string(),
                            });
                        }
                        match accepted_hello.as_ref() {
                            Some(accepted_hello)
                                if accepted_hello.protocol_version == hello.protocol_version
                                    && accepted_hello.stream_name == hello.stream_name
                                    && accepted_hello.schema == hello.schema
                                    && accepted_hello.schema_hash == hello.schema_hash => {}
                            Some(_) => {
                                return Err(ZippyError::InvalidConfig {
                                    reason: "incoming hello changed after stream lock".to_string(),
                                });
                            }
                            None => {
                                accepted_hello = Some(hello.clone());
                                sink.emit(SourceEvent::Hello(hello))?;
                            }
                        }
                    }
                    MessageKind::Data => {
                        if accepted_hello.is_none() {
                            continue;
                        }
                        validate_stream_name(&accepted_hello, &envelope.meta.stream_name)?;

                        let batch =
                            decode_batch(&require_payload(MessageKind::Data, envelope.payload)?)?;
                        if batch.schema() != self.expected_schema {
                            return Err(ZippyError::SchemaMismatch {
                                reason: "incoming batch schema does not match expected schema"
                                    .to_string(),
                            });
                        }
                        sink.emit(SourceEvent::Data(SegmentTableView::from_record_batch(
                            batch,
                        )))?;
                    }
                    MessageKind::Flush => {
                        if accepted_hello.is_none() {
                            return Err(ZippyError::InvalidConfig {
                                reason: "source hello must arrive before flush".to_string(),
                            });
                        }
                        require_seq_no(&envelope.meta, MessageKind::Flush)?;
                        validate_stream_name(&accepted_hello, &envelope.meta.stream_name)?;
                        ensure_no_payload(MessageKind::Flush, envelope.payload)?;
                        sink.emit(SourceEvent::Flush)?;
                    }
                    MessageKind::Stop => {
                        if accepted_hello.is_none() {
                            return Err(ZippyError::InvalidConfig {
                                reason: "source hello must arrive before stop".to_string(),
                            });
                        }
                        require_seq_no(&envelope.meta, MessageKind::Stop)?;
                        validate_stream_name(&accepted_hello, &envelope.meta.stream_name)?;
                        ensure_no_payload(MessageKind::Stop, envelope.payload)?;
                        sink.emit(SourceEvent::Stop)?;
                        return Ok(());
                    }
                    MessageKind::Error => {
                        validate_stream_name(&accepted_hello, &envelope.meta.stream_name)?;
                        ensure_no_payload(MessageKind::Error, envelope.payload)?;
                        sink.emit(SourceEvent::Error(envelope.meta.message.unwrap_or_else(
                            || "remote source sent empty error message".to_string(),
                        )))?;
                        return Ok(());
                    }
                }
            }
        }
    }

    fn validate_stream_name(accepted_hello: &Option<StreamHello>, stream_name: &str) -> Result<()> {
        if let Some(accepted_hello) = accepted_hello.as_ref() {
            if accepted_hello.stream_name != stream_name {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "incoming stream name does not match hello stream expected=[{}] actual=[{}]",
                        accepted_hello.stream_name, stream_name
                    ),
                });
            }
        }
        Ok(())
    }

    impl CorePublisher for ZmqPublisher {
        fn publish_table(&mut self, table: &SegmentTableView) -> Result<()> {
            let batch = table.to_record_batch()?;
            let payload = encode_batch(&batch)?;
            self.socket
                .send(payload, 0)
                .map_err(|error| socket_error("failed to send zmq payload", error))?;
            Ok(())
        }
    }

    impl CorePublisher for ZmqStreamPublisher {
        fn publish_table(&mut self, table: &SegmentTableView) -> Result<()> {
            let batch = table.to_record_batch()?;
            self.publish_data(&batch)
        }

        fn flush(&mut self) -> Result<()> {
            self.publish_flush()
        }

        fn close(&mut self) -> Result<()> {
            self.publish_stop()
        }
    }

    impl Source for ZmqSource {
        fn name(&self) -> &str {
            &self.name
        }

        fn output_schema(&self) -> SchemaRef {
            self.expected_schema.clone()
        }

        fn mode(&self) -> SourceMode {
            self.mode
        }

        fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
            let source = *self;
            let stop_requested = source.stop_requested.clone();
            let join_handle = thread::spawn(move || source.run(sink));

            Ok(SourceHandle::new_with_stop(
                join_handle,
                Box::new(move || {
                    stop_requested.store(true, Ordering::SeqCst);
                    Ok(())
                }),
            ))
        }
    }
}

#[cfg(not(feature = "zmq-publisher"))]
mod implementation {
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use zippy_core::{
        Publisher as CorePublisher, Result, SchemaRef, SegmentTableView, Source, SourceEvent,
        SourceHandle, SourceMode, SourceSink, ZippyError,
    };

    /// Publish bare Arrow IPC batches over a ZMQ PUB socket.
    pub struct ZmqPublisher;

    /// Subscribe to bare Arrow IPC batches from a ZMQ SUB socket.
    pub struct ZmqSubscriber;

    /// Publish multipart stream envelopes and Arrow batches over ZMQ.
    pub struct ZmqStreamPublisher;

    /// Consume multipart stream envelopes from ZMQ and emit `SourceEvent`s.
    pub struct ZmqSource {
        name: String,
        expected_schema: SchemaRef,
        mode: SourceMode,
    }

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

    impl ZmqSubscriber {
        pub fn connect(endpoint: &str, _timeout_ms: i32) -> Result<Self> {
            Err(ZippyError::Io {
                reason: format!(
                    "failed to connect zmq endpoint because feature is disabled endpoint=[{}]",
                    endpoint
                ),
            })
        }

        pub fn recv(&mut self) -> Result<RecordBatch> {
            Err(ZippyError::Io {
                reason: "failed to receive zmq payload because feature is disabled".to_string(),
            })
        }
    }

    impl ZmqStreamPublisher {
        pub fn bind(endpoint: &str, _stream_name: &str, _schema: SchemaRef) -> Result<Self> {
            Err(ZippyError::Io {
                reason: format!(
                    "failed to bind zmq stream publisher because feature is disabled endpoint=[{}]",
                    endpoint
                ),
            })
        }

        pub fn last_endpoint(&self) -> Result<String> {
            Err(ZippyError::Io {
                reason: "failed to inspect zmq stream endpoint because feature is disabled"
                    .to_string(),
            })
        }

        pub fn publish_data(&mut self, _batch: &RecordBatch) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream data because feature is disabled".to_string(),
            })
        }

        pub fn publish_hello(&mut self) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream hello because feature is disabled"
                    .to_string(),
            })
        }

        pub fn publish_flush(&mut self) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream flush because feature is disabled"
                    .to_string(),
            })
        }

        pub fn publish_stop(&mut self) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream stop because feature is disabled".to_string(),
            })
        }

        pub fn publish_error(&mut self, _message: &str) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream error because feature is disabled"
                    .to_string(),
            })
        }
    }

    impl ZmqSource {
        pub fn connect(
            name: &str,
            endpoint: &str,
            expected_schema: SchemaRef,
            mode: SourceMode,
        ) -> Result<Self> {
            let _ = endpoint;
            Ok(Self {
                name: name.to_string(),
                expected_schema,
                mode,
            })
        }
    }

    impl CorePublisher for ZmqPublisher {
        fn publish_table(&mut self, _table: &SegmentTableView) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq payload because feature is disabled".to_string(),
            })
        }
    }

    impl CorePublisher for ZmqStreamPublisher {
        fn publish_table(&mut self, _table: &SegmentTableView) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream data because feature is disabled".to_string(),
            })
        }

        fn flush(&mut self) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream flush because feature is disabled"
                    .to_string(),
            })
        }

        fn close(&mut self) -> Result<()> {
            Err(ZippyError::Io {
                reason: "failed to publish zmq stream stop because feature is disabled".to_string(),
            })
        }
    }

    impl Source for ZmqSource {
        fn name(&self) -> &str {
            &self.name
        }

        fn output_schema(&self) -> SchemaRef {
            self.expected_schema.clone()
        }

        fn mode(&self) -> SourceMode {
            self.mode
        }

        fn start(self: Box<Self>, _sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
            let _ = SourceEvent::Stop;
            Err(ZippyError::Io {
                reason: "failed to start zmq source because feature is disabled".to_string(),
            })
        }
    }
}

pub use implementation::{ZmqPublisher, ZmqSource, ZmqStreamPublisher, ZmqSubscriber};
