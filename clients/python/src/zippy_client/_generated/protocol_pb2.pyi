from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SessionRole(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SESSION_ROLE_UNSPECIFIED: _ClassVar[SessionRole]
    SESSION_ROLE_READ: _ClassVar[SessionRole]
    SESSION_ROLE_ADMINISTRATOR: _ClassVar[SessionRole]
    SESSION_ROLE_WRITER: _ClassVar[SessionRole]

class PutMutation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PUT_MUTATION_UNSPECIFIED: _ClassVar[PutMutation]
    PUT_MUTATION_APPEND: _ClassVar[PutMutation]

class PutDurability(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PUT_DURABILITY_UNSPECIFIED: _ClassVar[PutDurability]
    PUT_DURABILITY_MEMORY: _ClassVar[PutDurability]
    PUT_DURABILITY_WAL: _ClassVar[PutDurability]
    PUT_DURABILITY_PERSISTED: _ClassVar[PutDurability]

class QueryPriority(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    QUERY_PRIORITY_UNSPECIFIED: _ClassVar[QueryPriority]
    QUERY_PRIORITY_INTERACTIVE: _ClassVar[QueryPriority]
    QUERY_PRIORITY_NORMAL: _ClassVar[QueryPriority]
    QUERY_PRIORITY_MAINTENANCE: _ClassVar[QueryPriority]
SESSION_ROLE_UNSPECIFIED: SessionRole
SESSION_ROLE_READ: SessionRole
SESSION_ROLE_ADMINISTRATOR: SessionRole
SESSION_ROLE_WRITER: SessionRole
PUT_MUTATION_UNSPECIFIED: PutMutation
PUT_MUTATION_APPEND: PutMutation
PUT_DURABILITY_UNSPECIFIED: PutDurability
PUT_DURABILITY_MEMORY: PutDurability
PUT_DURABILITY_WAL: PutDurability
PUT_DURABILITY_PERSISTED: PutDurability
QUERY_PRIORITY_UNSPECIFIED: QueryPriority
QUERY_PRIORITY_INTERACTIVE: QueryPriority
QUERY_PRIORITY_NORMAL: QueryPriority
QUERY_PRIORITY_MAINTENANCE: QueryPriority

class SessionOpenRequest(_message.Message):
    __slots__ = ("protocol_version",)
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    def __init__(self, protocol_version: _Optional[int] = ...) -> None: ...

class SessionOpenResponse(_message.Message):
    __slots__ = ("protocol_version", "session_id", "bearer_token", "expires_at_ns", "role")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    BEARER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_NS_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    session_id: bytes
    bearer_token: str
    expires_at_ns: int
    role: SessionRole
    def __init__(self, protocol_version: _Optional[int] = ..., session_id: _Optional[bytes] = ..., bearer_token: _Optional[str] = ..., expires_at_ns: _Optional[int] = ..., role: _Optional[_Union[SessionRole, str]] = ...) -> None: ...

class PutCommand(_message.Message):
    __slots__ = ("protocol_version", "session_id", "put_id", "stream_name", "first_batch_sequence", "mutation", "durability", "producer_id", "connection_id", "expected_writer_epoch")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PUT_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    FIRST_BATCH_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_ID_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    session_id: bytes
    put_id: bytes
    stream_name: str
    first_batch_sequence: int
    mutation: PutMutation
    durability: PutDurability
    producer_id: bytes
    connection_id: bytes
    expected_writer_epoch: int
    def __init__(self, protocol_version: _Optional[int] = ..., session_id: _Optional[bytes] = ..., put_id: _Optional[bytes] = ..., stream_name: _Optional[str] = ..., first_batch_sequence: _Optional[int] = ..., mutation: _Optional[_Union[PutMutation, str]] = ..., durability: _Optional[_Union[PutDurability, str]] = ..., producer_id: _Optional[bytes] = ..., connection_id: _Optional[bytes] = ..., expected_writer_epoch: _Optional[int] = ...) -> None: ...

class PutBatchResult(_message.Message):
    __slots__ = ("protocol_version", "put_id", "batch_sequence", "row_count", "durability_reached", "request_id", "producer_epoch", "first_log_offset", "last_log_offset", "previous_log_offset", "committed_log_offset", "persisted_snapshot_version")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    PUT_ID_FIELD_NUMBER: _ClassVar[int]
    BATCH_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_REACHED_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    FIRST_LOG_OFFSET_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_OFFSET_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_LOG_OFFSET_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_LOG_OFFSET_FIELD_NUMBER: _ClassVar[int]
    PERSISTED_SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    put_id: bytes
    batch_sequence: int
    row_count: int
    durability_reached: PutDurability
    request_id: bytes
    producer_epoch: int
    first_log_offset: int
    last_log_offset: int
    previous_log_offset: OffsetWatermark
    committed_log_offset: OffsetWatermark
    persisted_snapshot_version: int
    def __init__(self, protocol_version: _Optional[int] = ..., put_id: _Optional[bytes] = ..., batch_sequence: _Optional[int] = ..., row_count: _Optional[int] = ..., durability_reached: _Optional[_Union[PutDurability, str]] = ..., request_id: _Optional[bytes] = ..., producer_epoch: _Optional[int] = ..., first_log_offset: _Optional[int] = ..., last_log_offset: _Optional[int] = ..., previous_log_offset: _Optional[_Union[OffsetWatermark, _Mapping]] = ..., committed_log_offset: _Optional[_Union[OffsetWatermark, _Mapping]] = ..., persisted_snapshot_version: _Optional[int] = ...) -> None: ...

class QueryCommand(_message.Message):
    __slots__ = ("protocol_version", "query_id", "session_id", "sql", "bindings", "priority", "deadline_ns", "memory_limit", "spill_limit", "result_batch_rows")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SQL_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_NS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    SPILL_LIMIT_FIELD_NUMBER: _ClassVar[int]
    RESULT_BATCH_ROWS_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    query_id: bytes
    session_id: bytes
    sql: str
    bindings: _containers.RepeatedCompositeFieldContainer[TableBinding]
    priority: QueryPriority
    deadline_ns: int
    memory_limit: ResourceLimit
    spill_limit: ResourceLimit
    result_batch_rows: int
    def __init__(self, protocol_version: _Optional[int] = ..., query_id: _Optional[bytes] = ..., session_id: _Optional[bytes] = ..., sql: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[TableBinding, _Mapping]]] = ..., priority: _Optional[_Union[QueryPriority, str]] = ..., deadline_ns: _Optional[int] = ..., memory_limit: _Optional[_Union[ResourceLimit, _Mapping]] = ..., spill_limit: _Optional[_Union[ResourceLimit, _Mapping]] = ..., result_batch_rows: _Optional[int] = ...) -> None: ...

class TableBinding(_message.Message):
    __slots__ = ("sql_name", "name", "table_id")
    SQL_NAME_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    sql_name: str
    name: TableName
    table_id: bytes
    def __init__(self, sql_name: _Optional[str] = ..., name: _Optional[_Union[TableName, _Mapping]] = ..., table_id: _Optional[bytes] = ...) -> None: ...

class TableName(_message.Message):
    __slots__ = ("namespace", "table")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    table: str
    def __init__(self, namespace: _Optional[str] = ..., table: _Optional[str] = ...) -> None: ...

class ResourceLimit(_message.Message):
    __slots__ = ("kind", "bytes")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        KIND_UNSPECIFIED: _ClassVar[ResourceLimit.Kind]
        KIND_SERVER_DEFAULT: _ClassVar[ResourceLimit.Kind]
        KIND_DISABLED: _ClassVar[ResourceLimit.Kind]
        KIND_BYTES: _ClassVar[ResourceLimit.Kind]
    KIND_UNSPECIFIED: ResourceLimit.Kind
    KIND_SERVER_DEFAULT: ResourceLimit.Kind
    KIND_DISABLED: ResourceLimit.Kind
    KIND_BYTES: ResourceLimit.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    kind: ResourceLimit.Kind
    bytes: int
    def __init__(self, kind: _Optional[_Union[ResourceLimit.Kind, str]] = ..., bytes: _Optional[int] = ...) -> None: ...

class QueryTicket(_message.Message):
    __slots__ = ("protocol_version", "server_instance_id", "ticket_id", "query_id", "generation", "expires_at_ns")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SERVER_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    TICKET_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_NS_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    server_instance_id: bytes
    ticket_id: bytes
    query_id: bytes
    generation: int
    expires_at_ns: int
    def __init__(self, protocol_version: _Optional[int] = ..., server_instance_id: _Optional[bytes] = ..., ticket_id: _Optional[bytes] = ..., query_id: _Optional[bytes] = ..., generation: _Optional[int] = ..., expires_at_ns: _Optional[int] = ...) -> None: ...

class MetadataTicket(_message.Message):
    __slots__ = ("protocol_version", "server_instance_id", "ticket_id", "catalog_revision", "expires_at_ns")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SERVER_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    TICKET_ID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_REVISION_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_NS_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    server_instance_id: bytes
    ticket_id: bytes
    catalog_revision: int
    expires_at_ns: int
    def __init__(self, protocol_version: _Optional[int] = ..., server_instance_id: _Optional[bytes] = ..., ticket_id: _Optional[bytes] = ..., catalog_revision: _Optional[int] = ..., expires_at_ns: _Optional[int] = ...) -> None: ...

class MetadataInfo(_message.Message):
    __slots__ = ("protocol_version", "catalog_revision", "expires_at_ns")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    CATALOG_REVISION_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_NS_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    catalog_revision: int
    expires_at_ns: int
    def __init__(self, protocol_version: _Optional[int] = ..., catalog_revision: _Optional[int] = ..., expires_at_ns: _Optional[int] = ...) -> None: ...

class QueryInfo(_message.Message):
    __slots__ = ("protocol_version", "query_id", "generation", "catalog_revision", "planned_at_ns", "expires_at_ns", "snapshots")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    CATALOG_REVISION_FIELD_NUMBER: _ClassVar[int]
    PLANNED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_NS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    query_id: bytes
    generation: int
    catalog_revision: int
    planned_at_ns: int
    expires_at_ns: int
    snapshots: _containers.RepeatedCompositeFieldContainer[QueryTableVersion]
    def __init__(self, protocol_version: _Optional[int] = ..., query_id: _Optional[bytes] = ..., generation: _Optional[int] = ..., catalog_revision: _Optional[int] = ..., planned_at_ns: _Optional[int] = ..., expires_at_ns: _Optional[int] = ..., snapshots: _Optional[_Iterable[_Union[QueryTableVersion, _Mapping]]] = ...) -> None: ...

class QueryTableVersion(_message.Message):
    __slots__ = ("table_id", "snapshot_version", "schema_version")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    table_id: bytes
    snapshot_version: int
    schema_version: int
    def __init__(self, table_id: _Optional[bytes] = ..., snapshot_version: _Optional[int] = ..., schema_version: _Optional[int] = ...) -> None: ...

class ErrorEnvelope(_message.Message):
    __slots__ = ("protocol_version", "code", "message", "retryable", "request_id", "query_id", "table_id", "revision_conflict", "producer_epoch", "sequence_gap", "schema_version", "offset_expired", "resource_limit", "data_corruption", "unsupported_plan")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_CONFLICT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_GAP_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    OFFSET_EXPIRED_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    DATA_CORRUPTION_FIELD_NUMBER: _ClassVar[int]
    UNSUPPORTED_PLAN_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    code: str
    message: str
    retryable: bool
    request_id: bytes
    query_id: bytes
    table_id: bytes
    revision_conflict: RevisionConflict
    producer_epoch: ExpectedActual
    sequence_gap: ExpectedActual
    schema_version: ExpectedActual
    offset_expired: OffsetExpired
    resource_limit: ResourceLimitExceeded
    data_corruption: DataCorruption
    unsupported_plan: UnsupportedPlan
    def __init__(self, protocol_version: _Optional[int] = ..., code: _Optional[str] = ..., message: _Optional[str] = ..., retryable: _Optional[bool] = ..., request_id: _Optional[bytes] = ..., query_id: _Optional[bytes] = ..., table_id: _Optional[bytes] = ..., revision_conflict: _Optional[_Union[RevisionConflict, _Mapping]] = ..., producer_epoch: _Optional[_Union[ExpectedActual, _Mapping]] = ..., sequence_gap: _Optional[_Union[ExpectedActual, _Mapping]] = ..., schema_version: _Optional[_Union[ExpectedActual, _Mapping]] = ..., offset_expired: _Optional[_Union[OffsetExpired, _Mapping]] = ..., resource_limit: _Optional[_Union[ResourceLimitExceeded, _Mapping]] = ..., data_corruption: _Optional[_Union[DataCorruption, _Mapping]] = ..., unsupported_plan: _Optional[_Union[UnsupportedPlan, _Mapping]] = ...) -> None: ...

class RevisionConflict(_message.Message):
    __slots__ = ("expected", "actual")
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    expected: int
    actual: int
    def __init__(self, expected: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class ExpectedActual(_message.Message):
    __slots__ = ("expected", "actual")
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    expected: int
    actual: int
    def __init__(self, expected: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class OffsetExpired(_message.Message):
    __slots__ = ("requested", "earliest", "latest", "snapshot_recovery_available")
    REQUESTED_FIELD_NUMBER: _ClassVar[int]
    EARLIEST_FIELD_NUMBER: _ClassVar[int]
    LATEST_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_RECOVERY_AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    requested: OffsetWatermark
    earliest: OffsetWatermark
    latest: OffsetWatermark
    snapshot_recovery_available: bool
    def __init__(self, requested: _Optional[_Union[OffsetWatermark, _Mapping]] = ..., earliest: _Optional[_Union[OffsetWatermark, _Mapping]] = ..., latest: _Optional[_Union[OffsetWatermark, _Mapping]] = ..., snapshot_recovery_available: _Optional[bool] = ...) -> None: ...

class OffsetWatermark(_message.Message):
    __slots__ = ("state", "offset")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATE_UNSPECIFIED: _ClassVar[OffsetWatermark.State]
        STATE_EMPTY: _ClassVar[OffsetWatermark.State]
        STATE_KNOWN: _ClassVar[OffsetWatermark.State]
        STATE_LEGACY_UNKNOWN: _ClassVar[OffsetWatermark.State]
    STATE_UNSPECIFIED: OffsetWatermark.State
    STATE_EMPTY: OffsetWatermark.State
    STATE_KNOWN: OffsetWatermark.State
    STATE_LEGACY_UNKNOWN: OffsetWatermark.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    state: OffsetWatermark.State
    offset: int
    def __init__(self, state: _Optional[_Union[OffsetWatermark.State, str]] = ..., offset: _Optional[int] = ...) -> None: ...

class ResourceLimitExceeded(_message.Message):
    __slots__ = ("resource", "limit", "requested")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_FIELD_NUMBER: _ClassVar[int]
    resource: str
    limit: ResourceLimit
    requested: int
    def __init__(self, resource: _Optional[str] = ..., limit: _Optional[_Union[ResourceLimit, _Mapping]] = ..., requested: _Optional[int] = ...) -> None: ...

class DataCorruption(_message.Message):
    __slots__ = ("object", "expected_checksum", "actual_checksum")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    object: str
    expected_checksum: str
    actual_checksum: str
    def __init__(self, object: _Optional[str] = ..., expected_checksum: _Optional[str] = ..., actual_checksum: _Optional[str] = ...) -> None: ...

class UnsupportedPlan(_message.Message):
    __slots__ = ("operator",)
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    operator: str
    def __init__(self, operator: _Optional[str] = ...) -> None: ...

class CapabilitiesRequest(_message.Message):
    __slots__ = ("protocol_version",)
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    def __init__(self, protocol_version: _Optional[int] = ...) -> None: ...

class CapabilitiesResponse(_message.Message):
    __slots__ = ("protocol_version", "server_version", "capabilities", "max_message_bytes", "max_pending_tickets", "authentication_required", "session_ttl_ms", "administrator_authentication_enabled", "metadata_max_rows", "metadata_max_bytes", "writer_authentication_enabled", "ingest_enabled", "ingest_max_concurrent_puts", "ingest_max_batches_per_put", "ingest_max_rows_per_batch", "ingest_max_rows_per_put", "ingest_max_batch_bytes", "ingest_max_bytes_per_put")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SERVER_VERSION_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    MAX_MESSAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_PENDING_TICKETS_FIELD_NUMBER: _ClassVar[int]
    AUTHENTICATION_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    SESSION_TTL_MS_FIELD_NUMBER: _ClassVar[int]
    ADMINISTRATOR_AUTHENTICATION_ENABLED_FIELD_NUMBER: _ClassVar[int]
    METADATA_MAX_ROWS_FIELD_NUMBER: _ClassVar[int]
    METADATA_MAX_BYTES_FIELD_NUMBER: _ClassVar[int]
    WRITER_AUTHENTICATION_ENABLED_FIELD_NUMBER: _ClassVar[int]
    INGEST_ENABLED_FIELD_NUMBER: _ClassVar[int]
    INGEST_MAX_CONCURRENT_PUTS_FIELD_NUMBER: _ClassVar[int]
    INGEST_MAX_BATCHES_PER_PUT_FIELD_NUMBER: _ClassVar[int]
    INGEST_MAX_ROWS_PER_BATCH_FIELD_NUMBER: _ClassVar[int]
    INGEST_MAX_ROWS_PER_PUT_FIELD_NUMBER: _ClassVar[int]
    INGEST_MAX_BATCH_BYTES_FIELD_NUMBER: _ClassVar[int]
    INGEST_MAX_BYTES_PER_PUT_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    server_version: str
    capabilities: _containers.RepeatedScalarFieldContainer[str]
    max_message_bytes: int
    max_pending_tickets: int
    authentication_required: bool
    session_ttl_ms: int
    administrator_authentication_enabled: bool
    metadata_max_rows: int
    metadata_max_bytes: int
    writer_authentication_enabled: bool
    ingest_enabled: bool
    ingest_max_concurrent_puts: int
    ingest_max_batches_per_put: int
    ingest_max_rows_per_batch: int
    ingest_max_rows_per_put: int
    ingest_max_batch_bytes: int
    ingest_max_bytes_per_put: int
    def __init__(self, protocol_version: _Optional[int] = ..., server_version: _Optional[str] = ..., capabilities: _Optional[_Iterable[str]] = ..., max_message_bytes: _Optional[int] = ..., max_pending_tickets: _Optional[int] = ..., authentication_required: _Optional[bool] = ..., session_ttl_ms: _Optional[int] = ..., administrator_authentication_enabled: _Optional[bool] = ..., metadata_max_rows: _Optional[int] = ..., metadata_max_bytes: _Optional[int] = ..., writer_authentication_enabled: _Optional[bool] = ..., ingest_enabled: _Optional[bool] = ..., ingest_max_concurrent_puts: _Optional[int] = ..., ingest_max_batches_per_put: _Optional[int] = ..., ingest_max_rows_per_batch: _Optional[int] = ..., ingest_max_rows_per_put: _Optional[int] = ..., ingest_max_batch_bytes: _Optional[int] = ..., ingest_max_bytes_per_put: _Optional[int] = ...) -> None: ...
