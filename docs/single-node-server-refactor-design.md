# Zippy 单机数据库服务器完整重构方案

日期：2026-07-13

状态：实施中（Phase 0/1 已完成，Phase 2 P2.2 与 Phase 3 持久化查询租约基础设施进行中）

适用范围：Zippy 单机部署模式。本文不设计集群、Raft、多副本、跨节点查询和分布式事务。

关联文档：

- [`adr/README.md`](adr/README.md)
- [`control_plane_lifecycle_runbook.md`](control_plane_lifecycle_runbook.md)
- [`cross-platform-remote-gateway-design.md`](cross-platform-remote-gateway-design.md)
- [`keyvalue-table-changelog-refactor-design.md`](keyvalue-table-changelog-refactor-design.md)
- [`frame-table.md`](frame-table.md)
- [`stream-table-sharding.md`](stream-table-sharding.md)
- [`performance_audit_fix_status_2026-05-12.md`](performance_audit_fix_status_2026-05-12.md)

---

## 1. 执行摘要

当前 Zippy 的 `master + gateway` 架构适合早期单机实时数据总线：

```text
master
  -> process/source/engine/stream 注册
  -> writer fencing 和 reader lease
  -> persisted file metadata
  -> JSON registry snapshot

gateway
  -> remote write
  -> remote subscribe
  -> remote collect
  -> 部分查询执行
```

它已经具备 mmap segment、Arrow/Parquet、writer epoch、snapshot lease、远端流式查询和
有界资源预算等良好基础，但尚不具备成熟单机数据库服务器应有的四个核心边界：

1. 可事务恢复、可增量维护的 Catalog。
2. 完全由服务器拥有的查询计划和查询生命周期。
3. 可幂等重试、可选择持久级别的写入协议。
4. 带稳定 offset、retention 和 replay 的流订阅协议。

本重构的最终产品形态是一个统一的 `zippy-server`：

```text
一个安装包
一个服务入口
一个数据目录
一套统一配置
一个明确的恢复协议
```

内部不再以 `master` 和 `gateway` 划分数据库职责，而是拆成：

```text
FrontendService
CatalogService
StorageEngine
IngestService
SubscriptionService
QueryService
MaintenanceService
```

默认部署仍然简单：用户只运行 `zippy server run`。除 maintenance worker 外，第一阶段不要求
拆成多个常驻 OS 进程。内部必须使用明确接口、独立线程池和独立资源预算，避免重新形成一个
大型共享状态对象。

核心技术决策：

| 领域 | 决策 |
| --- | --- |
| Catalog | SQLite WAL + schema migration + 类型化 CatalogCommand |
| 数据文件 | immutable Parquet + versioned manifest |
| 热数据 | 保留 active/sealed mmap segment |
| 写入恢复 | per-table ingest WAL + producer sequence |
| 查询引擎 | Apache DataFusion 扩展，不再继续手写通用关系算子 |
| 远端协议 | Arrow Flight/Flight SQL；旧 Gateway 协议作为迁移适配层 |
| 本地协议 | Unix socket control + mmap fast path |
| 维护任务 | 独立 `zippy-maintenance` 子进程 |
| 运行监督 | 继续使用 `zippy-rspm`，但不引入集群 agent 语义 |

重构必须增量推进，不采用一次性替换。每一个阶段都要满足：

```text
旧 API 仍可工作
新旧结果可对比
新后端可关闭
已有 Parquet 不被覆盖
失败后能回滚到上一阶段
```

---

## 2. 单机系统边界

### 2.1 明确目标

- 单机上提供统一的实时写入、订阅、历史查询和当前状态查询。
- 进程崩溃或机器重启后，Catalog 和持久数据恢复到一个确定状态。
- 网络响应丢失后，客户端重试不会重复提交同一批数据。
- 查询计划、快照和执行位置由服务器决定，不依赖 Python 客户端版本。
- 查询、订阅和 maintenance 具有明确资源预算，不能拖垮 ingest/control 热路径。
- 本地行情和因子链路继续使用 mmap/segment，不为数据库化牺牲低延迟优势。
- append、frame、key-value 三种表语义在 Catalog、写入、订阅和查询中保持一致。
- 新旧数据格式和 API 有可执行的迁移路径。

### 2.2 非目标

- 不实现 Raft 或多 master。
- 不实现跨机器副本和自动修复。
- 不实现跨节点查询、exchange 和分布式 shuffle。
- 不实现分布式 2PC。
- 不承诺第一阶段支持多表 ACID 事务。
- 不把 DataFusion 放入逐 tick callback 热路径。
- 不要求用户直接理解 WAL、manifest、segment descriptor 或 snapshot lease。
- 不在第一阶段删除旧 Gateway 协议和 `MasterClient` API。

### 2.3 成熟单机服务器的最低定义

达到以下条件后，Zippy 才能被视为成熟单机数据库服务器：

1. 任意 metadata mutation 中断后均能确定恢复，不依赖进程内存。
2. Catalog 指向的每个数据文件都存在且通过校验；未引用文件可以安全回收。
3. 同一幂等写入请求重复提交不会产生重复逻辑行。
4. 查询读取固定 snapshot，不会同时看到 compaction 前后两套文件。
5. 查询可以取消、超时、限内存和 spill。
6. 断线订阅可以从持久 offset 恢复。
7. Compaction、GC 和备份不会阻塞 ingest/control。
8. 本地与远端查询对同一 snapshot 返回一致结果。
9. 数据目录有格式版本、迁移工具、备份和完整性检查。
10. 所有安全敏感 token 和凭据不写入日志。

---

## 3. 当前实现中应保留与替换的部分

### 3.1 保留

| 现有能力 | 后续归属 |
| --- | --- |
| active/sealed mmap segment | `StorageEngine::LiveStore` |
| `writer_epoch` fencing | `IngestService` 和 Catalog producer epoch |
| reader lease | snapshot/file lease |
| Arrow schema 和 RecordBatch | 全系统统一内存格式 |
| Parquet persistence | immutable persisted data file |
| row-group statistics pruning | DataFusion/Zippy data source pruning |
| append/frame/key-value table kinds | Catalog 的强类型 table kind |
| query bytes budget | 全局 `ResourceManager` 查询内存池 |
| `subscribe()`/`subscribe_frames()` | SubscriptionService 的表语义适配 |
| `zippy-rspm` | server/maintenance 进程监督 |

### 3.2 替换

| 当前实现 | 替换方案 |
| --- | --- |
| `Registry` 中的全局 `Mutex<BTreeMap<...>>` | SQLite Catalog + Catalog actor |
| `master-registry.json` 全量快照 | SQLite WAL/checkpoint + 版本迁移 |
| `StreamRecord.persisted_files: Vec<Value>` | 规范化 `data_files` 和 snapshot visibility |
| 内存 `envelope_responses` 幂等缓存 | 有界持久 `idempotent_requests` |
| master 内 compaction 线程 | `zippy-maintenance` worker |
| Gateway 内手写通用查询算子 | DataFusion QueryService |
| Python remote/residual plan split | 服务器端完整逻辑/物理计划 |
| 自定义 query JSON framing | Arrow Flight/Flight SQL |
| 无 producer sequence 的 `write_batch` | producer epoch + batch sequence |
| best-effort reconnect | durable offset + replay |

### 3.3 兼容保留但逐步废弃

- `zippy master run`
- `MasterClient`
- `GatewayServer`
- 旧 `ControlRequest/ControlResponse`
- 旧 remote Gateway frame protocol
- `master-registry.json`

这些入口在迁移期转发到新服务，不再作为新内核的事实来源。

---

## 4. 目标总体架构

```text
Python / Rust / C++ / Windows clients
                  |
                  | Unix socket / Arrow Flight / legacy adapter
                  v
+------------------------------------------------------------------+
|                         zippy-server                             |
|                                                                  |
|  +--------------------- FrontendService ----------------------+  |
|  | auth | session | protocol | rate limit | request routing  |  |
|  +---------------------------+--------------------------------+  |
|                              |                                   |
|        +---------------------+------------------------+          |
|        |                     |                        |          |
|        v                     v                        v          |
|  CatalogService        QueryService             IngestService   |
|  SQLite actor          DataFusion                table actors    |
|  schema/version        query registry            producer seq    |
|  manifest/jobs         memory/spill              ingest WAL      |
|        |                     |                        |          |
|        +---------------------+------------------------+          |
|                              |                                   |
|                              v                                   |
|                        StorageEngine                             |
|             active/sealed segment + immutable Parquet           |
|                              |                                   |
|                              v                                   |
|                    SubscriptionService                          |
|                    offset / replay / callbacks                  |
+------------------------------------------------------------------+
                  |
                  | maintenance job protocol
                  v
       +-------------------------------+
       | zippy-maintenance             |
       | compaction / GC / verify      |
       | statistics / backup           |
       +-------------------------------+
```

### 4.1 产品进程模型

默认进程：

```text
zippy-server
zippy-maintenance  # server 按需拉起或由 rspm 管理
```

第一阶段 QueryService 作为 `zippy-server` 内的独立 runtime 和内存池运行。只有在以下情况出现
真实证据后，才增加隔离 query worker 进程：

- 第三方 UDF 可以崩溃进程。
- 查询 OOM 无法被内存池可靠阻止。
- 查询 allocator fragmentation 明显影响 ingest 长跑 RSS。
- 独立 worker 的 mmap attach 成本可接受。

单机定位意味着不需要为了“看起来像分布式数据库”而制造 controller/data/compute node。

### 4.2 内部运行时隔离

| Runtime | 默认职责 | 不允许执行 |
| --- | --- | --- |
| `control_runtime` | Catalog RPC、health、shutdown | Parquet scan、排序、compaction |
| `ingest_runtime` | table actor、WAL group commit、segment commit | 用户查询 |
| `query_runtime` | DataFusion operator、result stream | Catalog 写事务 |
| `io_runtime` | Parquet/manifest/WAL 文件 IO | Python callback |
| `subscription_runtime` | offset 分发、client backpressure | 全表 collect |
| maintenance process | compaction、GC、verify、backup | active segment 写入 |

所有 runtime 共享一个只负责预算协调的 `ResourceManager`，不共享任意可变业务 map。

---

## 5. CatalogService 设计

### 5.1 为什么使用 SQLite

Catalog 是低吞吐、高一致性、强审计的数据，不属于 tick 热路径。单机系统直接使用 SQLite
可以获得：

- 原子事务和 crash recovery。
- WAL 模式下的单写多读。
- 稳定的 schema migration。
- 索引、唯一约束和外键。
- 在线备份和完整性检查能力。
- 避免维护自研 WAL、checkpoint 和 BTree 实现。

推荐初始化参数：

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA wal_autocheckpoint = 0;
```

Checkpoint 由 CatalogService 在低负载或 WAL 达到阈值时显式执行。Catalog 文件和 WAL 必须位于
本地文件系统，不支持放在 NFS/SMB 网络目录。

### 5.2 Catalog 数据模型

第一版逻辑 schema 至少包括以下实体。下列 SQL 是字段关系草图，不是当前 migration 的逐字 DDL；
physical v2 已将稳定 ID 编码为非零 BLOB16、`u64`/UTC 纳秒编码为 big-endian BLOB8，并将
`Option<u64>` 与 watermark 编码为带显式状态 tag 的 canonical BLOB9，避免 SQLite signed INTEGER
和通用 JSON number 丢失 `u64::MAX` 或混淆 empty/legacy-unknown。

```sql
CREATE TABLE catalog_meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
);

CREATE TABLE namespaces (
    namespace_id BLOB PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at_ns INTEGER NOT NULL
);

CREATE TABLE tables (
    table_id BLOB PRIMARY KEY,
    namespace_id BLOB NOT NULL,
    name TEXT NOT NULL,
    table_kind TEXT NOT NULL,
    current_schema_version INTEGER NOT NULL,
    current_snapshot_version INTEGER NOT NULL,
    writer_epoch INTEGER NOT NULL,
    state TEXT NOT NULL,
    config_json TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    UNIQUE(namespace_id, name),
    FOREIGN KEY(namespace_id) REFERENCES namespaces(namespace_id)
);

CREATE TABLE table_schemas (
    table_id BLOB NOT NULL,
    schema_version INTEGER NOT NULL,
    schema_ipc BLOB NOT NULL,
    schema_hash TEXT NOT NULL,
    compatibility TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    PRIMARY KEY(table_id, schema_version),
    UNIQUE(table_id, schema_hash),
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
);

CREATE TABLE table_snapshots (
    table_id BLOB NOT NULL,
    snapshot_version INTEGER NOT NULL,
    parent_snapshot_version INTEGER,
    operation TEXT NOT NULL,
    commit_lsn INTEGER NOT NULL,
    committed_log_offset INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    summary_json TEXT NOT NULL,
    PRIMARY KEY(table_id, snapshot_version),
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
);

CREATE TABLE data_files (
    file_id BLOB PRIMARY KEY,
    table_id BLOB NOT NULL,
    schema_version INTEGER NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    file_size_bytes INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    min_log_offset INTEGER NOT NULL,
    max_log_offset INTEGER NOT NULL,
    partition_json TEXT NOT NULL,
    statistics_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    legacy_ordering INTEGER NOT NULL DEFAULT 0,
    added_snapshot_version INTEGER NOT NULL,
    removed_snapshot_version INTEGER,
    state TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
);

CREATE INDEX data_files_visible_by_table
ON data_files(table_id, added_snapshot_version, removed_snapshot_version);

CREATE INDEX data_files_offsets
ON data_files(table_id, min_log_offset, max_log_offset);

CREATE TABLE data_file_sources (
    file_id BLOB NOT NULL,
    source_segment_id INTEGER NOT NULL,
    source_generation INTEGER NOT NULL,
    min_log_offset INTEGER NOT NULL,
    max_log_offset INTEGER NOT NULL,
    PRIMARY KEY(file_id, source_segment_id, source_generation),
    FOREIGN KEY(file_id) REFERENCES data_files(file_id)
);

CREATE TABLE producers (
    producer_id BLOB PRIMARY KEY,
    table_id BLOB NOT NULL,
    producer_epoch INTEGER NOT NULL,
    last_batch_sequence INTEGER NOT NULL,
    last_committed_log_offset INTEGER NOT NULL,
    state TEXT NOT NULL,
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
);

CREATE TABLE subscriptions (
    subscription_id BLOB PRIMARY KEY,
    consumer_group TEXT,
    table_id BLOB NOT NULL,
    committed_log_offset INTEGER NOT NULL,
    config_json TEXT NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
);

CREATE TABLE snapshot_leases (
    lease_id BLOB PRIMARY KEY,
    table_id BLOB NOT NULL,
    snapshot_version INTEGER NOT NULL,
    owner_id TEXT NOT NULL,
    expires_at_ns INTEGER NOT NULL,
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
);

CREATE TABLE maintenance_jobs (
    job_id BLOB PRIMARY KEY,
    job_type TEXT NOT NULL,
    table_id BLOB,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    lease_owner TEXT,
    lease_expires_at_ns INTEGER,
    input_json TEXT NOT NULL,
    progress_json TEXT NOT NULL,
    error_text TEXT,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL
);

CREATE TABLE idempotent_requests (
    request_id BLOB PRIMARY KEY,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_blob BLOB NOT NULL,
    committed_at_ns INTEGER NOT NULL,
    expires_at_ns INTEGER NOT NULL
);
```

实际 migration 可以拆表，但上述字段是第一版语义下限。

普通查询的短生命周期 snapshot lease 不逐条写入 SQLite，否则 query admission 会退化为
Catalog 写热路径。它们保存在 `zippy-server` 的内存 `LeaseRegistry` 中，并由 CatalogService 在
maintenance 删除文件前统一检查。`snapshot_leases` 只用于需要跨进程或跨重启保持的长任务，
例如 backup、offline export 和显式长生命周期 snapshot。

### 5.3 文件可见性模型

文件在 snapshot `S` 可见，当且仅当：

```text
added_snapshot_version <= S
AND
(removed_snapshot_version IS NULL OR removed_snapshot_version > S)
AND
state = 'committed'
```

Compaction 不修改旧文件记录，而是在同一事务中：

```text
insert table_snapshots(S+1)
insert compacted files with added_snapshot_version = S+1
insert compacted-file source lineage
mark source files removed_snapshot_version = S+1 while keeping state = 'committed'
update tables.current_snapshot_version = S+1
```

`data_files.added_snapshot_version` 的外键是立即约束，因此 snapshot 必须先于 file row 写入；
`removed_snapshot_version` 与 `tables.current_snapshot_version` 的外键虽然是 deferred，也统一按上述
顺序执行。事务提交前的中间顺序对读者不可见。source file 在 GC 判定无旧 lease 前保持
`committed`，否则旧 snapshot 会提前失去可见文件。

长查询持有旧 snapshot lease 时仍能读取 source files。GC 只有在不存在早于删除版本的 lease 后
才能物理删除文件。

### 5.4 Catalog command 模型

业务代码不得直接执行任意 SQL。所有写入使用类型化命令：

```rust
pub struct CatalogCommandEnvelope {
    pub request_id: RequestId,
    pub expected_revision: Option<u64>,
    pub actor: CatalogActor,
    pub command: CatalogCommand,
}

pub enum CatalogCommand {
    CreateTable(CreateTableCommand),
    AlterTableSchema(AlterTableSchemaCommand),
    AcquireProducer(AcquireProducerCommand),
    CommitDataFiles(CommitDataFilesCommand),
    CommitCompaction(CommitCompactionCommand),
    CommitConsumerOffset(CommitConsumerOffsetCommand),
    AcquireSnapshotLease(AcquireSnapshotLeaseCommand),
    ReleaseSnapshotLease(ReleaseSnapshotLeaseCommand),
    CreateMaintenanceJob(CreateMaintenanceJobCommand),
    TransitionMaintenanceJob(TransitionMaintenanceJobCommand),
    DropTable(DropTableCommand),
}
```

Catalog actor 按顺序执行：

```text
validate request hash
-> check idempotent_requests
-> begin immediate transaction
-> validate expected revision / invariants
-> apply command
-> write idempotent response
-> increment catalog revision
-> commit
-> publish CatalogEvent
```

同一个 `request_id` 携带不同 request hash 必须失败，不能返回旧响应。

冻结 V1 中的两个 manifest command 只定义 server-internal 的窄能力：

- `CommitDataFiles` 只接受 append table 的非空、按 file ID 严格排序的 native 文件。新文件的
  row count、offset range 和 source lineage 必须完全一致；所有新范围的并集必须从 parent
  watermark 的下一 offset 连续开始，并恰好结束于 command 的 `committed_log_offset`。
- `CommitCompaction` 只接受 append table 的 row-preserving compaction。输入必须是规划 snapshot
  中精确可见且按 file ID 严格排序的集合；输出的 row count、offset、按 schema version 分组的
  offset coverage 与 source-lineage 并集必须与输入完全一致，且 `committed_log_offset` 必须等于
  parent snapshot，不能借 compaction 推进水位或改写 schema。
- SQLite admission 在进入 actor writer lock 前限制 new/output files 与 compaction inputs 各 4096
  个、每文件 lineage 4096 条、总 lineage 65536 条、每 command 编码 metadata 64 MiB，以及每个
  typed metadata BLOB 1 MiB；所有累计使用 checked arithmetic。进入 actor queue 前还会按命令的
  输入结构预留内存，队列聚合预算默认 128 MiB 且可配置，避免 64 个最大命令同时驻留。
  metadata JSON 使用 64 KiB 分块、有 1 MiB 硬上限的 writer，escaped string 超限时不会先构造
  完整膨胀 JSON；command fingerprint 也直接把 canonical JSON 流入 SHA-256，不分配整条编码。
  所有 catalog command 的 actor principal 在入队前限制为 4 KiB；manifest reservation 计入该
  字符串，避免用超长审计身份绕过队列字节背压。

这两个 command 不携带 producer ID/epoch/batch sequence，不能作为 producer-fenced public ingest
协议；也不携带 maintenance job/lease generation/lease-bound worker identity，不能作为
lease-fenced worker 协议。envelope 中的 actor 只是审计元数据，不是 RPC 鉴权或 lease token。
文件和目录 fsync、producer fencing、maintenance lease 校验均由上层在调用前完成。
preserve-order legacy 缺少 discovery ordinal，frame rewrite 缺少类型化 frame coverage，KV
materialization 也不是 row-preserving；这些语义必须进入 V2 command，V1 明确返回
`UnsupportedPlan`。

### 5.5 Catalog read snapshot

查询不得逐个 RPC 获取 table/files/schema，因为中间状态可能变化。一次 query 涉及的所有表必须
在同一个 SQLite read transaction 内解析：

```rust
pub struct CatalogQuerySnapshot {
    pub catalog_revision: u64,
    pub tables: Vec<CatalogTableSnapshot>,
}

pub struct CatalogTableSnapshot {
    pub table: TableDescriptor,
    pub schema: SchemaDescriptor,
    pub snapshot: TableSnapshot,
    pub files: Arc<[DataFileDescriptor]>,
}
```

该对象在一个 SQLite read transaction 内构建。QueryService 后续只使用这个 immutable snapshot，
不在执行中重新读取 current snapshot。第一版只承诺每张表内部 snapshot 一致，不承诺多张 active
table 之间存在原子跨表事务；多表 SQL 的 Catalog 版本一致，但各表 live high watermark 由
SnapshotCoordinator 依次固定。这个边界必须出现在 query profile 中。

### 5.6 Schema 版本

每次 schema 变更创建新的 `schema_version`，不覆盖历史 schema。第一版兼容规则：

- append/frame 表允许在末尾新增 nullable 列。
- key-value 表不允许隐式修改 key 列。
- 删除列、重命名列和类型收窄默认拒绝。
- 类型 widening 必须经过显式 compatibility check。
- active segment rollover 后才切换新 schema。
- 一个 Parquet 文件只对应一个 schema version。
- QueryService 负责把旧文件 projection 到目标 schema，缺失 nullable 列补 null。

### 5.7 Persistent Catalog 与 RuntimeRegistry 分离

Catalog 只保存跨重启仍然成立的事实：

```text
namespace/table/schema
table snapshot/data files
producer epoch 和 durable sequence checkpoint
durable consumer offset
long-lived snapshot lease
maintenance job
idempotent control response
```

以下状态属于内存 `RuntimeRegistry`，不能因为 heartbeat 或 metrics 更新而写 SQLite：

```text
server session/connection
process heartbeat 和 boot id
source/engine runtime instance
active producer connection lease
active subscription connection
active query
active segment descriptor/high watermark
ordinary query snapshot lease
runtime metrics/health
```

RuntimeRegistry 使用分域 map 或 actor-owned state，不使用一个全局业务 mutex。Server 重启后这些
记录全部视为失效，由真实进程和客户端重新注册；禁止从旧 snapshot 恢复出看似 alive 的 runtime
status。需要持久保存的 pipeline/source 定义应成为独立 Catalog 对象，不能和 runtime instance
混在一个 record 中。

Admin status API 在响应时组合 Catalog descriptor 和 RuntimeRegistry status，但两者拥有不同 revision
和生命周期。轻量 status 不加载 file manifest，full inspect 才读取历史 metadata。

---

## 6. StorageEngine 设计

### 6.1 数据目录

```text
<data_root>/
  FORMAT_VERSION
  server.lock
  catalog/
    catalog.db
    catalog.db-wal
    catalog.db-shm
  wal/
    <table_uuid>/
      <producer_epoch>-<segment>.wal
  segments/
    <table_uuid>/
      active-<generation>.shm
      sealed-<generation>.shm
  parquet/
    <table_uuid>/
      <partition...>/
        <file_uuid>.parquet
  spill/
    <query_id>/
  tmp/
    persist/
    compact/
    import/
  backup/
  logs/
```

`FORMAT_VERSION` 在打开任何数据前检查。高版本数据目录禁止由低版本程序打开。

Catalog 中的 managed `file_path` 保存相对于 `data_root` 的规范化路径，不保存部署机器的绝对
路径。所有路径在创建和打开时拒绝 `..`、符号链接逃逸和 data root 外目标。Legacy importer 对
位于旧 persist root 内的绝对路径转换为相对路径；root 外文件只能以显式 `external_legacy` 状态
只读挂载，不能被 compaction/GC 自动删除。

### 6.2 Persist commit protocol

Persist worker 将 sealed segment 转成 Parquet：

```text
1. 固定 source_segment_id/generation 和 log offset range。
2. 在 tmp/persist 写 <txn_id>.parquet.tmp。
3. 写入 row count、partition、statistics 和 source identity metadata。
4. 关闭 writer。
5. 计算 checksum。
6. fsync 文件。
7. rename 到 parquet/<table_uuid>/.../<file_uuid>.parquet。
8. fsync 目标目录。
9. CatalogCommand::CommitDataFiles。
10. 收到 catalog snapshot_version 后标记 segment persisted。
11. 当不存在 reader/snapshot lease 时回收 sealed segment。
```

只有第 9 步完成后文件才可查询。第 7 步完成但第 9 步未完成的文件是 orphan，由恢复扫描或 GC
根据 file UUID 和 catalog 引用判定。

### 6.3 ReadSnapshot 和 persisted/live 协调

一次查询真正固定的读边界是：

```rust
pub struct TableReadSnapshot {
    pub catalog_revision: u64,
    pub table_id: TableId,
    pub schema_version: u64,
    pub persisted_snapshot_version: u64,
    pub persisted_files: Arc<[DataFileDescriptor]>,
    pub writer_epoch: u64,
    pub active_segment: Option<SegmentDescriptor>,
    pub active_committed_rows: Option<usize>,
    pub retained_sealed_segments: Arc<[SegmentDescriptor]>,
    pub max_log_offset: u64,
    pub leases: SnapshotLeaseSet,
}
```

对存在 active writer 的表，QueryService 不能先任意读取 current Catalog，再独立读取 live
descriptor。正确流程是：

```text
1. QueryService 请求 table actor PinReadSnapshot。
2. table actor 在写入顺序内固定 writer_epoch、active high watermark、retained sealed segments，
   并返回它已经观察到的 persisted_snapshot_version。
3. CatalogService 在同一个 query read transaction 中读取该精确 snapshot version。
4. SnapshotCoordinator 校验 schema version、writer epoch 和 source segment identities。
5. 若 actor 与 Catalog 的已提交版本无法对应，释放 lease 并重试。
6. 构造 immutable TableReadSnapshot。
```

不存在 active writer 的 persisted-only 表直接固定 Catalog current snapshot。多表查询按稳定 table id
顺序 pin，避免不同请求形成锁顺序反转。`source_segment_id/generation` 用于在计划前消除 persisted
与 live overlap，不能依赖路径或 created_at 猜测，也不应在执行期做昂贵的逐行去重。

### 6.4 表类型和存储语义

| Table kind | 持久逻辑 | 写入边界 | 查询语义 |
| --- | --- | --- | --- |
| `append_table` | append log | batch atomic | snapshot 内历史行 |
| `frame_table` | append log + frame marker | frame atomic | 历史行，可按 frame 查询 |
| `key_value_table` | changelog + materialized snapshot | changelog batch atomic | 当前状态或 changelog |

`frame_table` 继续复用 append storage，不新建第二套文件引擎。Parquet metadata 需要保存 frame
offset range，确保 compaction 后仍可重建 frame boundary。

Key-value 表遵循：

```text
snapshot(version=V, last_changelog_offset=S)
+ changelog(offset>S)
= current state
```

### 6.5 隐藏逻辑列

Durable append/changelog 数据不能只依赖文件顺序推导 offset。Compaction 可能按 `dt` 或其他业务列
排序，因此持久文件必须保留隐藏逻辑列：

| 隐藏列 | 类型 | 适用表 | 作用 |
| --- | --- | --- | --- |
| `_zippy_log_offset` | `UInt64` | append/frame/key-value changelog | 稳定 replay 和 head/tail 边界 |
| `_zippy_frame_id` | `UInt64` nullable | frame | compaction 后恢复完整 frame |
| `_zippy_schema_version` | `UInt64` | 所有 durable log | 审计和恢复校验 |
| `_zippy_kv_op` | enum/string | key-value changelog | upsert/delete |

这些列不在默认用户 projection 中显示，但不能在 compaction、schema projection 或 backup 中删除。
`_zippy_log_offset` 在同一 durable table 内跨 producer epoch 和 server restart 单调且不复用。
File/row-group statistics 必须覆盖它，SubscriptionService 才能快速定位 replay 起点。

### 6.6 Retention 和 GC

逻辑 retention 和物理删除分离：

```text
mark removed at snapshot S
-> new queries no longer see file
-> old snapshot leases may still read file
-> maintenance checks min pinned snapshot
-> delete physical file
-> mark data_files.state='deleted'
```

删除失败必须保留 `deleting` 状态和错误，供下次重试。禁止 catalog 先删除记录再尝试物理删除。

### 6.7 事务和一致性保证矩阵

| 操作 | 原子单元 | 隔离/可见性 | 持久性 |
| --- | --- | --- | --- |
| create/alter/drop table | 一个 CatalogCommand | Catalog revision 原子切换 | SQLite commit |
| append batch | 一个 RecordBatch/request | committed prefix 后整体可见 | 取决于 durability |
| frame write | 一个完整 frame | 不允许读到半 frame | 取决于 durability |
| key-value changelog | 一个 batch | seq 连续，snapshot 带 watermark | 取决于 durability |
| persist files | 一个 manifest command | snapshot version 原子切换 | file fsync + SQLite commit |
| compaction | 一个 input set -> output set | 旧/新 snapshot 二选一 | file fsync + SQLite commit |
| single-table query | 一个 TableReadSnapshot | table snapshot isolation | 只读 |
| multi-table query | 每表各自 snapshot | Catalog 同 revision；live watermark 非跨表原子 | 只读 |
| multi-table write transaction | 不支持 | 明确拒绝 | 不适用 |

同一 session 在收到 write ACK 后立即发起查询，必须能读取该 ACK 对应的 committed active rows；
`durability=wal` 表示崩溃后可通过 WAL 恢复，并不要求该批已经进入 Parquet；
`durability=persisted` 才返回覆盖该批 offset 的 manifest snapshot version。

Zippy 第一版不宣称 serializable、多表 ACID 或 exactly-once callback。成熟性的要求是契约明确、
实现可恢复、错误不静默，而不是在没有需求时复制完整 OLTP 事务模型。

---

## 7. Ingest WAL 和写入协议

### 7.1 写入请求

所有远程 durable write 使用：

```rust
pub struct WriteBatchRequest {
    pub request_id: RequestId,
    pub table_id: TableId,
    pub schema_version: u64,
    pub producer_id: ProducerId,
    pub producer_epoch: u64,
    pub batch_sequence: u64,
    pub durability: Durability,
    pub payload: ArrowIpcPayload,
}

pub enum Durability {
    Memory,
    Wal,
    Persisted,
}
```

响应必须包含：

```rust
pub struct WriteBatchResponse {
    pub request_id: RequestId,
    pub producer_id: ProducerId,
    pub producer_epoch: u64,
    pub batch_sequence: u64,
    pub first_log_offset: u64,
    pub last_log_offset: u64,
    pub durability_reached: Durability,
    pub persisted_snapshot_version: Option<u64>,
}
```

### 7.2 producer fencing 和幂等

- 每张表默认一个 active producer，可按表配置多 producer 策略。
- producer 重连后通过 acquire 获得更高 `producer_epoch`。
- 旧 epoch 的写入全部拒绝。
- 同 epoch 内 `batch_sequence` 严格递增。
- 重复 sequence 且 payload hash 相同，返回原响应。
- 重复 sequence 但 payload hash 不同，返回 conflict。
- sequence 出现 gap 时默认拒绝，除非请求显式声明允许跳号。

这解决“服务器已提交、响应丢失、客户端不知道是否应重试”的网络歧义。

高频 batch 的幂等响应不逐批写入 Catalog `idempotent_requests`。IngestService 将 producer epoch、
sequence、payload hash 和 ACK boundary 写入 ingest WAL，并在 WAL checkpoint 时更新 Catalog
`producers` 水位。`idempotent_requests` 只处理 DDL、manifest commit、maintenance 和 admin 等
低频控制操作。

### 7.3 WAL record

建议 WAL record 使用固定 header + Arrow IPC payload：

```text
magic
format_version
header_length
payload_length
table_uuid
schema_version
producer_uuid
producer_epoch
batch_sequence
first_log_offset
row_count
payload_crc32c
header_crc32c
Arrow IPC bytes
```

记录写入顺序：

```text
validate producer/schema
-> assign log offsets
-> append WAL record
-> durability=wal 时进入 group commit
-> apply active segment
-> publish committed prefix
-> return ACK
```

`memory` 模式允许跳过 WAL；`persisted` 模式等待覆盖该 offset range 的 manifest commit。

`memory` 模式的幂等和 log offset 只在当前 server 进程生命周期内有效。进程崩溃后，所有未进入
WAL/manifest 的 ACK 数据均视为丢失，producer 必须获取新 epoch；durable subscription 不允许依赖
仅以 memory 模式写入的数据。该模式只用于明确接受进程故障丢失的极低延迟链路。

### 7.4 WAL checkpoint 和回收

WAL 只有在以下条件同时满足后才能截断：

```text
offset <= persisted_snapshot.committed_log_offset
AND
key-value/frame 所需边界已进入持久 metadata
AND
不存在要求从更早 offset replay 的 retention contract
```

重启恢复：

```text
load catalog current snapshot
-> open latest committed WAL checkpoint
-> scan records and validate CRC
-> truncate partial tail
-> replay offsets after persisted watermark
-> rebuild active segment/table state
-> publish readiness
```

---

## 8. SubscriptionService 设计

### 8.1 稳定 offset

当前 segment row offset 会随 rollover 或 snapshot replace 改变，不能作为数据库级消费位置。
新协议使用每张表单调递增的 `log_offset`：

```text
table_id + log_offset -> one logical row/event position
```

Compaction、Parquet rewrite 和 segment rollover 不得改变 log offset。

### 8.2 订阅请求

```rust
pub struct SubscribeRequest {
    pub subscription_id: SubscriptionId,
    pub table_id: TableId,
    pub start: SubscriptionStart,
    pub consumer_group: Option<String>,
    pub delivery: DeliveryMode,
    pub filter: Option<ExprPlan>,
    pub batch_rows: Option<usize>,
    pub throttle_ms: Option<u64>,
    pub persist_offset: bool,
}

pub enum SubscriptionStart {
    Latest,
    EarliestRetained,
    Offset(u64),
    Committed,
    SnapshotThenTail,
}
```

### 8.3 表类型映射

| API | append | frame | key-value |
| --- | --- | --- | --- |
| `subscribe()` | log rows | 不推荐 | changelog events |
| `subscribe_frames()` | 拒绝 | frame records | 拒绝 |
| `subscribe_table()` | incremental batches | complete frames/batches | full latest snapshots |
| `read_table()` | point-in-time history | point-in-time history | current state snapshot |

### 8.4 delivery contract

第一版支持：

```text
best_effort
at_least_once
```

不宣传 exactly-once callback。`at_least_once` 通过持久 consumer offset 和幂等 handler 实现。
回调成功后客户端或服务器提交 offset；崩溃后允许重复最后一批，但不能跳过已保留数据。

### 8.5 retention failure

请求 offset 已早于 retention watermark 时返回结构化错误：

```text
OFFSET_EXPIRED
requested_offset
earliest_retained_offset
latest_offset
snapshot_recovery_available
```

Key-value 可建议使用 `SnapshotThenTail`；append 表不能静默从 latest 继续。

Retention planner 必须考虑持久 consumer offset。若策略决定强制回收消费者尚未处理的数据，必须
先推进 `earliest_retained_offset` 并记录 forced expiration audit event，之后该消费者只能收到
`OFFSET_EXPIRED`，不能返回看似连续但实际缺行的数据。

---

## 9. QueryService 设计

### 9.1 使用 DataFusion 的边界

DataFusion 负责：

- SQL parser/binder。
- logical/physical optimizer。
- projection/filter/limit/sort/aggregate/join/window。
- Parquet scan。
- streaming execution。
- memory pool 和 spill。
- explain 和 operator metrics。

Zippy 负责：

- Catalog snapshot 和 table kind。
- active/sealed mmap segment scan。
- persisted/live overlap resolution。
- logical log offset 和 frame/key-value 语义。
- Zippy 特有 functions/operators。
- query admission 和 session policy。

禁止 fork DataFusion。扩展使用：

```text
TableProvider
ExecutionPlan
OptimizerRule
ScalarUDF/AggregateUDF/WindowUDF
ObjectStore
MemoryPool/DiskManager
```

接入前必须做 Arrow 版本兼容 spike；当前 workspace Arrow 版本和目标 DataFusion 版本必须统一，
不能在进程内长期保留两套 Arrow ABI/类型。

### 9.2 ZippyTableProvider

```rust
pub struct ZippyTableProvider {
    pub read_snapshot: Arc<TableReadSnapshot>,
    pub catalog: Arc<dyn CatalogRead>,
    pub storage: Arc<dyn StorageRead>,
}
```

`scan()` 生成：

```text
PersistedAndActiveExec(one output partition)
  -> ParquetExec(persisted files; drain first)
  -> ActiveSegmentExec(retained sealed + active high watermark; open after persisted EOF)
-> exact projection/filter/limit
```

文件列表只能来自 `TableReadSnapshot`，不能在 operator 执行中重新读取 current catalog。
SnapshotCoordinator 必须在物理计划生成前按 source segment identity 选出互不重叠的数据源，因此
正常查询计划中不放置全量 row-level dedup operator。

### 9.3 查询计划所有权

客户端发送以下之一：

```text
SQL text
Zippy typed logical plan protobuf
Substrait plan（后续可选）
```

服务器完成：

```text
submit + logical admission
-> parse
-> bind against catalog snapshot
-> type check
-> optimize
-> create physical plan
-> complete planning + attach snapshot root
-> execute
```

`submit` 必须发生在 parse/bind 和 snapshot pin 之前。它同步分配不可复用的
`QueryInstanceId(QueryId, generation)`，用服务端时钟记录 `submitted_at_ns`，把实例以
`Planning` 放入有界 registry，并返回不可伪造、不可 `Clone` 的一次性 planning token。token 提供
exact instance 与 cancellation；planner 只能消费它调用 `complete_planning` 或 `fail_planning`。
token 未消费即 drop 表示 `Cancelled`，不能把 planner error 静默改写成 cancellation。

Python 不再进行 remote/residual plan split。若服务端不支持某个操作，应在执行前返回
`UNSUPPORTED_PLAN`，不能先拉取全表后在客户端悄悄执行。

### 9.4 Query registry

```rust
pub struct QueryRecord {
    pub instance_id: QueryInstanceId,
    pub owner_session_id: SessionId,
    pub submitted_at_ns: u64,
    pub plan: Option<QueryPlanStatus>,
    pub started_at_ns: Option<u64>,
    pub finished_at_ns: Option<u64>,
    pub state: QueryState,
    pub priority: QueryPriority,
    pub deadline_ns: Option<u64>,
    pub memory_limit_bytes: usize,
    pub spill_limit_bytes: usize,
    pub cancellation: CancellationToken,
    pub metrics: QueryMetrics,
}

pub struct QueryPlanStatus {
    pub planned_at_ns: u64,
    pub expires_at_ns: u64,
    pub catalog_revision: u64,
    pub snapshots: Vec<QueryTableVersion>,
}
```

`plan=None` 表示尚未 bind snapshot；`plan=Some` 且 `snapshots=[]` 表示合法的无表查询。禁止用零
revision、空 snapshots 或登记时间填充尚未发生的 plan 指标。`complete_planning` 的线性化点必须在
`global -> query` 锁序内一次完成 snapshot root、snapshot owner 计数、plan metadata、plan TTL 与
`Planning -> Planned`；任一失败要么保留已经发生的 first-terminal，要么落成 `Failed`/`TimedOut`，
不能遗留无 planning token 的孤儿 `Planning`。

即使请求未提供 deadline，服务端也必须设置非零 `max_planning_duration`。当前 native 默认 30 秒，与
legacy QueryEngine 的 plan TTL 对齐，避免迁移期出现两套明显不同的悬挂窗口。Planning 实际 deadline 为
`min(request_deadline, submitted_at + max_planning_duration)`；进入 Planned 时必须原子删除该 deadline，
再安装仍需覆盖 Planned/Running 的 request deadline 和只覆盖 Planned 的 plan TTL。

状态机：

```text
submitted -> queued -> planning -> planned -> running -> finished
                         |          |          |-> failed
                         |          |          |-> cancelled
                         |          |          |-> timed_out
                         |          |-> failed/cancelled/timed_out
                         |-> failed/cancelled/timed_out
```

取消必须传播到 Parquet scan、active segment scan、sort/join/aggregate 和结果 socket。

### 9.5 ResourceManager

进程内存分成不可互相透支的预算：

```text
live_segment_bytes
ingest_wal_buffer_bytes
query_memory_bytes
query_spill_bytes
parquet_metadata_cache_bytes
subscription_buffer_bytes
catalog_cache_bytes
```

查询 admission 依据估算和硬上限，不再只按 semaphore 数量：

```text
global query memory
per-query memory
global query CPU slots
per-query parallelism
spill disk quota
deadline
priority
```

至少提供 `interactive`、`normal`、`maintenance` 三类优先级。Maintenance query 不得占满
interactive slots。

### 9.6 结果流和顺序

- 查询默认不承诺行顺序，只有显式 `sort` 或表语义操作才承诺。
- `head/tail` 按逻辑 log offset 定义，不能按文件创建时间猜测。
- `collect(stream=True)` 成为默认 server result stream，不再先物化完整结果。
- Arrow batch 和 IPC payload 都纳入 ResourceManager。
- 客户端断开等价于 cancel query，必须尽快释放 snapshot lease 和 memory reservation。

### 9.7 Explain 和 profile

```text
EXPLAIN
EXPLAIN ANALYZE
query_status(query_id)
cancel_query(query_id)
list_queries(filters)
```

返回内容包括：

```text
catalog revision
table snapshot versions
logical plan
physical plan
files/row groups pruned
rows scanned/returned
operator elapsed
peak memory
spill bytes
first batch latency
network write time
```

---

## 10. FrontendService 和协议

### 10.1 对外 endpoint

```text
unix://<data_root>/run/zippy.sock     # 本地默认
grpc+tcp://host:port                  # Arrow Flight
legacy://host:port                    # 迁移期 Gateway adapter
```

用户只配置一个逻辑 URI。Frontend 根据能力选择 mmap 本地路径或 Flight 远端路径。

### 10.2 Arrow Flight 映射

| Flight RPC | Zippy 用途 |
| --- | --- |
| `GetFlightInfo` | 解析查询并返回 result ticket/schema |
| `DoGet` | 流式查询结果 |
| `DoPut` | Arrow batch ingest |
| `DoExchange` | 双向 subscription/ack |
| `ListFlights` | catalog table discovery，可选 |
| `DoAction` | cancel、snapshot、admin、maintenance |
| Flight SQL metadata | namespace/table/schema discovery |

第一阶段不要求完整 SQL 标准兼容，但错误码、metadata 和 cancellation 必须使用稳定协议。

### 10.3 Admin API

Admin API 与 query/ingest 分开授权：

```text
server health/readiness
catalog list/get
create/alter/drop table
producer/session inspection
query list/cancel
maintenance jobs
backup/verify
shutdown
```

旧 `MasterClient` 作为 Admin API adapter，内部不得再访问 legacy Registry。

### 10.4 错误模型

所有错误至少包含：

```rust
pub struct ServerError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub request_id: Option<RequestId>,
    pub query_id: Option<QueryId>,
    pub table_id: Option<TableId>,
    pub details: BTreeMap<String, String>,
}
```

核心错误码：

```text
INVALID_ARGUMENT
UNAUTHENTICATED
PERMISSION_DENIED
NOT_FOUND
ALREADY_EXISTS
CONFLICT
STALE_PRODUCER_EPOCH
SEQUENCE_GAP
SCHEMA_MISMATCH
OFFSET_EXPIRED
RESOURCE_EXHAUSTED
DEADLINE_EXCEEDED
CANCELLED
UNAVAILABLE
DATA_CORRUPTION
INTERNAL
```

禁止客户端通过解析英文错误字符串决定重试策略。

### 10.5 安全

单机默认：

- Unix socket 权限控制。
- data root 权限为 owner-only。
- 不自动打印 token。
- 默认不监听非 loopback TCP。

启用远端访问时：

- TLS 必须显式配置。
- 支持 bearer token 或 mTLS。
- token 只保存 hash 或通过 secret manager 注入。
- 至少支持 `admin`、`read`、`write`、`subscribe` 权限。
- DDL、ACL、登录和 drop/backup 操作写 audit log。

---

## 11. MaintenanceService

### 11.1 任务类型

```text
persist sealed segment
compact files
garbage collect files
checkpoint catalog WAL
checkpoint ingest WAL
collect table statistics
verify file checksum
backup catalog/data
restore validation
orphan scan
```

### 11.2 持久任务状态机

```text
pending -> leased -> running -> succeeded
                         |-> retry_wait -> pending
                         |-> failed
                         |-> cancelled
```

任务 lease 存在 Catalog。Worker 崩溃后 lease 超时，任务可重领。每个任务必须幂等：

- 相同 compaction input file set 生成稳定 operation id。
- 目标文件使用 UUID，不覆盖既有文件。
- Catalog commit 重复提交返回原 snapshot version。
- 删除不存在文件视为成功，但必须核对 catalog state。

### 11.3 Compaction 计划

Compaction planner 根据：

```text
partition
schema version
file count
total bytes
row count
sort range overlap
query heat
snapshot lease
```

选择文件。禁止在 `zippy-server` control/query runtime 中直接读取并 concat 全部文件。

### 11.4 资源边界

Maintenance worker 独立配置：

```text
max_cpu_cores
max_memory_bytes
max_io_bytes_per_sec
max_temp_bytes
max_concurrent_jobs
quiet_hours
```

收盘阶段可以提高额度，交易阶段自动降级，但策略必须由配置明确，不依赖硬编码时间。

Maintenance worker 只能通过本地 Unix maintenance RPC lease/update job 和提交 CatalogCommand，
不得自行打开 `catalog.db`。SQLite 文件只由 CatalogService 管理，避免 schema migration、事务策略和
授权边界在多个进程中分叉。

### 11.5 Backup 和 restore

一致备份流程：

```text
1. 创建 BackupJob 并固定每张表的 persisted snapshot version。
2. 对需要覆盖最新 durable write 的表执行 WAL checkpoint，或把所需 WAL range 纳入备份。
3. 通过 SQLite online backup API 复制 Catalog，而不是直接复制活动中的 db 文件。
4. 复制固定 snapshot 可见的 Parquet 文件和必要 WAL 文件。
5. 写 backup manifest，包含 FORMAT_VERSION、catalog revision、table snapshots、文件大小和 checksum。
6. 对备份执行独立 verify。
7. 释放 backup snapshot leases。
```

Restore 只能写入空 data root：先校验 manifest/checksum，再恢复 Catalog 和数据文件，最后以
`--verify-only` 模式启动恢复检查。禁止直接把备份覆盖到正在运行的数据目录。

---

## 12. 生命周期和恢复

### 12.1 启动顺序

```text
1. acquire server.lock
2. validate FORMAT_VERSION
3. open logging without secrets
4. open SQLite catalog and run migrations
5. run catalog quick_check；完整 integrity_check 由 maintenance/显式命令执行
6. clean expired leases/jobs
7. scan tmp/orphan state
8. recover ingest WAL and active tables
9. start CatalogService
10. start Ingest/Subscription/Query runtimes
11. bind Unix/Flight endpoints
12. publish readiness
13. start maintenance worker
```

Gateway 不再先于 catalog/control readiness 启动。

### 12.2 Readiness

`ready=true` 必须同时满足：

```text
catalog writable
all required migrations complete
WAL recovery complete
all tables have valid current snapshot
frontend bound
ingest actors ready
```

Maintenance 未 ready 不阻止基础读写，但 health 中必须明确 degraded。

### 12.3 优雅关闭

```text
stop accepting new sessions
-> reject new queries/writes
-> cancel or drain queries according to timeout
-> flush subscription offsets
-> flush WAL group commit
-> seal/checkpoint active tables according to policy
-> checkpoint catalog WAL
-> release leases/server.lock
-> stop runtimes
```

超时必须报告未完成对象，不能记录“假成功”。

### 12.4 崩溃恢复原则

- Catalog 是 metadata 真相源。
- WAL 是未进入持久 snapshot 的写入真相源。
- Parquet 文件只有被 Catalog 引用才可见。
- Active segment 是可重建缓存，不是唯一持久真相。
- tmp/orphan 文件不直接删除，先通过 catalog/file identity 验证。

---

## 13. 可观测性

### 13.1 全局标识

所有请求传播：

```text
request_id
session_id
query_id
producer_id
subscription_id
table_id
catalog_revision
snapshot_version
```

### 13.2 指标

至少提供：

```text
catalog transaction latency/conflicts/WAL bytes/checkpoint age
ingest rows/bytes/WAL fsync/group size/segment rollover
producer duplicates/gaps/stale epochs
query queue/running/cancelled/failed/peak memory/spill
scan files/row groups/bytes/rows/pruning ratio
subscription lag/reconnect/replay/buffered bytes/offset commits
maintenance queue/runtime/input/output/temporary bytes
file checksum failures/orphan count/GC backlog
```

支持 Prometheus/OpenTelemetry 导出；现有 JSON metrics 保留为兼容视图。

### 13.3 日志和审计

- 日志全部结构化。
- 日志消息保持小写英文开头和方括号变量约定。
- token、password、Authorization header 永不记录。
- DDL、drop、backup、restore、ACL 和 remote login 进入独立 audit stream。
- Query text 默认可配置脱敏或只记录 hash。

---

## 14. 配置设计

建议配置结构：

```toml
[server]
data_dir = "data/zippy"
unix_endpoint = "data/zippy/run/zippy.sock"
shutdown_timeout_ms = 10000

[runtime]
max_cpu_cores = 4
max_query_buffer_bytes = 536870912

[catalog]
path = "data/zippy/catalog/catalog.db"
synchronous = "full"
checkpoint_bytes = 67108864
idempotency_retention_hours = 24

[ingest]
default_durability = "wal"
wal_group_commit_ms = 2
wal_segment_bytes = 268435456
max_buffered_bytes = 268435456

[query]
parallelism = 4
max_buffered_bytes = 134217728
max_spill_bytes = 4294967296
plan_ttl_ms = 30000

[query.resources]
max_concurrent_queries = 8
max_active_tasks = 16
max_memory_bytes = 536870912

[subscription]
max_clients = 128
max_buffered_bytes = 268435456
offset_flush_interval_ms = 1000

[flight]
enabled = false
host = "127.0.0.1"
port = 17691
tls_cert = ""
tls_key = ""

[maintenance]
enabled = true
max_cpu_cores = 2
max_memory_bytes = 536870912
max_concurrent_jobs = 1

[compatibility]
legacy_master_api = true
legacy_gateway_protocol = true
legacy_registry_import = true
```

`ZIPPY_MAX_CPU_CORES` 是 canonical 全局 CPU 配置上限；组件配置只能收紧，不能超过全局值。
在多个独立线程池统一接入同一个进程级 semaphore 前，不把该配置宣称为已实现的硬 CPU
admission。

---

## 15. Rust crate 和代码边界

推荐目标布局：

```text
crates/
  zippy-types/              # IDs、errors、table kinds、protocol-neutral DTO
  zippy-catalog/            # Catalog trait、SQLite 实现、migration
  zippy-storage/            # WAL、manifest、Parquet、live snapshot resolver
  zippy-query/              # DataFusion integration、query registry、resource manager
  zippy-ingest/             # producer、table actor、durability
  zippy-subscription/       # offset、replay、delivery
  zippy-protocol/           # Protobuf/Flight/admin protocol
  zippy-server/             # composition root、config、lifecycle
  zippy-maintenance/        # compaction/GC/verify/backup worker
  zippy-segment-store/      # 保留当前 mmap/segment 内核
  zippy-engines/            # 保留实时计算引擎
  zippy-python/             # Python bindings 和兼容适配
```

迁移期现有 crate 不立即重命名。先引入接口：

```rust
#[async_trait]
pub trait Catalog: Send + Sync {
    async fn command(&self, command: CatalogCommandEnvelope) -> Result<CatalogResponse>;
    async fn read_snapshot(&self, tables: &[TableRef]) -> Result<CatalogQuerySnapshot>;
}

pub trait StorageEngine: Send + Sync {
    fn pin_read_snapshots(&self, catalog: CatalogQuerySnapshot) -> Result<QueryReadSnapshot>;
    fn open_live_writer(&self, request: OpenWriterRequest) -> Result<Box<dyn TableWriter>>;
}

#[async_trait]
pub trait QueryEngine: Send + Sync {
    async fn plan(&self, session: &Session, request: QueryRequest) -> Result<PlannedQuery>;
    async fn execute(&self, query: PlannedQuery) -> Result<SendableRecordBatchStream>;
    async fn cancel(&self, query_id: QueryId) -> Result<()>;
}
```

禁止形成一个 `ServerState` 暴露所有内部 map 和 mutex。Composition root 只保存各 service trait
handle，不允许跨层直接访问实现字段。

---

## 16. Python API 兼容策略

### 16.1 保持不变

```python
zippy.connect(...)
zippy.read_table(...)
zippy.subscribe(...)
zippy.subscribe_table(...)
zippy.subscribe_frames(...)
session.append_table(...).publish(...)
session.frame_table(...).publish(...)
session.key_value_table(...).publish(...)
```

### 16.2 内部替换

| Python 对象 | 新实现 |
| --- | --- |
| `MasterClient` | AdminClient compatibility facade |
| `RemoteTableReader` | Flight query client |
| `RemoteGatewayWriter` | Flight DoPut producer client |
| `RemoteStreamSubscriber` | Flight DoExchange subscriber |
| `Table.collect()` | server-side QueryService |
| `Table.explain()` | server explain API |

### 16.3 行为变化必须显式版本化

- 查询无显式 sort 时不承诺顺序。
- `subscribe(reconnect=True)` 从 best-effort 改为可选择 replay。
- remote write 增加 producer/session 生命周期。
- schema mismatch 返回结构化错误。
- 不支持的远端计划不再自动退回客户端全表执行。

提供 `compatibility.query_residual_execution=true` 作为短期过渡开关，并在日志和 metrics 中明确
记录 residual 执行。新安装默认关闭。

---

## 17. 数据迁移策略

### 17.1 原则

- 永不原地覆盖旧 `master-registry.json`。
- 永不重写原始 Parquet 以完成 catalog migration。
- Migration 可重复运行。
- 导入结果包含 source snapshot hash 和 migration version。
- 迁移完成后保留只读备份。

### 17.2 JSON Catalog 导入

```text
read master-registry.json
-> validate stream records and table-relevant source metadata
-> assign stable table UUIDs
-> normalize schemas
-> import persisted_files as committed data_files
-> derive log offset ranges where possible
-> mark unknown ranges as legacy ordering
-> create table snapshot version 1
-> do not import process/source/engine runtime status as alive
-> write migration report
```

旧 source/engine 记录只用于 migration report 和补齐必要 table metadata。运行实例由真实进程在新
server 上重新注册；若未来需要持久 pipeline definition，必须通过独立 migration 显式创建 Catalog
对象，不能直接复制旧 runtime status。

无法可靠推导 offset 的旧文件必须标记：

```text
legacy_ordering = true
```

查询仍按现有 deterministic file order 兼容读取，但 durable subscription 不允许从这些文件任意
offset replay，除非执行离线 backfill/reindex。

### 17.3 Shadow 模式

上线顺序：

```text
legacy Registry 为主
SQLite Catalog 接收同一 CatalogCommand shadow write
-> 周期比较 table/schema/files/source identity
-> 记录 mismatch
-> mismatch 清零后切 read path
-> 再切 write authority
```

禁止在业务代码里分别调用两套写 API。必须先生成一个 immutable `CatalogCommand`，再由 migration
adapter fan-out，避免两边验证逻辑不同。

### 17.4 回滚

切换 SQLite authority 后，legacy JSON 只作为降级导出，不再双向同步。回滚到旧版本需要使用：

```text
zippy catalog export-legacy --at-revision <revision>
```

而不是直接让旧 binary 打开新数据目录。

---

## 18. 分阶段实施计划

依赖关系：

```text
Phase 0 contracts
    -> Phase 1 service boundaries
        -> Phase 2 SQLite Catalog
            -> Phase 3 manifest storage
                +-> Phase 4 maintenance worker
                +-> Phase 5 DataFusion query -> Phase 6 Flight frontend
                +-> Phase 7 durable ingest/subscription
                    -> Phase 8 remove legacy
```

Phase 4、5、7 在 Phase 3 稳定后可以分别开发，但 Phase 8 必须等待三条路径全部完成。Flight
frontend 依赖 QueryService 的 query lifecycle，不应先用 Flight 包装 legacy collect 形成第二套临时
协议。

### Phase 0：契约冻结和基线

目标：在改内核前固定行为和证据。

任务：

1. 为 append/frame/key-value 建立 query/write/subscribe golden fixtures。
2. 固定旧 JSON registry fixture 和 persisted file fixture。
3. 为 `writer_epoch`、lease、snapshot、drop、compaction 建立 contract tests。
4. 建立 crash harness，可在指定 failpoint 执行 `abort()`/`kill -9`。
5. 记录当前 ingest/query/control 性能基线。
6. 定义 `FORMAT_VERSION=1` 和数据目录 ownership。
7. 写 ADR：SQLite、DataFusion、Flight、单机边界。

验收：

- 所有现有测试通过。
- Golden 输出进入仓库。
- 每个关键 commit point 都有命名 failpoint。
- 性能报告记录 binary、git SHA、配置和数据集。

回滚：无运行时变更。

### Phase 1：服务接口和 composition root

目标：先拆代码依赖，不改变行为。

任务：

1. 新建 `zippy-types`，迁移 ID、table kind、error code 和通用 DTO。
2. 定义 `Catalog`、`RuntimeRegistry`、`StorageEngine`、`QueryEngine`、`IngestService` 接口。
3. 将 process/source/engine/query/subscription live status 放入 RuntimeRegistry adapter。
4. 为当前 Registry 的持久部分实现 `LegacyCatalogAdapter`。
5. 为当前 Gateway collect 实现 `LegacyQueryEngineAdapter`。
6. 新建 `zippy-server` composition root，内部仍组合 legacy 实现。
7. 增加 `zippy server run`，`zippy master run` 转发到它。
8. 增加统一 lifecycle/readiness/health contract。

验收：

- 用户 API 和 wire 行为不变。
- `zippy server run` 能运行全部 legacy integration tests。
- crate dependency graph 不出现循环。
- 业务层只依赖 trait，不依赖 Registry/GatewayState 字段。
- Server restart 后旧 runtime records 不会恢复成 alive，真实客户端可重新注册。

回滚：继续使用原 master binary。

### Phase 2：SQLite Catalog v2

目标：替换 Registry 和 JSON snapshot。

实施必须拆成两个不可混淆的物理版本阶段：

- P2.1 只建立非权威 migration foundation。SQLite physical schema v1 只包含
  `catalog_meta` 和 `catalog_migrations`，用于验证单连接 actor、WAL、迁移审计、完整性检查与
  fail-closed 边界；它不实现 `Catalog` trait，也不接受 shadow 或业务写入。
- P2.2 冻结完整类型化 domain schema 后再增加 namespace/table/schema/snapshot/file、producer、
  subscription、lease、job 和 idempotency 表。所有 `u64`、watermark、writer epoch、lineage 与
  provenance 必须先定义无损编码和 golden migration test，之后才允许接入 server。

当前实施状态（2026-07-14）：physical v2 domain schema、无损 BLOB 编码、physical v3 active-table
cursor indexes、v1/v2 -> v3 migration、schema inventory、`CreateNamespace`/`CreateTable` 单写者事务、
server-internal native append `CommitDataFiles`、append row-preserving `CommitCompaction`、
namespace/table get/list/resolve 和跨重启幂等回放已经实现。manifest command 在同一个 immediate
transaction 中先写 snapshot，再写 files/lineage、标记 compaction inputs、切换 current snapshot，
最后提交 catalog revision 与幂等响应；严格验证 watermark 连续性，以及 input/output 的 row、
logical offset、schema-version coverage 和 lineage 等价。
`CreateTable` 在同一事务中写入 table、schema version 1、snapshot version 1、catalog revision 与幂等
响应；Arrow Schema 在获取 writer lock 前按有界 schema-only IPC stream 校验：IPC payload 不超过
16 MiB、递归 field 不超过 4096、嵌套不超过 32 层、metadata 不超过 4096 项/1 MiB，FlatBuffer
verifier 最多遍历 32768 个 table 和 16 MiB apparent size；schema 以版本化 canonical logical SHA-256
（`sha256-v1:<64 hex>`）标识；持久化 typed BLOB 通过冻结的 V1 storage DTO 编解码，避免运行时
domain 类型演进静默改变落盘格式。manifest preflight 在取得 command permit 后按每 256 个 file 或
metadata work item 协作式 yield，完成后仍以精确 byte charge 进入聚合预算；fingerprint 校验与纯语义
preparation 已移到独立有界 worker。control read 与 exact snapshot read 分别使用独立的有界队列、OS
worker 和 read-only WAL connection，因此大 snapshot 不再阻塞 discovery/revision 或 writer commit。
mutation 保持 actor admission 顺序；重叠 read 可以读取前一个 committed revision，取消响应不会取消
已接受写入，close 会排空已接受工作。legacy importer 的 Server V2 planner/executor 已接线；完整的
shadow command fan-out/diff authority gate 仍待收口。其余 V1 mutation command、producer-fenced public
ingest、maintenance lease-fenced worker、frame/KV V2 manifest、CreateTable preparation 隔离、manifest
finalize 减负、read parallelism 和 active read interrupt 尚未实现。SQLite actor 内部仍没有绝对
shutdown deadline；启用 native storage 时由独立 OS supervisor
隔离同步 open/close，server timeout 只停止等待且保留 supervisor、Catalog 与 data-root ownership，
后台继续按 coordinator drain -> Catalog close 清理。因此 SQLite 可以作为显式灰度的 required
startup dependency，但尚不能成为 `zippy server run` 的 read/write authority。
modern manifest 的多表 exact `CatalogQuerySnapshot` 已在同一个 deferred transaction 中读取
table/schema/snapshot/visible files/native lineage；schema IPC 与
hash、snapshot chain、metadata checksum、file visibility 和 lineage coverage 都会重新验证。

snapshot admission 要求 selector 按 table ID 严格升序且版本非零，最多 4096 张表；全有或全无的
raw response 上限为 64 MiB、65536 个 visible files 和 262144 条 source lineage。historical manifest
通过 `data_files_visible_by_table` seek，不在 SQLite 内 `ORDER BY`，而是在预算内按 file ID 做 Rust
排序，避免临时 B-tree；lineage 按最多 512 个 file ID 分块读取，避免逐文件 N+1。raw snapshot 继续
保持 `file_id` 规范顺序，preserve-order legacy descriptor 另外携带显式 discovery ordinal；pinned
query manifest/task 才按该 ordinal 生成旧 Gateway 兼容的逻辑扫描顺序，native 与 non-preserved 文件
保持 `file_id` 顺序。这样 raw Catalog 序列化/影子比对顺序与物理 scan scheduling 不再混为一层。

轻量 table discovery 同时受条数与字节预算约束：list page 最多 4096 项，并在累计 raw descriptor
达到 4 MiB 前提前结束、返回可继续的 keyset cursor；`resolve_tables` 保持全有或全无，累计超过
4 MiB 返回 `ResourceExhausted`。引用列表在复制进 actor queue 前限制为 4096 项和 1 MiB 名称字节，
新 SQLite Catalog namespace/table 写入限制为 1 KiB UTF-8 字节，但不改变 frozen V1 decoder 与
LegacyCatalogAdapter 的历史名称域，避免合法但超大的新请求在单连接 actor 中形成无界内存、
队首阻塞；另外按实际落盘 name/definition/config 编码大小执行 4 MiB 单行 admission check，保证
分页游标始终可以推进。迁移的版本读取、schema fingerprint 校验和所有待执行 migration 由同一个
`BEGIN IMMEDIATE` 覆盖，因此并发首次打开或 v1/v2 升级不会使用锁外的过期版本判断。

V1 manifest admission 同样在 writer lock 前执行：new/output files 与 compaction inputs 各最多
4096 个，每文件 source lineage 最多 4096 条、每 command 总计最多 65536 条，三类 metadata 的
实际编码总量最多 64 MiB，单个 partition/statistics/metadata typed BLOB 最多 1 MiB。该预算既限制
内存，也限制单连接 writer 的锁持有时间；超过边界统一返回 `ResourceExhausted`，不会产生部分
snapshot。进入 actor queue 前另按命令输入估算并原子预留字节，默认聚合上限 128 MiB，可通过
`SqliteCatalogConfig::with_manifest_queue_capacity_bytes` 调整；它限制排队驻留内存，不替代上述
单命令实际编码上限。

SQLite physical schema version 与 `CatalogCommand` wire version 是两个独立版本空间，不能用同一个
“v1/v2”标签推断彼此已实现。

当前补充状态（2026-07-14）：physical schema v5 以 O(1) schema migration 新增独立永久
`legacy_import_requests` replay 表，不重建、不扩展长期 `idempotent_requests`，并保持其历史 rows、
indexes 与 response bytes 不变；硬编码 migration checksum 和完整 schema-inventory golden 冻结
v1-v4 兼容性。迁移前、迁移后各执行一次有界 `quick_check`/`foreign_key_check`，因此损坏的 v4
Catalog 会在写入 v5 之前 fail closed，保留旧 binary rollback 边界。可信启动期内部 V2 legacy
import primitive 已能
在一个事务中创建 table、发布 Import snapshot、写入经过物理验证的 ExternalLegacy files、恢复
writer epoch、推进 revision 并在专表保存永久幂等响应；预处理复用既有单个有界 manifest worker
和命令字节预算。`zippy-storage` verifier 会从 canonical legacy-root fd 逐组件
`openat(NOFOLLOW|NONBLOCK)`，先验证 Parquet footer/Schema/row count，再计算完整 SHA-256。查询侧
pinned reader 会在同一打开 FD 上重新校验 external checksum，并在成功后回卷到 offset 0；managed
file 不承担该额外 O(file size) 成本。导入只能经不可克隆、绑定单个 Catalog instance 的 startup
session 执行；session drop/seal 或第一个普通 mutation 会永久关闭本实例的导入窗口，actor 仍只用于
审计。verifier 从实际比对 Parquet footer 的 Arrow Schema 计算 Catalog canonical hash，并把保留 proof
写入 file metadata，Catalog 事务要求它和 table command 完全一致。V1 与 V2 replay 表共享全局
RequestId 空间，任一方向的重复都会在 mutation 前冲突。Master 已经在不改变 frozen source hash 的
前提下提供强类型 persisted-file snapshot 和 Gateway-compatible effective scan order，Server V2
planner/executor 也已接线。执行器先走固定成本的 request-ID replay probe；它只返回有界 committed
response 或 `Missing` 以及同一只读事务观察到的 revision，不携带物理文件 fingerprint，因此只能作为
hint。Server 必须把 replay response 中可由当前 plan 推导的 request、revision、namespace/table、
definition/config、writer epoch、snapshot 形状以及 summary 文件数、legacy revision 和已知 row count
逐项比对。只有在 expected revision 上得到 `Missing` 时才允许扫描、校验和 hash 物理文件并首次
提交；replay 不重复 migration I/O。

每次 native startup 仍会在 ready 前，对固定 Catalog revision 下所有当前可见 ExternalLegacy 文件
执行 fail-closed 完整物理校验，包括 regular-file identity、Catalog size、SHA-256、Parquet footer、
精确 Schema、row count 以及读取前后 descriptor identity。因此 replay probe 不会把启动健康门降级成
metadata-only。ExternalLegacy 路径在验证后仍可被外部修改，校验与后续读取之间的 TOCTOU 只能靠
legacy root 静默/只读和逐 reader 重校验暂时约束；切换 SQLite/Query 权威前，必须把文件 copy/reflink
到 managed immutable storage，并通过受管 capability 打开。

V2 `discovery_order` 表示旧 Gateway 对 persisted files 应用
`(writer_epoch, source_segment_id, source_generation, created_at, file_path)` 后的零基稳定扫描位置，
不是 Registry vector index。Registry index 只保留为迁移审计信息；Server planner 必须在任何文件
读取前完成全 snapshot 路径去重和单表文件数准入，避免部分表先提交后才发现后续表不可导入。
raw snapshot 仍按 `file_id` 排序；只有 pinned scan view 根据 discovery ordinal 调度读取。Step 3 已将
QueryService 接到生命周期绑定的 persisted snapshot pin/lease capability API，并关闭 native query
descriptor 直接携带 raw path DTO 的入口。Gateway compatibility authority 仍能沿旧路径持有路径 DTO，
必须在后续 authority 切换中改为使用同一 pinned reader capability，不能绕过 pin owner 打开文件。

任务：

1. 新建 `zippy-catalog` 和 foundation-only migration framework。
2. 实现 SQLite 初始化、WAL、integrity check；checkpoint 与绝对 shutdown deadline 在接入
   server 前补齐。
3. 在独立 physical migration 中实现 namespace/table/schema/snapshot/file 表。
4. 实现 producer、subscription、lease、job、idempotency 表。
5. 实现类型化 `CatalogCommand` 和单 writer actor。
6. 实现多表同事务的 `CatalogQuerySnapshot`。
7. 实现 JSON registry importer 和 migration report。
8. 实现 shadow command fan-out 和 diff metrics。
9. 先切 status/list/get read path，再切 metadata write authority。
10. 实现 legacy JSON exporter，作为受控回滚工具。

验收：

- 同一 command 重放得到同一响应。
- 不同 payload 复用 request id 明确失败。
- 百万级 data_files 查询使用索引，不做全量 JSON clone。
- 在 transaction 前、中、后 failpoint 崩溃均可恢复。
- shadow mismatch 连续稳定为 0 后方可切 authority。

回滚：SQLite 未成为 authority 前直接关闭 shadow；成为 authority 后通过 exporter 回滚。

### Phase 3：Manifest Storage v2

目标：建立数据文件和 Catalog 的原子可见边界。

当前实施状态（2026-07-14）：managed data root 已实现 owner-only `server.lock`、非阻塞进程独占
锁、UID/权限/硬链接/inode 校验、崩溃自动释放和只读 layout open；初始化只在取得锁后写
`FORMAT_VERSION` 或补建 managed directories。`zippy server run` 和 legacy native master 入口会在
日志、snapshot、listener 之前取得 `[server].data_dir` 所有权；不同 endpoint 指向同一 root 时第二
实例 fail closed，干净 shutdown 最后释放，不完整 shutdown 保持 capability。可写
`SqliteCatalogConfig` 必须持有 `Arc<OwnedDataDirectory>`，Catalog worker 退出前不会提前释放锁。
通用 durable file primitive 已实现同目录临时 inode、file fsync、no-clobber publish、directory
fsync、取消清理和 symlink 拒绝。legacy registry snapshot 已迁入 data root 的 `catalog/`；默认
数据根发现旧 endpoint snapshot 时会 fail closed，只有显式声明离线导入并确认旧 endpoint 不活跃
后才迁移，显式数据根不会继承 endpoint 状态。Parquet candidate writer 已固定 UUID 路径、
Parquet profile、footer provenance、`sha256-v1`、no-clobber publication 和 schema 预检。
Catalog commit coordinator 已实现单 worker FIFO、64 条/128 MiB command/8 GiB candidate 默认预算、
durable intent sidecar、commit-unknown 原 request ID 重放、响应复核、取消安全和启动前恢复。
启动期 orphan reconciliation 已实现规范化物理扫描、durable intent 保护、`FileId + managed path`
同事务双键 probe、全程 revision 稳定性检查以及只读分类报告；扫描、验证、intent 总字节和报告
常驻元数据均有独立预算，多文件 intent 必须整体 exact 或整体 unreferenced。只读 report 方法刻意不删除文件，
但 server 在 admission 仍关闭的私有启动阶段会删除 candidate/intent 临时文件和完整复核后的 orphan，
随后再执行一次完整 reconciliation。在线 definitive rejection 只消费 coordinator 创建的不可复制
cleanup token，并在 Catalog publication exclusion 内完成 revision、引用、物理身份复核和 unlink；
普通历史文件删除仍依赖 snapshot lease 和 Catalog GC 状态机。反向
`StartupCatalogFileVerifier` 已按固定 revision 分页读取 active table/current snapshot，并在 ready 前
有界核验每个当前可见 managed 或 ExternalLegacy 文件。managed 文件通过 data-root/table directory
fd 逐级 `openat(O_NOFOLLOW)`；ExternalLegacy 逐组件拒绝 symlink，并复核路径身份、大小、完整
checksum、Parquet footer、精确 Schema、row count 及读取前后 descriptor identity。缺失、symlink
替换、身份变化或内容损坏均 fail closed；该完整检查在每次 native startup 执行，包括 migration
request-ID 已 replay、未重复扫描导入文件的启动。
该 gate 不声明任意历史 snapshot 可用；native query 只允许固定本次规划选择的 current persisted
snapshot，历史文件深度审计属于 maintenance。普通查询的进程内 snapshot pin、min pinned snapshot
和 safe-GC 排除已接线；logical drop、跨进程长 lease、active segment lease 和权威 Gateway 写入口仍未
完成，因此 legacy metadata 仍是 server authority，不能把本段基础设施视为 Phase 3 验收完成。显式设置
`[server].native_storage_enabled = true`（或 `ZIPPY_SERVER_NATIVE_STORAGE=true`）后，
`NativeStorageRuntime` 会在任何 listener 绑定前依次执行 Catalog open、pre-replay reconciliation、
durable intent replay、post-replay reconciliation 和 Catalog-to-physical current-snapshot verification；
只有双向报告完成且 revision 一致才允许 ready。supervisor 使用独立 OS 线程和 Tokio runtime；server
启动超时会先取消并把 owning handle 交给具名后台 reaper，再按时返回；reaper 持有 supervisor join
handle 和 data-root lock 直到 Catalog cleanup 完成。shutdown timeout 不会取消已接受 commit 或提前释放
data-root lock。该 gate
默认关闭，启用后失败必须阻止启动，不允许静默降级到 legacy-only。

当前 server 接线进展（2026-07-14）：Catalog-to-physical 验证现在会在同一轮固定 revision 分页中
额外生成有界、原子发布的 active-table bootstrap；默认最多保留 128 MiB table/schema/snapshot
metadata、schema IPC 与 decoded Arrow schema 常驻计费，可见 file descriptor 仍为批次局部。decoded
schema 使用递归字段大小、root metadata、Arc/allocator 裕量形成保守逻辑 charge；server 每解码一张表
就立即计入同一预算，超限会在创建 actor 前 fail closed。`zippy-ingest` 已建立独立 crate，启动期使用
Catalog 的唯一受限 persisted-schema decoder 复核 schema hash、table definition 列引用、current
version、writer epoch 和 committed log watermark，再一次性构建 server-owned table runtime registry。
每张 active table 会在 ready 前创建串行 actor；默认 per-table mailbox 64、全局 outstanding command
4096，均为立即有界 admission。server 现在还会启动独占 Catalog commit coordinator 的
`PersistController`：默认 64 个 accepted jobs、512 MiB 完整 retained input、4 路 Parquet writer，writer
并发取 process-wide `runtime.max_cpu_cores` 并受 64 路安全上限约束。同表只允许一个 accepted persist，
不同表可以并行；footer 在准入前完成 1 MiB 有界流式编码，schema、provenance、metadata、容器和 Arrow
buffer 都计入输入预算。shutdown 顺序为关闭 table actor admission、排空已接受 actor command、排空
PersistController 的 writer 与 Catalog commit、关闭 SQLite Catalog。actor 现在会逐请求复核 table、
snapshot、精确 Arrow schema、writer epoch 和连续 offset；写盘与 Catalog commit 在 actor turn 外执行，
receipt 回到同一 actor 后再校验 request/table/parent/version/operation/LSN/watermark 并推进本地快照。
同表 persist single-flight，快照观察不被写盘阻塞。storage 到 actor 的 admission/completion 结果显式
区分 request-local `Rejected` 与 runtime-terminal `Fatal`，不再根据错误码或 Advance/Replay 猜测；只有
terminal failure 会在响应调用方前关闭全局 admission、拉低 native/server health，并按 actor、controller、
Catalog 顺序清理。Catalog 持久 idempotency 记录存在期间，原 request ID 可在跨重启后返回原 receipt，
actor 不会把较新 snapshot 回滚；同 ID 不同 fingerprint 返回 Conflict，且不会毒化后续写入。Gateway
权威写入口仍未接入。并行 writer 只生成 affine 临时 candidate；完整 envelope replay probe 发生在
最终发布前，exact、fingerprint conflict 和 missing historical replay 都只删除临时文件，只有 missing
advance 才能进入 publication gate。共享发布许可从 no-clobber publish 一直转移到 Catalog actor 的
terminal result；拒绝清理取得独占许可，因此并发 Catalog mutation 不能穿过最后一次引用检查与 unlink。
底层 durable-file 错误还会区分未发布与“最终目录项已创建但目录 fsync 未确认”，后者仍携带 cleanup
authority。daemon 已每 50 ms 检查动态 readiness，并以共享 latch 保留首个 native 终态错误；每轮
先查故障再查信号，收到信号后还会复核。native runtime 终态故障后会把同 generation 的 ready record
原子迁移为带稳定 ErrorCode 的 failed，再关闭 server admission、执行有界清理并非零退出；cleanup 错误
或超时只作为附加诊断，不覆盖首因。正常信号清理前迁移为 stopping，清理中 status 不再从旧 ready
记录报告 healthy。detached outcome 同样保留稳定 ErrorCode，不再统一降级为 runtime_error。
但由于 Gateway authority、logical drop 和 active segment lease 仍未完成，legacy Gateway 仍是业务写
authority，不能据此宣称 native ingest 已对外可用。

当前 query snapshot 接线进展（2026-07-15）：`NativeQuerySnapshotCoordinator` 是
`NativeStorageRuntime` 持有的单实例能力，不由每个调用方临时创建。它先按稳定 `TableId` 去重排序，
再让有 actor 的 native 表在串行写入边界观察 `writer_epoch`、persisted snapshot version 与 committed
offset；没有 actor 的 legacy shadow persisted-only 表直接选择 Catalog current snapshot。所有表随后在
同一个 query-attributed Catalog read 中全有或全无地 pin，冲突进行有界重试，任一解析、预算或读取失败
都通过 RAII 回滚全部 pin。Pinned storage snapshot 至少包含一张物理表；常量等无表查询后续使用不持有
data-root capability 的独立计划类型，避免出现未计入 `active_pins` 的空 owner。

每个 pin 记录 `query_id`、唯一 `lease_id`、table、snapshot version 和取得时间，供状态与故障审计；
普通短查询只保存在进程内 registry，不对 SQLite 热路径逐条写 lease。对上暴露的
`TableReadSnapshotDescriptor` 只有 table/schema/snapshot/writer/offset 与 segment identity 审计字段，
不包含文件路径；执行器只能从不可伪造、绑定 Catalog instance 的 pinned reader capability 打开文件。
QueryService 在复制 task capability 前先预留 query/task/CPU/memory 预算，并用 storage origin nonce 拒绝
其他 runtime 伪造或复用 guard。物理 task admission 只接受 server 内部选择的 table index，不把任意
capability callback 暴露给执行器；生产构建中的 task permit 也不提供 resource 提取接口，因此 capability
不能通过 clone 或回调副作用逃出 permit 生命周期。

Query registry 持有一个可撤销 snapshot root：未执行计划到 TTL、计划 drop、cancel、timeout 或 shutdown
都会恰好一次撤销 pin，即使调用方仍泄漏 plan token；已经运行的 physical task 则保留 pin 到最后一个
task permit 释放。Native storage shutdown 先关闭 snapshot coordinator、Catalog 通用 runtime-pin gate
和 table admission；Catalog gate 的关闭会等待所有并发 pin admission 越过线性化点，随后再等待 in-flight
snapshot planning、QueryService 和 Catalog runtime pins 都归零，最后才通知 storage supervisor 关闭
Catalog 与 data-root capability。即使调用方保留了 shutdown 前克隆的 raw Catalog read handle，也不能在
零 pin 检查后插入新 pin。该阶段
只完成 persisted files 的生命周期绑定；active/sealed segment guard、DataFusion operator 取消传播以及
Gateway compatibility reader 的能力化仍分别属于后续 query、ingest 和 authority 切换工作。

QueryService registry 已从弱引用清理骨架升级为强持有、严格有界的 retained registry。每个查询实例除
`QueryId` 外还有进程内单调 `generation`；terminal status 默认保留 5 分钟，registry 默认最多 4096 条。
容量满时只允许淘汰已经 terminal、physical task 为零且 snapshot root 已从 registry 分离的最老记录，并
为提前淘汰的 QueryId 保留到原 retention 截止时间的 tombstone；tombstone 默认上限 65,536，耗尽时
fail closed。所有 snapshot root 操作同时校验 QueryId 和 generation，因此 retention 到期后即使旧 plan
token 或 execution scope 仍被调用方持有，它们的 drop/cancel 也不能释放或终止同 ID 的新查询实例。
仍有 physical task 的 terminal query 不进入可淘汰堆，避免以缩小 registry 为名提前复用身份或丢失 pin
撤销根。终态保留只保留控制状态，不保留已经可释放的 snapshot、task、CPU 或 memory 预算。

QueryService 现在还维护严格有界的 deadline `BTreeSet`。`submit` 只登记一个 generation-bound
`PlanningExpiry=min(request deadline, submitted_at+max_planning_duration)`；native 默认 planning 上限为
30 秒，与 legacy plan TTL 对齐。`complete_planning` 在同一线性化点删除 PlanningExpiry，再登记覆盖
`Planned`/`Running` 的 request deadline（若有）和只覆盖未执行 `Planned` 的 plan TTL。开始执行会删除
plan TTL，任一 terminal 转换都会精确删除全部对应 key。到期处理只弹出当前时刻已经 due 的实例，不再
周期性复制和扫描整个 registry；旧 generation 的 timer 即使残留也不能命中新实例。默认 100 ms 的具名
sweeper 使用可中断 `Condvar`，只持有 coordinator weak reference，
QueryService shutdown 会发停止信号并 join，不能遗留 detached sleeping thread。terminal 转换在
global/query 锁内只更新状态，随后在锁外通过 watch channel 发布一次终态通知；physical task permit 可
取得不暴露 reader capability 的 cancellation handle，既保留低成本 atomic 检查，也能异步等待主动唤醒。

查询控制契约不再使用裸 `QueryId`。`QueryInstanceId` 将 caller-assigned ID 与非零 generation 绑定，
execution token、result stream、status 和 cancel 均携带这一 exact-instance fence；保留期后同 ID 新实例
出现时，任何旧 owner/admin 请求都返回 `NOT_FOUND`，不会回退命中新 generation。generation 只解决 ABA，
不等价于授权：native QueryService 的 submission 已要求 engine-bound owner capability，并把 session 与
owner nonce 固化进 control；普通 owner 和 admin 使用不同 handle，同 session 的另一 capability
也不能读取或取消该实例。仅供单元测试的裸 QueryId helper 在生产构建中不存在。

Stage 4.3 planning 前移小步已完成（2026-07-15）：生产 API 先调用 `submit(owner, metadata, budget)`，
由 QueryService 服务端时钟记录 `submitted_at_ns`、分配 generation、以 `Planning` 登记并返回私有字段、
不可 `Clone` 的 `QueryPlanningToken`。token 暴露 exact instance 和 cancellation，且必须被
`complete_planning` 或 `fail_planning` 消费；未消费 drop 精确取消该 generation。显式 planner error 落
`Failed`，不会被 token drop 覆盖成 `Cancelled`。超时、owner/admin cancel、shutdown 和 complete 竞争均
遵循 first-terminal；late complete 不会挂载 snapshot。

`NativeQuerySnapshotCoordinator::pin_persisted` 现在必须借用 planning token，因而生产类型签名不再允许
先 pin、后登记。整个 resolve tables、table actor snapshot wait、Catalog pin/read future 与 terminal watch
使用 cancellation-priority `tokio::select!` 竞争，而非只在 await 前后轮询；timeout、cancel 或 shutdown 会
主动 drop 当前 planning future。Catalog resolve/read 的迟到 oneshot 响应只会被丢弃，TableActor snapshot
命令仍完成并释放其有界 permit，Catalog query pins 则由 future 内同步 RAII guard 立即全量撤销。成功返回
后仍复核 first-terminal，防止同一 poll 边界的 late cancel 挂载 snapshot。`complete_planning` 使用固定
`global -> query` 锁序，在一个
线性化点同时挂载 snapshot root、增加 owner 计数、安装 server-side `planned_at`/catalog/snapshots、加入
plan TTL 并执行 `Planning -> Planned`。snapshot owner 容量、plan 过期或其他 completion 失败不会留下
无人持有的 `Planning`。一次性 token 使安全 Rust 调用方无法重复 complete；foreign service token 被
origin nonce 拒绝并由原服务 drop-cancel。

native `QueryRuntimeStatus` 分开记录可信 `submitted_at_ns`、可选 `plan: QueryPlanStatus`、可选
`started_at_ns` 和 `finished_at_ns`。`plan=None` 明确表示尚未 bind；`Some` 内的空 snapshots 才表示合法
无表计划，避免用零 revision、空数组或调用方 supplied `planned_at` 伪造规划指标。测试专用
`*_at`/planned-registration helper 只在 `cfg(test)` 存在，用于确定性 deadline 和保持旧 Planned+ 单测；
生产调用方只能走 submit-first 流程。

Stage 4.4 资源账本小步已完成（2026-07-15）：`CoordinatorState` 在同一把 global 锁内维护只包含
非空项的 session usage ledger，按 session 汇总 logical query、physical task、CPU slot、task memory、
result permit 和 result memory；没有增加第二把 session 锁，也不会让 session 状态与全局 admission 发生
时间差。查询、session 和全局三层把 task/result memory 计入同一个硬上限，状态同时保留分项和总量。
当前不配置 per-session quota：owner capability 仍由 crate 内调用方按裸 `SessionId` 创建，在 Gateway
建立可信 connection/session authority 前加配额只会形成伪隔离。

Task admission 现在先生成不可提取的 RAII reservation，再执行 snapshot-bound resource builder；builder
返回错误或 unwind 都会归还 query/session/global 的 task、CPU 和 memory 计数。内部
`QueryResultPermit` 同样用 RAII 计量结果缓冲，并且 admission 失败会正常析构调用方传入的结果对象。
每查询和全局 result permit 数量还分别受现有 active-task 上限约束，不能通过大量一字节 charge 先耗尽
permit/control 元数据。`QueryTaskPermit::into_result` 在同一个 `global -> query` 临界区释放 task/CPU
charge 并取得 result charge，使已经占满预算的 operator 可以把同一批内存无缝交给 delivery，而不需要
双重计费或制造未计费窗口。payload、result 或 snapshot 析构 unwind 时，独立 scope guard 仍会归还
reservation 和 snapshot-owner 计数。

成功执行到 EOF 时，QueryService 先记录 `execution_complete=true`。若仍有 result permit，状态暂时保持
`Running`，禁止再交付新结果；最后一个 result permit 释放时才原子进入 `Finished`。这样执行 scope 可以在
算子与 reader 已全部释放后结束，而已经交给慢客户端的批次仍保持精确内存计费。计算或传输错误、cancel、
timeout 和 shutdown 形成的 terminal query 在最后一个 task 离开后立即撤销 snapshot root，但只有 task、
retained 和 result permit 都归零后才进入 retention/eviction heap。shutdown 也会等待 result permit，因而
不会把仍被发送的缓冲误报为已 drain。内部 native persisted stream 已用 permit-owned batch 覆盖调用方取得
批次后的长期持有；Legacy/Gateway 的公开 `RecordBatchStream` 仍未切换到这条路径。

Stage 4.5 执行失败终态小步已完成（2026-07-15）：`QueryExecutionScope::fail(error)` 消费唯一执行
scope，并在同一个 `global -> query` 临界区只允许 `Running -> Failed`。QueryService 会把真实 query ID
绑定到算子错误、释放 logical admission、删除 deadline、设置 cancellation flag，并在锁外发布一次终态
通知；scope drop 不会再把已记录的执行错误覆盖成 `Cancelled`。失败可以与仍存活的 task/result permit
并存，后者收到取消后仍按既有 RAII 规则退出：最后一个 task 撤销 snapshot root，最后一个 result permit
才使 terminal 实例进入 retention。执行失败与 owner/admin cancel、timeout 或 shutdown 竞争时继续遵循
first-terminal-wins，迟到的 `fail` 返回已经存在的终态错误。

Stage 4.6 服务端 plan TTL 小步已完成（2026-07-15）：`query.plan_ttl_ms` 和
`ZIPPY_QUERY_PLAN_TTL_MS` 统一配置未执行 plan 的存活时间，默认 30 秒，允许范围为 1 毫秒到 24 小时。
server runtime 将同一值注入当前 authoritative Legacy/Gateway adapter 和 staged native QueryService，
避免迁移期同一配置出现两种行为。native `complete_planning` 使用服务端完成时刻重写
`planned_at_ns`，并计算
`expires_at_ns=min(request_deadline, completed_at_ns+plan_ttl)`；没有 request deadline 时只使用服务端
TTL。request deadline 覆盖 Planned/Running；server plan TTL 只覆盖 Planned，开始执行后立即删除。

这一小步仍是 native staging，不是 authority 切换。可信 session policy、完整外部 `QueryStatus` 和 Gateway
transport 背压仍未完成；内部 persisted result stream 已接入实际取消传播，但当前 Python/Gateway
`collect_stream` 仍绕过 native QueryService，因此不能据此宣称生产查询已经获得多租户隔离或统一资源治理。

任务：

1. 定义 `DataFileDescriptor` 和 checksum contract。
2. Persist target 改用 UUID 文件名和 tmp/final 两阶段。
3. 实现 fsync file + rename + fsync directory。
4. 使用 `CommitDataFiles` 发布 manifest snapshot。
5. Query snapshot 从 Catalog 读取 typed files。
6. 实现 added/removed snapshot visibility。
7. 实现 persisted/live source segment identity dedup。
8. 实现 snapshot lease 和 min pinned snapshot。普通进程内 persisted query pin 已完成；跨进程长 lease
   与 active segment lease 待后续阶段。
9. 实现 orphan scanner、verify 和 safe GC。
10. 将 drop table 改为 logical drop + asynchronous physical GC。

验收：

- Persist 每个步骤崩溃后只出现 committed file 或 orphan，不出现 dangling catalog reference。
- Compaction 前后并发查询不重不漏。
- 长查询持有旧 snapshot 时 source file 不被删除。
- Checksum 损坏返回 `DATA_CORRUPTION`，不静默跳过。

回滚：保留 legacy file metadata reader；新文件 metadata 可导出为旧 persisted_files。

### Phase 4：Maintenance worker

目标：把大 IO/CPU 任务移出 server 热路径。

WebUI、maintenance 和 compaction 的进程所有权、启动/关闭、健康、安全及资源预算契约见
[`server-owned-auxiliary-services.md`](server-owned-auxiliary-services.md)。该文档当前只冻结目标设计，
不会让 `zippy server run` 自动启动辅助进程。

任务：

1. 新建 `zippy-maintenance` binary。
2. 实现持久 job lease/state machine。
3. 迁移 compaction planner/executor。
4. 实现 GC、orphan scan、checksum verify。
5. 实现 catalog/WAL checkpoint job。
6. 增加 CPU/memory/IO/temp quota。
7. 增加交易期/收盘期显式 profile 配置。

验收：

- Worker 强杀后任务可重领。
- 相同 compaction job 重放不会产生重复可见文件。
- Compaction 压力下 control/ingest 性能不突破既有 gate。
- Server 可在 maintenance unavailable 时以 degraded 状态提供基础读写。

回滚：禁用 maintenance，新 compaction 不执行；不回退 manifest catalog。

### Phase 5：DataFusion QueryService

目标：服务器拥有完整查询计划和执行。

Stage 5.1 Arrow/DataFusion ABI spike 已完成（2026-07-15）：新增独立 `zippy-query` crate，固定
DataFusion `44.0.0`，只启用 `parquet` 与 `recursive_protection` feature。该版本声明 Arrow/Parquet
`53.3.0` 兼容范围，当前锁图将
Zippy workspace、DataFusion 和 Parquet 统一解析为 `53.4.1`，没有第二套 Arrow 类型。编译期测试把
workspace `RecordBatch` 直接赋给 DataFusion re-export 类型，并单独验证 clone 后 Arrow value buffer 地址
不变；另一项运行时测试把 workspace batch 注册到隔离的 `SessionContext` 并执行 SQL。不允许增加
IPC/FFI round-trip 作为版本不兼容的补救。当前 Rust `1.86` 工具链还要求锁定传递依赖
`comfy-table 7.1.4` 和 `psm 0.1.28`；后者避免 recursive protection 解析到要求 Rust 1.88 的 archive
writer。升级任一依赖前必须先提升并验证 workspace Rust 基线。

此提交只建立查询 crate 和 per-session context policy，不把 DataFusion 依赖接入 `zippy-server`、ingest、
table actor 或 callback 热路径，也不宣称查询 authority 已切换。下一小步先让 native planner 产出不含
服务端时间的 capability-free draft，再实现持有 QueryService task permit 的 pinned Parquet source；
DataFusion 不能取得裸路径、裸文件或脱离资源账本的 reader。

Stage 5.2 time-free native plan draft 已完成（2026-07-15）：`QueryPlanDraft` 是 `zippy-server` 内部、不可
序列化的 capability-free 类型，只包含 query ID、Catalog revision 和 table snapshot/schema versions。
`NativeQueryReadSnapshot::plan_draft()` 不再接收 `planned_at_ns/expires_at_ns`，planner 因而无法表达或伪造
服务端时间。`QueryResourceCoordinator::complete_planning` 在校验 draft 与 opaque snapshot descriptor
完全一致后，才以服务端完成时刻、server plan TTL 和 request deadline 唯一构造公开 `QueryPlanInfo`。
公开 wire/status 类型保持不变；测试专用 planned-registration helper 虽继续接受最终 info 以复用资源账本
用例，但会先丢弃其中时间字段并转换为 draft，生产 API 不存在该入口。

draft 也不能保存 `PinnedDataFile`、reader、路径或其他 lease capability，因此未执行 plan 过期时，registry
撤销 snapshot root 后不会有隐藏的文件 pin。下一小步是在 execution 开始后先取得 QueryService task
permit，再从 snapshot 派生单文件 capability，并让 Parquet source/range readers 共同持有该 permit。

Stage 5.3 permit-owned pinned Parquet source 已完成（2026-07-15）：Catalog 的
`PinnedDataFileReader` 新增 positioned range read，但仍不暴露 path、裸 `File`、descriptor clone 或可脱离
lease 的 reader。`NativeQuerySnapshotCoordinator::admit_file_task` 严格先向 QueryService 预留 task/CPU/
memory，再从 exact snapshot 派生 `PinnedDataFile`；`QueryTaskPermit::try_map_resource` 在 bounded blocking
worker 打开文件时保留同一 reservation，错误或 panic 通过 RAII 完整回滚。

`zippy-query::AdmittedParquetSource` 及其每个 `ChunkReader` cursor 共同持有 server 提供的完整 permit owner，
因此 outer source、execution scope 或 logical query 提前结束，都不能让仍在读盘的 cursor 脱离资源账本或
Catalog pin。真实 Parquet 测试覆盖 persisted write、exact snapshot、文件准入、解码、取消和 final-cursor
释放；取消后不再返回字节，最后一个 cursor drop 后 task/CPU/memory/pin 才同时归零。

每次 `ChunkReader::get_bytes` 还必须先取得 exact-size QueryService result-memory permit，再分配和填充 range
buffer；返回值使用 `Bytes::from_owner(QueryResultPermit<Vec<u8>>)`，所以所有 zero-copy `Bytes` clone 共享
同一 reservation，只有最终 clone drop 才释放 result permit 和字节额度。大 range 的 positioned I/O 按
1 MiB 分块，在块间重新检查取消；分块只负责取消响应，动态 result permit 才是内存硬边界。

Stage 5.4a permit-owned async Parquet reader 已完成（2026-07-15）：
`AdmittedAsyncParquetReader` 实现 Parquet `AsyncFileReader`，把 range read 和 metadata parsing 交给
`tokio::spawn_blocking`，避免同步 descriptor I/O 阻塞 async runtime worker。每个 blocking job clone 的仍是
完整 admitted source，并持有所有 source clone 共享的 capacity-one operation permit，因此调用方 drop future
时既不会提前归还 task/CPU/pin，也不能反复 poll/drop future 向 Tokio blocking queue 注入无界 detached job；
job 返回的 `Bytes` 继续携带 exact-size result-memory permit。单元测试覆盖 async range/footer metadata 以及
drop future 后第二个 job 必须等待真实 I/O 退出，server 真实 persisted 文件测试覆盖 permit-owned async
metadata 路径。同一个 admitted source 在执行层只能选择同步或异步 adapter 之一，不能并发混用，因为该
operation gate 只约束 async blocking job；生产 DataFusion 路径固定使用异步 adapter。

Stage 5.4b capability-free persisted planning primitive 已完成（2026-07-15）：
`PersistedFileKey` 把 Query snapshot 中的 table/file scan ordinal 与稳定 `TableId/FileId` 绑定，
`PersistedFileDescription` 再保存 snapshot/schema version、文件大小和行数；所有字段都是纯值，不包含 Catalog
物理位置、reader、pin、callback 或 runtime capability。`PersistedTableDescription` 验证 ordinal 连续性、身份和
版本一致性、重复文件及 `usize`/聚合溢出，同时保留 Catalog 计算的 decoded-schema memory charge，供执行准入
使用。Arrow schema metadata 属于逻辑 schema，原样保留，但不能被解释为物理文件定位能力。

`ZippyTableProvider` 只生成 planning-only `ParquetExec`：`FileMeta.location` 是单段 synthetic key，typed
extension 携带同一纯文件描述，私有 marker factory 对任何 persisted-file I/O 都 fail closed。Filter pushdown
声明为 `Inexact`，Parquet predicate 仅做 pruning，DataFusion 必须保留上层精确 `FilterExec`；有 filter 时不能
向 scan 下推 limit。file-range repartition 已关闭，当前所有文件放进一个执行 partition；DataFusion 仍可能
预打开下一个文件，因此并发硬边界始终是 QueryService file-task/CPU admission，而不是 file group 数量。
空表使用 projected `EmptyExec`，无需虚构 reader capability。

长期 QueryService token 仍不得保存 DataFusion `ExecutionPlan/Expr`、`TaskContext/SessionState/RuntimeEnv` 或
真实 reader factory；这些对象可能携带 UDF、object-store、memory-pool 和 spill 能力。下一阶段在
`begin_execution` 之后从同一 snapshot 投影 table description，创建受限 execution context，再把完整物理树中
的 marker factory 全部替换为持 `Weak<NativeExecutionLease>` 的 server factory。factory 必须联合核验 typed
extension、synthetic location、table/file ordinal、ID、snapshot/schema version、size，并拒绝 missing/unknown
extension、location mismatch 与 file range；实际 open/checksum 继续延迟到 admitted blocking job。

Stage 5.4c execution-retained memory accounting 已完成（2026-07-15）：QueryService 将 capability-free
Schema、投影 manifest、physical plan、Parquet metadata 和 operator arena 作为独立 retained 资源记账。
task、retained 和 result memory 共用 query/session/process 三级硬上限；task 构建峰值向 retained 实际占用的
转换在同一个 `global -> query` 临界区原子完成，同时释放 task count 与 CPU slot，且 retained charge 只允许
小于或等于原 task memory。构建失败、panic、cancel 或 draining 竞态都会先析构产物，再通过 RAII 回滚 task
reservation。retained permit 会阻止 `Finished`、terminal eviction、session 清理和 shutdown complete，资源
析构始终先于账本额度释放；原 snapshot root 仍可在 physical task 归零后释放，因为 retained 类型不得携带
reader、pin、路径或其他存储 capability。

Stage 5.4d planning snapshot peak admission 已完成（2026-07-15）：`QueryPlanningToken` 只能在
`Planning + Accepting` 状态预留 snapshot memory；该 permit 不占 task count 或 CPU slot，但和 task、retained、
result 共用 query/session/process 三级内存硬上限。总预留由三个 checked 部分相加：Catalog table-resolution peak、
exact pinned-snapshot peak，以及 1 MiB 固定加每个输入引用 4 KiB 的 Server coordination envelope。resolution
estimator 覆盖最多 1 MiB 请求、4 MiB 原始响应、解码/重复 descriptor 和 lookup container；exact estimator 覆盖
SQLite raw value 的 32 倍解码膨胀、table/file/lineage 容器、最多 128 MiB 的单 schema 解码瞬时空间、pinned
scan index/schema charge sidecar 和 allocator margin。Server 初始 exact guardrail 为 2 MiB raw response、4096 个
visible files 和 16384 条 source-lineage row；2 MiB 不是最终记账值，status 展示的是三部分完整保守峰值。

非 Clone、非序列化且字段私有的 concrete RAII lease 由 Catalog 启动时配对的 issuer/verifier 签发和验源；普通
`SqliteCatalog::open` 不泄露 issuer，native runtime 使用显式启动 API取得 issuer，而 read handle 只持 verifier。
reservation identity 由进程级 authority ID 和 authority 内递增序号组成，descriptor 还包含 `reserved_bytes` 与完整
`QueryInstanceId`。因此依赖方不能通过实现公开 trait、复制 descriptor 或创建另一套 issuer 伪造当前 Catalog 的
能力。Catalog 在任何排队、pin 或分配前联合核对 origin、额度和 attribution。所有公开 pinned-snapshot API 都必须
显式接收 lease；query API 拒绝无 attribution 或错误 query ID，通用 maintenance API 拒绝 query-attributed lease。

issuer 的 owner factory 把 QueryService 的私有 accounting permit 结构性装入 concrete lease；QueryService 同时按
`reservation_id -> 完整 descriptor` 记账。lease 先进入 table-resolution control queue/SQLite worker，随 resolved
response 原样返回，再进入 exact-snapshot queue/worker；caller 在任一排队或同步读取期间取消，只会关闭 response，
不会提前退还预算。成功后 lease 随 response 进入 `PinnedSnapshotOwner`，并继续跟随 plan token、execution scope 和
最后一个派生 task；读取错误、worker panic、response send failure、pinned-owner 构造失败和最终 owner drop 都只
释放一次。每次 snapshot conflict retry 都重新准入，不会复用已经释放的 reservation。

QueryService 的生产 `complete_planning` 只接受私有构造的 `NativeQueryReadSnapshot`，不再接受调用方提供的任意
opaque guard 或独立 permit。它从 pinned owner 读取实际 descriptor，并在同一个 `global -> query` 临界区按 authority、
reservation ID、bytes 和 query generation 全等核验账本后立即安装同一 snapshot root；一个无关的 1-byte permit
既不能代替真实快照额度，也不能在检查与安装之间制造 TOCTOU。terminal retention、session 清理和 shutdown 会等待
外部 worker-owned permit，而 `finish()` 不要求它预先归零：当 physical task 归零后先从 registry 取出 snapshot
root，在锁外析构 root，再由内嵌 permit 按 exact reservation 退账，避免形成生命周期环。

当前为 fail-closed，成功构建后仍保留保守峰值而不缩减为实际 owned bytes，因此会压缩同一查询可用的执行内存。
后续可增加只减不增的 actual-size reclassification，但不得先分配再追认额度。下一步是在 execution 开始后从同一
snapshot 投影并记账 exact table description，再接入 `NativeExecutionLease` 和延迟文件准入 factory。

Stage 5.4e exact persisted snapshot projection 已完成（2026-07-15）：execution 开始后，Server 以整个
`NativeQueryReadSnapshot` 为单位只申请一个 task，而不是逐表申请 task 或 retained permit。第一次短借用 snapshot
只读取 Catalog 预计算的 `PinnedSnapshotProjectionProfile` 标量；它包含表数、可见文件数和 decoded-schema charge
总额，不调用会 clone owner 的 table/file capability API。QueryService 完成 task/CPU/memory reserve 后，第二次短
借用才允许 clone pinned owner 和最小 path-free audit sidecar。因此低预算、取消或 shutdown 在 reserve 阶段失败时，
不会提前派生 pin capability 或开始 schema decode。

投影按 table ID 和 Catalog scan ordinal 顺序构造一个 capability-free `ProjectedSnapshotManifest`。Server 联合校验
query ID、Catalog revision、table count、连续 table/file ordinal、严格递增且唯一的 `TableId`、selector、table/
schema/snapshot descriptor、文件 table identity、Committed/visibility interval、非零版本/大小和 schema 单调性。
每张表的 Arrow schema 必须由同一个 `PinnedTableManifest` 通过 table-aware decoder 解码，decoder 得到的 retained
charge 必须与 Catalog pin 时预计算值完全相等；最终仍经过 `PersistedTableDescription::try_new` 复核重复 `FileId`、
snapshot 绑定和平台/聚合溢出。任一表失败或执行取消都会析构已完成的部分并由 task RAII 一次回滚，不会发布部分
manifest。

retained charge 覆盖全部 decoded schema、manifest/table/file 容器和 allocator margin；构建 task peak 在完整 retained
输出之外，再独立覆盖一次 128 MiB 最大 schema decode transient、audit/校验临时容器和 Vec-to-Arc 重叠。该 transient
不能复用 snapshot planning permit 的保守余量，因为后者不是可独占转移的子额度。成功转换在 QueryService 的同一
临界区释放 task count/CPU/peak memory，并登记一个 `ProjectedSnapshotManifest` retained permit。synthetic key、
`PartitionedFile`、DataFusion physical plan、metrics 和执行 operator 分配不在该 charge 中，后续必须分别由
`PhysicalPlan` 与 `OperatorArena` permit 覆盖。

Stage 5.4f bounded persisted plan binding 已完成（2026-07-16）：`PreparedPersistedPlan` 在解析 SQL 前先按 limits
申请不依赖解析结果的保守 preflight task；profile 生成后再以 profile 中的 exact build peak 覆盖异步 prepare 和 bind
全过程。profile format version 参与指纹，preflight、profile、prepare、bind 的错误按稳定分类映射到
`INVALID_ARGUMENT`、`DATA_CORRUPTION`、`UNSUPPORTED_PLAN`、`RESOURCE_EXHAUSTED` 或 `INTERNAL`，不解析错误
字符串。

zippy-query-owned 全树 binding 会唯一替换全部 planning marker，并在替换前后复核 synthetic location、typed
extension、table/file ordinal、ID、snapshot/schema version、file range 和 Parquet options。缺失、重复、普通 Parquet
scan 或任意 metadata 错配都会 fail closed。实际生成的 `ParquetExec` 显式关闭 page-index 和 bloom-filter read，bind
校验也拒绝重新开启；row-group statistics pruning 保留。reader factory 替换基于原节点 clone，因此不会丢失已经
核验的 options。

非 Clone 的 `NativeExecutionLease` consuming 地接管原始
`QueryRetainedPermit<ProjectedSnapshotManifest>`，并在 QueryService 的原子 rebuild 中把 manifest task/retained
账转换为独立 `PhysicalPlan` retained 账。bound plan 和 file factory 只持有 weak execution authority，复制或长期
保留 DataFusion plan 不能延长 query、snapshot pin 或文件读取能力。`finish`、`fail` 和 drop 都先关闭 reader
admission；已发布终态优先于资源冲突返回，避免 admin cancel 已获胜后又向调用方伪造新的 `CONFLICT`。

每个 persisted file reader 先申请 64 KiB task memory，实际打开后读取并校验精确 8-byte Parquet trailer，再按
`64 KiB + serialized_footer_bytes * 64` 原子扩充同一个 query/session/process task reservation，成功后才把 reader
交给 DataFusion。扩充拒绝不改变任何一级账本；reader、task permit 和 snapshot owner 共寿命。合法的 Zippy-managed
Parquet 文件由该公式覆盖；若未来允许不可信本地文件，恶意 Thrift collection length 仍需要 bounded decoder 或独立
进程/allocator 硬限制。

Stage 5.4g internal persisted result stream 已完成（2026-07-17）：`NativeExecutionLease::execute` 先用一个 build
task 申请配置的 operator pool、execution-context retained margin 与构建瞬时余量，构建成功后原子收缩为 pool 加
context margin 的 `OperatorArena` retained permit。每次执行创建隔离的 DataFusion runtime，使用与配置 pool 同额的
`GreedyMemoryPool`，并显式关闭 disk manager；因此 sort/hash/aggregate 等算子超过准入额度时失败，不会写入未受
配额管理的临时目录。执行期间另持有一个 execution-driver task permit，其 CPU slots 等于 session
`target_partitions`，并计入固定 driver memory；persisted file task 在此基础上独立计费，形成保守的总并发边界。
bound plan 通过 DataFusion 单流执行接口按需拉取 batch，不做全量 collect。

每个向下游返回的 `NativeQueryBatch` 按 Arrow `get_array_memory_size()`、递归 schema retained charge 和固定控制块
余量申请独立 result permit，permit 跟随 batch 到最终 owner drop。共享 schema 在各批次重复计费以保持单批次可独立
存活的安全上界。EOF 路径先 drop DataFusion stream 和 `OperatorArena`，确认 reader/task/retained 已归零，
再标记 execution complete；若下游还持有批次，查询保持不可继续产出的 `Running`，由最后一个 result drop 转为
`Finished`。stream drop 则先发布 `Cancelled`，随后释放 DataFusion、operator 和 reader 资源。terminal query 的
snapshot root 在最后一个 file task 离开后即可撤销，已经物化的批次只保留内存账，不阻塞 compaction。

Stage 5.4g 完成时，Arrow batch 仍由 DataFusion 产出后才立即申请 result permit：生产过程只有
`OperatorArena` 的间接边界，尚不存在 batch-specific 的产出前 reservation。该缺口由 Stage 5.4i 针对首批
persisted fixed-width streaming 子集关闭；复杂算子仍需后续进程级 pool、受配额 spill/temp 与 transport
backpressure。ActiveSegmentExec 也尚未进入这条流。

本阶段故障矩阵：

| 边界 | 必须结果 | 自动化证据 |
| --- | --- | --- |
| preflight/profile 拒绝 | 发布 `Failed`，task/retained/memory 归零 | `preflight_and_profile_failures_publish_failed_and_release_resources` |
| query instance/revision/storage issuer 错配 | prepare 前 fail closed，不派生 reader | `foreign_instance_revision_and_storage_issuer_fail_before_preparation` |
| bind metadata 错配 | 原 manifest 与 build task 同时回滚 | `bind_failure_rolls_back_build_and_manifest_reservations` |
| finish/fail/drop | 保留第一个终态，执行 authority 不可复活 | `successful_finish_failure_and_drop_release_execution_authority` |
| open 通过首检但尚未取得 execution mutex | close 后第二道门禁拒绝，不产生 task/pin | `reader_admission_second_gate_linearizes_finish_fail_and_drop` |
| reader 已准入后 finish | 拒绝成功完成、关闭新 reader、旧 task/pin 随 reader drop 归零 | `real_reader_finish_closes_admission_and_drains_task_and_snapshot_pin` |
| reader 已准入后 fail/drop | 保留 `Failed`/`Cancelled`，旧 task/pin 随 reader drop 归零 | `real_reader_fail_and_drop_keep_task_pin_until_reader_drop` |
| admin cancel 先于 finish | finish 返回已发布的 `Cancelled`，不改写为 `Conflict` | `cancelled_lease_with_reader_finish_returns_first_terminal_error` |
| 合法 footer task 扩充 | 三层 task memory 精确等于计算 charge | `admitted_parquet_cursor_retains_task_and_pin_until_final_drop` |
| metadata 足额但 decoded window 超预算 | sticky `RESOURCE_EXHAUSTED`，reader 不发布且无重复 open | `decoded_output_growth_rejection_is_sticky_and_releases_task_and_pin` |
| footer grow 与 admin cancel 竞争 | 只允许 grow-first 或 cancel-first，三层账本一致 | `task_memory_growth_and_admin_cancel_are_linearized_across_all_ledgers` |
| page-index/bloom 被篡改 | planning/bind 双重拒绝 | `binder_rejects_tampered_manifest_metadata_before_opening` |
| operator context 超预算 | runtime 构建前发布 `Failed`，plan/pin/账本归零 | `operator_context_budget_rejection_fails_and_releases_execution_lease` |
| EOF 时下游仍持有 batch | operator/reader 先释放，最后一个 result drop 后进入 `Finished` | `native_stream_accounts_batches_and_finishes_after_delivery_drains` |
| result stream 提前 drop | 先发布 `Cancelled`，batch 内存继续计费，snapshot pin 可撤销 | `dropping_native_stream_cancels_before_result_reservation_release` |

Stage 5.4g 已完成 persisted-only 的内部 native result stream，但当时尚未对外暴露 Gateway capability。decoded Arrow
batch 产出前准入、ActiveSegmentExec、DataFusion process-wide memory pool、spill/temp quota 和 transport backpressure
仍需按后续阶段补齐，不能把 per-execution operator arena 误当成完整的进程级查询资源治理。Stage 5.4i 已关闭首批
persisted fixed-width streaming 子集的 decoded-output 缺口，其余边界仍保持 fail closed。

Stage 5.4h Server-owned persisted query canary 已完成（2026-07-17）：`[query.canary]` 默认关闭，且要求
`server.native_storage_enabled=true`。Server runtime 在 native storage readiness 之后构造 canary service，shutdown
时先关闭 canary admission，再关闭 snapshot admission，并等待 QueryService、reader 与 runtime pin 排空。canary 的
公开 Rust handle 只允许操作自己签发且保留在有界 instance registry 中的 query，不能把内部 admin capability 用于
其他 QueryService 工作。Gateway `query_sql` 和 `query_manifest_snapshot_pinning` capability 继续保持 `false`。

一次 canary run 同步取得 `QueryInstanceId`，再异步执行 submit、persisted snapshot pin、plan completion、execution、
manifest projection、physical prepare/bind 和 pull-based result digest 全链路。每个 `NativeQueryBatch` 的 result permit
覆盖完整 digest 处理寿命，处理完成后先 drop 当前批次再拉下一批。max rows、batches 和“累计 Arrow charge +
canonical bytes”任一超限都会通过 execution lease 发布 `Failed`；cancel、deadline 与 run drop 保留已有的
`Cancelled`/`TimedOut` 终态。QueryService task 拒绝现在同时报告 task、CPU、memory 的 after 值与 limit，不再只给
无法定位维度的笼统错误。

golden 摘要格式为 `zippy-arrow-multiset-sha256-v1`，摘要字符串携带同名格式前缀。它固定内存地累计规范化
flat scalar logical schema 与排序 metadata hash、row count、两个独立 256-bit row-hash wrapping sum 和 row
hash XOR，因此忽略合法的 batch/行调度差异，同时保留重复行数量。primitive 值直接哈希位模式，变长 scalar
流式哈希；暂不支持的 nested/dictionary/run-end 列会明确拒绝，而不是退化为可能碰撞的 pretty-display。公开的
`NativeQueryCanaryDigestBuilder` 可直接逐批消费 legacy Arrow stream，生成 `NativeQueryCanaryExpected`，无需 collect
完整结果。schema IPC 与 scalar canonical bytes 同样有界，batch schema 变化会明确返回 `SCHEMA_MISMATCH`。
报告把 snapshot/plan、完整 execution、等待 DataFusion/Parquet batch 和 canonical digest 时间分别计为微秒，LDC
灰度可据此区分存储/执行瓶颈与 golden 本身的 CPU 成本。

多表 binding 不再假设请求顺序等于 snapshot 顺序：snapshot capability 保留每个原始 table reference 对应的已解析
table ID，再映射到去重且按 table ID 排序的 canonical ordinal。Name/Id 混用和逆序多表请求因此不会把 SQL alias
绑定到错误的 persisted manifest。当前 persisted planner 仍要求每个 canonical manifest table 恰好一个 binding，
同表 self-join / 多 alias 不在本阶段能力内。

Stage 5.4h 完成时仍不是查询 authority 切换。公开 `QueryEngine` 返回 cloneable 裸 `RecordBatch`，无法证明 native
result permit 覆盖调用方最终 batch drop；decoded output 产出前准入、ActiveSegmentExec、process-wide pool、受配额
spill/temp 和 transport backpressure 当时也仍未完成。canary 只返回摘要报告，用于在这些前置条件完成前收集正确性与
性能证据。

Stage 5.4i bounded decoded-output pre-admission 已完成（2026-07-17）：DataFusion 44 的 `MemoryPool` 文档明确说明
`ParquetExec`、算子间流动的 `RecordBatch` 和其他小额中间分配不计入 pool，因此不能用扩大 per-execution
`GreedyMemoryPool` 冒充 decoded-output 硬边界。本阶段把 persisted native execution 收窄为一个可证明有界的首批子集：
最多一个 Parquet scan（空表允许优化成零 scan）、固定宽度 Arrow schema、直接列 projection、所有中间节点同样只产生
固定宽度值且属于显式基础 allowlist（column/literal/binary/fixed-width cast/null/not/negative）的 filter，以及
local/global limit/offset。exact `FilterExec` 与 Parquet pushed predicate 使用同一校验；`Utf8`/`Binary`、计算
projection、`IN`/UDF predicate、`CoalesceBatchesExec`、sort、aggregate、join、union、window 或多 scan 在
reader 创建前按 `UNSUPPORTED_PLAN` fail closed。

`QuerySessionPolicy` 现在显式固定默认 8192 行 batch，并关闭 DataFusion batch coalesce。后者会缓存任意数量的小 batch；
只按目标行数或物理节点数无法覆盖极端的一行一 batch 控制块，因此在有独立精确记账前不能进入 allowlist。完整物理树
在 bind 后生成 version 1 `PersistedDecodedOutputAdmission`：按 scan fixed-width row size、validity/alignment、递归 schema、
array/control margin，以及 pipeline physical/expression node 数计算同时存活 Arrow batch 峰值。

每个文件仍先以 64 KiB task reservation 打开并按 `64 KiB + footer_bytes * 64` 扩充，随后在同一个 blocking open
中只解析一次 metadata，校验 footer row count 与 Catalog commit row count 一致。Server 再按“两份最大 row group
declared uncompressed bytes + 固定 decoder margin + admitted Arrow pipeline peak”原子扩充同一个 file task；只有成功后才
把缓存 metadata 和 reader 交给 DataFusion。压缩 page/range `Bytes` 继续持有独立 exact result permit；batch 产出后仍
申请 caller-lifetime result permit，因此生产窗口与交付窗口在 handoff 时刻保守重叠，而不是出现未记账空档。

自动化覆盖固定宽度单 scan 接受、变长列/多 scan/coalesce/非 allowlist predicate 拒绝、pushed predicate 不能绕过、
filter + projection + limit 执行、Catalog row count 校验、metadata 足额但 decoded window 不足时的 sticky
`RESOURCE_EXHAUSTED`，以及失败/取消/EOF 后 task、result 和 snapshot pin 排空。Gateway capability 继续保持关闭：
该边界只覆盖 persisted streaming 子集；ActiveSegmentExec、复杂算子的 process-wide pool 与 spill/temp quota、transport
backpressure，以及公开调用方 batch ownership contract 仍未完成。

Stage 5.4j snapshot-bound active segment execution 已完成（2026-07-18）：append table actor 现在接受一次完整替换的
live publication，内容由零个或多个 retained sealed segment 加最多一个 active segment 组成。actor 在同一个串行 turn 内
核验 table/schema/writer epoch、source identity、共享内存 seal 状态、row count 与逻辑 offset 连续性，再 attach 全部
segment；任一项失败都不会替换旧 publication。查询 snapshot 会先按 durable committed watermark 去除已被 persisted file
覆盖的 live 前缀，并同时固定剩余 segment 的 row end 与偶数 payload version。快照之后的正常 append 不可见；对已固定
payload 的原地改写会以 `DATA_CORRUPTION` fail closed，不能返回混合版本 batch。

规划层只持有 path-free `ActiveSegmentDescription`，不携带 shm locator、descriptor 或 mapping capability。绑定后的
`ActiveSegmentExec` 也只持有 weak opener；复制 DataFusion plan 不能延长 query authority 或共享内存 lease。
`PersistedAndActiveExec` 暴露一个 output partition，并严格先耗尽 persisted child，随后才向 Server opener 申请 active task，
因此不会同时保有 Parquet reader task 和 active decode task，也不会依赖通用 `UnionExec` 的并发调度顺序。Server 在克隆
actor-owned `RowSpanView` 前再次逐字段对照 pinned manifest，并为 fixed-width decoded batch、每段控制块、一个 task 和一个
CPU slot 预先准入；task permit 覆盖整个 active stream 寿命。

固定宽度 projection/filter/limit/offset 现在可对 persisted-only、active-only 和 persisted + live snapshot 使用同一 exact
DataFusion 语义。自动化覆盖真实 segment-store rollover 后的 retained-sealed + active 顺序、快照后 append 不可见、
persisted/live 边界无重叠、filter + projection + limit、弱 authority 过期、payload rewrite 失败，以及终态后 query task、
result permit 和 snapshot mapping 排空。该实现不引入 row-level dedup；互斥性由 Catalog durable watermark 与 actor 连续
offset publication 在 snapshot projection 时证明。

本阶段只完成 Server 进程内 native query 的读取能力和 RAII shm lease。native writer/rollover 尚未自动把生产 live
publication 接入 table actor，跨进程长 lease registry 也不在本阶段；Gateway query capability 继续保持关闭。复杂算子的
process-wide pool、受配额 spill/temp、transport backpressure、公开 batch ownership contract，以及依赖 sort 的 tail 仍按
后续任务推进。

任务：

1. 完成 DataFusion/Arrow version spike。（已完成）
2. 新建 `zippy-query` 和 per-session `SessionContext` policy。（persisted planning 已完成）
3. 实现 `ZippyTableProvider`。（persisted-only 已完成）
4. 实现 `ActiveSegmentExec` 和 snapshot lease guard。（已完成；生产 writer publication 接线待后续）
5. 先迁移 projection/filter/head/tail/slice。（fixed-width projection/filter/head/slice 已覆盖 persisted + active；tail 随 sort 迁移）
6. 迁移 sort/aggregate/join/window。
7. 实现 query registry、cancel、deadline。（registry/lifecycle 与内部 persisted result stream 已完成）
8. 接入全局 memory pool、spill 和 temp quota。
9. 实现 explain/profile/operator metrics。
10. Python Table 改为发送完整 server plan，关闭 residual fallback。

验收：

- 新旧引擎对 golden queries 结果一致。
- Query snapshot 在执行期间保持固定。
- 客户端断开后 query 被取消并释放 lease/memory。
- 超预算 query spill 或返回 `RESOURCE_EXHAUSTED`。
- Join/aggregate 不在 Python 客户端执行。
- 本地热订阅性能不受 DataFusion 引入影响。

回滚：按 query capability 路由到 legacy engine；不得自动拉全表做 residual。

### Phase 6：Flight Frontend

目标：用标准 Arrow 协议替换自定义远端查询/写入 framing。

任务：

1. 新建 `zippy-protocol` Protobuf 和 error mapping。
2. 实现 Flight query `GetFlightInfo/DoGet`。
3. 实现 Flight ingest `DoPut`。
4. 实现 query cancel/admin actions。
5. 实现 Flight SQL catalog metadata。
6. 实现 TLS/auth middleware。
7. Python remote client 切 Flight。
8. 旧 Gateway endpoint 转为 compatibility adapter。
9. 增加 protocol version negotiation 和 capability discovery。

验收：

- Python/Rust 客户端可流式查询和批量写入。
- 错误使用稳定 code，不依赖 message parsing。
- TLS 配置和证书轮换测试通过。
- Legacy 和 Flight 对相同 request 返回一致业务结果。

回滚：客户端继续使用 legacy endpoint；Catalog/Storage/QueryService 不回滚。

### Phase 7：Durable ingest 和 subscription

目标：解决写入歧义和断线数据缺口。

任务：

1. 实现 producer acquire/epoch/sequence。
2. 实现 per-table ingest actor。
3. 实现 WAL record、CRC、group commit。
4. 实现 memory/wal/persisted durability ACK。
5. 实现 WAL recovery/checkpoint/truncation。
6. 为所有表分配稳定 log offset。
7. 实现 Flight DoExchange subscription。
8. 实现 consumer offset commit 和 replay。
9. 实现 retention watermark/offset expired。
10. 适配 frame boundary 和 key-value snapshot/changelog watermark。

验收：

- 响应丢失后重试不重复。
- stale producer epoch 永远不能写入。
- WAL 尾部 partial record 可检测并截断。
- 重启后 active state 与 WAL replay 一致。
- Subscriber 可从 committed offset 继续，允许重复但不跳行。

回滚：按表保留 `durability=memory` 和 best-effort subscription；已写 WAL 不删除。

### Phase 8：统一 server 和移除 legacy 内核

目标：完成产品级收口。

任务：

1. `zippy master run` 标记 deprecated。
2. `GatewayServer` 标记 deprecated。
3. `MasterClient` 固定为 AdminClient facade。
4. 移除 legacy Registry authority。
5. 移除 JSON snapshot runtime write。
6. 移除 Gateway 手写通用 query executor。
7. 移除 Python residual remote execution。
8. 保留离线 legacy import/export 工具。
9. 更新所有文档、examples、WebUI 和 runbook。
10. 执行完整数据目录升级演练。

验收：

- 新安装只启动 `zippy-server` 和 maintenance worker。
- 旧 API compatibility tests 通过。
- 新数据目录不再生成 `master-registry.json`。
- 生产数据备份、升级、回滚演练完成。

回滚：只能回滚到 Phase 7 的新内核版本，不再支持直接运行 legacy binary。

---

## 19. 测试方案

### 19.1 单元测试

- Catalog command validation 和 unique constraints。
- Schema compatibility。
- Snapshot file visibility。
- Producer sequence/fencing。
- WAL encode/decode/CRC/partial tail。
- Offset retention。
- Query state machine 和 cancellation。
- Maintenance job transitions。
- Error code mapping。

### 19.2 状态机属性测试

使用固定随机种子生成：

```text
create/write/persist/compact/query/drop/restart
```

序列，验证：

- 任何 snapshot 不重复引用同一 source segment。
- current snapshot 只引用 committed file。
- removed file 在旧 snapshot 仍可见。
- producer epoch 单调。
- log offset 单调且不重用。
- idempotent command 重放不改变状态。

### 19.3 Crash matrix

Failpoint 至少覆盖：

```text
after WAL append
after WAL fsync
after active commit
after parquet close
after parquet fsync
after parquet rename
before catalog begin
before catalog commit
after catalog commit
before response write
mid compaction
mid GC
mid catalog checkpoint
```

每个 failpoint 自动执行：

```text
start -> inject -> kill -> restart -> verify -> replay request -> verify again
```

### 19.4 Integration tests

- Local mmap write/read/subscribe。
- Flight write/query/subscribe。
- Legacy adapter compatibility。
- Append/frame/key-value 完整生命周期。
- Schema evolution。
- Long query + concurrent compaction。
- Slow client + query memory pressure。
- Maintenance worker crash/restart。
- Backup/restore 到新 data root。

### 19.5 性能 gates

重构不接受“先正确，性能以后再看”的无限延期。每阶段使用现有 perf workflow，并增加：

```text
catalog command p50/p99
manifest lookup by partition
ingest memory/wal modes
WAL group commit batch size and latency
query first batch/p50/p99
query peak memory/spill
subscription live/replay lag
compaction during ingest impact
server 24h RSS slope
```

硬规则：

- Catalog 不进入逐 row/tick 热路径。
- `durability=memory` 本地写入不得新增 fsync。
- QueryService 不持有 ingest table actor 锁。
- Maintenance 不使用 control/query runtime。
- 任何内存预算都要有 observed peak 指标。
- 任何新缓存都要有容量和失效协议。

---

## 20. 发布与数据升级

### 20.1 Feature flags/config gates

```text
catalog.backend = legacy | sqlite_shadow | sqlite
storage.manifest_v2 = false | true
query.engine = legacy | datafusion
frontend.flight.enabled = false | true
ingest.wal.enabled = false | true
subscription.durable_offsets = false | true
compatibility.legacy_gateway = true | false
```

当前增量实现先提供临时 bootstrap gate：

```toml
[server]
native_storage_enabled = false
```

它只控制 SQLite Catalog、commit recovery 和 reconciliation 的 server-owned 生命周期，不等价于
`catalog.backend=sqlite_shadow`，也不复用 legacy `table.persist.enabled`。完成 shadow/diff 后应由上面的
最终 authority gate 替换并删除此临时布尔项。

这些 gate 只用于迁移，最终必须删除，不能永久形成两套内核。

### 20.2 发布顺序

```text
开发 fixture
-> CI crash tests
-> 本地 sim7x24
-> LDC 收盘只读 shadow
-> LDC 收盘写 shadow
-> 单表 canary
-> 全量 authority switch
-> 观察完整交易日
-> 禁用 legacy write
-> 最终删除 legacy
```

### 20.3 升级工具

```text
zippy catalog inspect
zippy catalog migrate --dry-run
zippy catalog migrate
zippy catalog verify
zippy catalog export-legacy
zippy storage orphan-scan --dry-run
zippy storage verify --checksums
zippy backup create
zippy backup restore --verify-only
```

所有 destructive 操作默认 dry-run，并输出明确的数据目录、对象数量和预计删除字节。

---

## 21. 主要风险和缓解

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| DataFusion Arrow 版本与 workspace 不兼容 | 大量复制或依赖冲突 | Phase 5 前做独立 spike，统一 Arrow 版本 |
| SQLite transaction 进入热路径 | 写入尾延迟上升 | Catalog 只提交 batch/file/offset metadata，不逐 row 提交 |
| WAL fsync 增加延迟 | durable 模式 p99 上升 | 明确 durability，group commit，memory 模式保持原快路径 |
| Snapshot/file 表增长 | Catalog 变大 | 索引、checkpoint、历史 snapshot retention、增量 GC |
| Legacy offset 无法恢复 | 老数据不能任意 replay | 标记 legacy ordering，提供离线 reindex/backfill |
| 双后端 shadow 漂移 | 切换后数据不一致 | 单一 CatalogCommand fan-out，持续 diff，禁止业务层双写 |
| Flight 引入协议复杂度 | 交付延期 | 先 QueryService，后 Flight；legacy adapter 保持可用 |
| Query OOM 影响 server | 全服务重启 | DataFusion memory pool/spill，必要时再引入 query worker 进程 |
| Compaction 删除仍被读文件 | 查询失败 | snapshot lease + removed version + safe GC |
| 重构长期保留两套内核 | 维护成本翻倍 | 每阶段设 legacy 删除条件和最终截止 gate |

---

## 22. 已决策与待决策

### 22.1 已决策

1. 产品定位为单机数据库服务器。
2. 不实现 Raft、多副本、分布式查询和分布式 2PC。
3. 使用统一 `zippy-server` 产品入口。
4. Catalog 使用 SQLite WAL，不自研 metadata WAL。
5. 数据文件使用 immutable Parquet + versioned manifest。
6. 保留 mmap/segment 热路径。
7. 通用查询基于 DataFusion 扩展。
8. 新远端协议基于 Arrow Flight。
9. 写入协议增加 producer epoch、sequence 和 durability。
10. 订阅协议增加稳定 log offset 和 replay。
11. Compaction/GC 从 server 热路径隔离。
12. 采用增量迁移，不做 big-bang rewrite。
13. SQLite driver 使用 `rusqlite`，Catalog connection 只属于专用 OS actor thread。
14. Catalog actor 使用有界 command channel，不在 async runtime 线程执行 SQLite IO。
15. Maintenance worker 通过本地 Unix RPC 访问 CatalogService，不直接打开 `catalog.db`。
16. 非 loopback Flight endpoint 必须启用 TLS。
17. 第一版每张表只允许一个 active producer；多 producer merge 留待独立设计。

### 22.2 实施前待决策

1. WAL payload 是否直接复用 Arrow IPC stream message。
2. WAL CRC 和 file-level checksum 的具体算法。
3. DataFusion 目标版本和 Arrow 升级窗口。
4. Typed plan 使用自有 Protobuf 还是直接采用 Substrait。
5. Consumer offset 默认同步提交还是周期提交。
6. Query history 保留在 SQLite 还是独立滚动日志。
7. Query history 的 SQL text 默认保存原文、脱敏文本还是仅保存 hash。

这些决策必须通过小型 spike 或基准，不应在主重构分支中边写边猜。

---

## 23. Definition of Done

完整重构完成的判定条件：

- [ ] 默认命令为 `zippy server run`。
- [ ] Catalog authority 是 SQLite，运行时不写 JSON registry。
- [ ] 所有持久文件通过 manifest snapshot 可见。
- [ ] Persist/compaction/GC crash matrix 全通过。
- [ ] Durable write 支持跨重启 request idempotency；memory write 支持进程内幂等。
- [ ] WAL 模式支持确定恢复，memory 模式语义明确。
- [ ] 所有 durable table 拥有跨重启稳定 log offset。
- [ ] Durable subscription 可以 replay。
- [ ] Python remote query 不再执行 residual plan。
- [ ] DataFusion 承担通用关系查询。
- [ ] Query 支持 id、cancel、deadline、memory 和 spill。
- [ ] Flight 查询/写入/订阅可用。
- [ ] Legacy Gateway 只剩兼容 adapter。
- [ ] Compaction/GC 在 maintenance worker 执行。
- [ ] 本地 mmap 热路径性能 gate 无回退。
- [ ] 24 小时 sim7x24 长跑无持续 RSS 增长。
- [ ] LDC 完整交易日 canary 通过。
- [ ] 数据目录 upgrade/backup/restore 演练通过。
- [ ] Secrets 不出现在日志和 metrics。
- [ ] 文档、examples、runbook 和 WebUI 全部切新架构术语。

---

## 24. 第一轮可直接执行的工作包

第一轮不要同时引入 SQLite、DataFusion 和 Flight。建议拆成以下连续提交：

### Commit 1：类型和接口

```text
refactor: introduce single-node server service contracts
```

- 新建 `zippy-types`。
- 定义 ID、table kind、ErrorCode、Catalog/Storage/Query traits。
- 不改运行时行为。

### Commit 2：Server composition root

```text
refactor: compose legacy runtime behind zippy server
```

- 新建 `zippy-server`。
- legacy master/gateway 通过 adapter 接入。
- 增加 `zippy server run`。

### Commit 3：Catalog migration framework

```text
feat: add versioned sqlite catalog schema
```

- 引入 SQLite，并由专用 OS actor thread 独占连接。
- physical schema v1 只建 migration metadata，完成 WAL、迁移审计与 integrity check。
- 明确标记为 foundation-only：不实现 `Catalog` trait，不接 shadow 或业务写入。
- 完整类型化 domain schema 作为 P2.2 的后续 physical migration，不复用或改写 v1。

### Commit 4：Shadow Catalog

```text
feat: mirror catalog commands into sqlite shadow
```

- 类型化 CatalogCommand。
- Legacy primary + SQLite shadow。
- 增加 diff report。

### Commit 5：Read authority canary

```text
feat: serve catalog status reads from sqlite
```

- 先切 list/status/get 轻量读。
- full file metadata 仍可回退 legacy。

### Commit 6：Manifest commit prototype

```text
feat: commit persisted files through catalog snapshots
```

- 只选一张测试 append table。
- 完成 tmp/fsync/rename/catalog commit。
- 建 crash failpoints。

第一轮完成后再决定是否进入全面 manifest migration。这个切点能最早验证重构中风险最高的
Catalog 和文件提交协议，同时不触碰查询与实时热路径。

---

## 25. 参考资料

- SQLite WAL：<https://www.sqlite.org/wal.html>
- SQLite atomic commit：<https://www.sqlite.org/atomiccommit.html>
- Apache DataFusion：<https://datafusion.apache.org/user-guide/introduction.html>
- DataFusion architecture：<https://datafusion.apache.org/contributor-guide/architecture.html>
- Apache Arrow Flight：<https://arrow.apache.org/docs/format/Flight.html>
- Apache Arrow Flight SQL：<https://arrow.apache.org/docs/format/FlightSql.html>
