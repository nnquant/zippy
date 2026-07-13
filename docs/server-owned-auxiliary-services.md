# Server-owned WebUI 与维护服务生命周期设计

## 1. 文档状态与本轮边界

本文档定义单机 `zippy-server` 对 WebUI、maintenance 和 compaction 的目标所有权、配置及
生命周期契约。它补充
[`single-node-server-refactor-design.md`](single-node-server-refactor-design.md)，不替代 Catalog、
manifest、查询快照或存储格式设计；同时收紧该文档中“maintenance 可由 server 或 RSPM 管理”的
开放表述：默认产品模式由 server 监督，external ownership 延后实现。

> **重要：本文档是设计草案。本轮不会让 `zippy server run` 自动启动 WebUI 或
> `zippy-maintenance`，下文配置也尚未接入配置解析。**

当前版本继续保持以下行为：

- `zippy server run` 只组合 master、Gateway 和 Phase 1 兼容适配层。
- `zippy server run` 默认后台化并在启动摘要中探测、显示 WebUI 地址，但
  `managed=[no]` 明确表示它不会启动或关闭 WebUI；WebUI 不可达也不会降低核心 server 健康。
- `zippy webui` 仍由用户显式启动为独立 Python/Uvicorn 进程。
- `[table.persist.compaction]` 若启用，仍由 legacy master 内部线程执行。
- `zippy-rspm` 仍按现有 PM 配置管理任务，不会因为本文档出现新的隐式任务。

本轮不修改上述组件所有权行为，是为了先冻结进程所有权、失败语义和安全边界，再逐阶段接入。任何
后续实现都必须通过“配置关闭时不创建进程、线程、socket 或端口”的回归测试。

## 2. 现状审计

### 2.1 `zippy-server` 现状

当前 `LegacyServerRuntime` 已经具备一个可靠的基础组合根：先建立 master listener readiness，
再启动 Gateway 并执行真实 master 依赖探测，最后开放兼容适配层 admission。关闭时先关闭
admission，再停止 Gateway，最后停止 master，并共享一个绝对关闭 deadline。

它目前没有以下能力：

- 通用子进程监督、restart backoff 或 restart circuit breaker。
- WebUI/maintenance 的 startup barrier、readiness 和结构化 shutdown outcome。
- 辅助服务专用 local RPC、认证 token 或 capability handshake。
- 跨 runtime 的 CPU、内存、IO、临时磁盘和文件描述符预算协调。

因此不能把“在 `run_server_daemon` 中直接 `spawn` 一个命令”视为完成 server ownership。

### 2.2 Python WebUI 现状

现有 `python/zippy/webui.py` 是 FastAPI/Uvicorn 应用，主要提供 dashboard、表详情和表数据预览。
它还存在多条不适合作为成熟 server 管理接口的旁路：

- dashboard 直接读取 JSONL 日志和 `master-registry.json`。
- 通过 `pgrep` 猜测 master 进程，通过 `zippy-rspm` JSON-RPC读取 PM 任务。
- HTTP query 参数可以覆盖 master URI，服务端可向另一个地址发起连接。
- 表预览在 WebUI 进程内创建普通查询客户端，没有会话级 deadline、内存预留或并发 admission。
- 没有认证、授权、CSRF/CORS 策略或专用 liveness/readiness endpoint。

当前 CLI 的默认 host 是 `127.0.0.1`，但 `--host` 可以覆盖为任意监听地址，应用本身也没有认证
middleware；因此它只是“默认 loopback”，并非强制 loopback-only。当前版本不得在不受信网络上绑定
非 loopback 地址。目标 WebUI 必须只消费 server-owned Admin API，不再自行发现进程、读取
Catalog/registry 文件或扫描日志目录。

### 2.3 Legacy compaction 现状

legacy master 在 `[table.persist.compaction].enabled = true` 时创建 detached OS thread，周期性列出
全部 stream、选择持久文件组、读写 Parquet，并在 registry snapshot 中替换文件列表。该路径当前
明确拒绝 `delete_sources = true`，因为还没有完整 persisted snapshot lease 保护。

这一实现可保留为迁移基线，但不满足目标维护服务契约：

- compaction 的 CPU、内存和 IO 与 control/ingest 共用进程资源。
- 任务、attempt、lease 和进度不是持久事实。
- thread 没有独立 join handle、启动探测或逐任务关闭结果。
- 文件替换仍以 legacy registry snapshot 为权威，不是 Catalog manifest 原子提交。

### 2.4 PM/RSPM 现状

当前 Python PM bridge 会按需拉起 detached `zippy-rspm`，通过 JSON-RPC 管理配置中的 task。
helper 仍以独立 master/gateway task 为主。目标架构不能让 RSPM 与 server 同时监督同一个 WebUI 或
maintenance worker，否则一次故障可能触发两套 restart、两个 worker 和重复 lease owner。

## 3. 组件所有权决策

“Server-owned”指 `zippy server run` 是配置、启动、readiness、重启、关闭和状态报告的唯一权威，
不等于所有组件必须处于同一个 OS 进程。

| 组件 | 目标执行形态 | 生命周期所有者 | 默认是否启用 | 选择理由 |
| --- | --- | --- | --- | --- |
| Catalog/Ingest/Query/Subscription | server 同进程、隔离 runtime | `zippy-server` | 按核心配置 | 保持 typed service 和低延迟数据面 |
| WebUI | server 监督的 Python 子进程 | `zippy-server` | 否 | 保留 FastAPI 资产，隔离 Python/GIL/依赖与 crash，便于独立升级 |
| Maintenance/Compaction | server 监督的 Rust 子进程 | `zippy-server` | 否 | 大 CPU/IO 工作移出 control/ingest 故障域，并使用稳定 local RPC |
| 顶层 server | 独立进程 | `zippy-rspm`、systemd 或用户 | 显式部署决定 | 外部 supervisor 只处理整个产品进程的存活与重启 |

默认部署的所有权树固定为：

```text
zippy-rspm / systemd
└── zippy server run --foreground
    ├── core runtimes (same process)
    ├── zippy webui --managed-child     # 配置启用后才拉起
    └── zippy-maintenance worker        # 配置启用后才拉起
```

约束如下：

1. 默认 PM 配置只能包含顶层 `zippy server run --foreground`，不能再列独立 master、Gateway、WebUI 或
   maintenance task。
2. server 启动的 child 必须具备 parent-death 行为；server 异常退出后 child 不能成为孤儿服务。
3. 高级部署若选择 `ownership = "external"`，server 只接受一个经过认证的外部 worker，且不得再
   spawn/restart 它。`server` 与 `external` 两种 ownership 互斥。
4. 第一阶段只实现 `ownership = "server"`。外部 ownership 在有明确部署需求和完整 contract test
   前保持拒绝解析，不能以 undocumented flag 开启。

WebUI 长期可以迁移为 Rust 同进程 Admin HTTP/static 服务，但必须先达到 API、认证、限流和 UI
回归对等。仅为了少一个进程而把 Python interpreter 嵌入 server，会把 GIL、依赖加载和 reload
故障带进 ingest 故障域，不作为迁移阶段方案。

## 4. 配置草案

以下 TOML 仅用于冻结目标字段和默认值，**当前配置加载器尚不识别这些 section**：

```toml
[server.auxiliary]
startup_timeout_ms = 10000
shutdown_timeout_ms = 30000
restart_policy = "on-failure"
restart_initial_backoff_ms = 500
restart_max_backoff_ms = 30000
restart_limit = 5
restart_window_sec = 300

[server.webui]
enabled = false
ownership = "server"
required = false
host = "127.0.0.1"
port = 17688
read_only = true
request_timeout_ms = 5000
max_in_flight_requests = 64
max_preview_rows = 1000
auth = "local-token"
credential_transport = "inherited-fd"
tls_cert_file = ""
tls_key_file = ""
trusted_proxies = []

[server.maintenance]
enabled = false
ownership = "server"
required = false
socket_name = "maintenance.sock"
lease_ttl_ms = 30000
heartbeat_interval_ms = 5000
job_poll_interval_ms = 1000
shutdown_grace_ms = 30000

[server.maintenance.resources]
max_cpu_cores = 1
max_memory_bytes = 536870912
max_io_bytes_per_sec = 134217728
max_in_flight_io_bytes = 268435456
max_temp_bytes = 4294967296
max_open_files = 256
max_concurrent_jobs = 1

[server.maintenance.compaction]
enabled = false
min_files = 8
target_file_bytes = 268435456
delete_obsolete_files = false
profile = "trading"
```

配置规则：

- 所有辅助服务默认 `enabled = false`，升级现有部署不会新增监听端口或后台负载。
- `required` 只影响 server startup readiness，不改变数据正确性规则。WebUI 默认 optional；
  maintenance 即使 optional，失败也必须进入 `degraded`，不能伪装健康。
- `host` 非 loopback 时必须同时配置 TLS 和非空认证策略；否则配置校验失败。
- 外部认证 secret 只能通过环境变量、owner-only credential file 或 secret provider 注入；server
  派生的 child token 通过 inherited file descriptor 传递。两者都不能直接写入可序列化配置、CLI
  输出或日志。
- `socket_name` 只能是单个文件名。server 必须通过 validated
  `DataDirectory::path(ManagedDataDirectory::Run)` 派生
  `<data_root>/run/maintenance.sock`；绝对路径、路径分隔符、`..` 和逃离 managed run directory 的
  symlink 一律拒绝，不能让配置把 maintenance RPC 重定向到 data root 外。
- `heartbeat_interval_ms` 必须小于 `lease_ttl_ms / 3`；lease 使用 server 单调时钟判断本次 boot
  的活性，Catalog 中的 UTC expiry 只用于跨重启恢复。
- 组件 CPU 额度必须是全局 CPU 上限的子集。当前代码仍使用
  `[runtime].max_parallelism`/`ZIPPY_MAX_PARALLELISM`；重构总设计中的 `ZIPPY_MAX_CPU_CORES` 在统一
  迁移完成后成为硬上限，不能同时保留两个含义不同的权威变量。
- maintenance memory、IO、temp 和 open-file budget 是 admission 上限，不是事后观测阈值。

### 4.1 Legacy compaction 配置迁移

`[table.persist.compaction]` 与 `[server.maintenance.compaction]` 不能同时拥有调度权。迁移按以下
顺序进行：

1. 新 worker 处于 shadow/dry-run 时，legacy 配置仍是执行权威，新配置只生成计划和 diff。
2. 切换前要求 manifest v2、snapshot lease 和 Catalog job authority 已启用。
3. 切换事务记录一个 catalog revision，随后 legacy scheduler 永久关闭，新 scheduler 成为唯一
   authority。
4. 兼容层只读取 legacy 的 `interval_sec`、`min_files` 和 `sort_column` 生成迁移报告，不执行双写或
   双调度。
5. `delete_sources` 不直接映射为 true；只有新 GC 在 visibility commit、lease 检查和 retention
   检查全部通过后才能删除 obsolete file。

## 5. Server 与辅助组件状态机

### 5.1 Server 启动状态机

```text
Created
  -> ConfigValidated
  -> CoreStarting
  -> CoreReady
  -> RequiredAuxStarting
  -> Ready
      |-> ReadyDegraded
      |-> Draining
  -> Stopped

任意 required stage 失败 -> StartupRollback -> Failed
```

启动规则：

1. 先完成 Catalog recovery、master/control listener、Gateway/Frontend 和核心 service admission。
2. 为每个 enabled child 创建 boot-scoped credential、socket 路径和 sanitized environment。
3. 启动 required child 并在同一个绝对 startup deadline 内完成 capability handshake 和真实依赖
   probe；不能仅以 PID 存活或端口 bind 作为 ready。
4. optional child 可在核心 ready 后启动。失败不撤销基础读写，但 server health 立即变为
   `degraded`，并按有界 backoff 重试。
5. restart 超过 `restart_limit/restart_window_sec` 后打开 circuit breaker，child 保持 stopped，等待
   显式 admin reset 或下一次 server boot，避免 crash loop 消耗热路径资源。

### 5.2 辅助组件状态机

```text
Disabled
  -> Starting
  -> Probing
  -> Ready
      |-> Degraded
      |-> RestartBackoff -> Starting
      |-> Draining -> Stopped
      |-> FailedPermanent
```

每次启动分配新的 `server_boot_id`、`child_instance_id` 和 credential。旧实例即使延迟恢复，也会因
boot/instance mismatch 被 local RPC 拒绝。

WebUI readiness 必须同时满足：

- child PID 存活且监听了配置地址；
- `/internal/ready` 返回匹配的 boot/instance ID；
- WebUI 能通过 scoped Admin API token 完成一次 server health 请求；
- static asset manifest 可加载并匹配构建版本。

Maintenance readiness 必须同时满足：

- Unix socket peer 和 per-boot token 认证成功；
- protocol version/capability handshake 成功；
- worker 回报的实际资源上限不超过 server 授予额度；
- worker 可以查询 job queue，但 readiness 不要求已有任务成功完成。

### 5.3 Liveness、readiness 与 health

三者必须分开：

| 信号 | 回答的问题 | 失败动作 |
| --- | --- | --- |
| Liveness | 事件循环/child 是否仍能响应 | supervisor 可重启该 child |
| Readiness | 是否可接收新请求或 job | 从 admission 移除，不必立刻重启 |
| Health | 功能、依赖、预算和 backlog 是否正常 | 对外显示具体 degraded reason |

server health 返回组件表，而不是一个会掩盖局部失败的布尔值。至少包含：

```text
component
instance_id
state
ready
required
since
restart_count
last_heartbeat
last_error_code
budget_usage
```

Maintenance 不可用时，基础 ingest/query 可以保持 ready，但整体 health 为 degraded，停止创建和
发放新 maintenance lease。WebUI 不可用不能改变核心数据面 ready；核心数据面不可用时 WebUI 即使
仍能返回静态页面也必须标记 not ready。

### 5.4 优雅关闭状态机

关闭顺序固定为：

```text
server readiness=false，关闭新 session/admission
-> WebUI 停止接受新 HTTP 请求并 drain 有界在途请求
-> MaintenanceService 停止规划和发放新 lease
-> maintenance worker checkpoint/cancel/drain 当前 job
-> Gateway/Frontend 停止接收新数据面请求
-> cancel/drain Query，停止 Subscription delivery 并 flush durable offset
-> 停止 Ingest admission，flush WAL/group commit，按策略 seal/checkpoint active table
-> checkpoint Catalog WAL，释放 snapshot/process lease 和 server lock
-> 停止 Query/Subscription/Ingest/Catalog runtime，再停止 master/control runtime
-> 汇总 child 与核心组件 shutdown outcome
```

Maintenance 必须先于 CatalogService 停止，因为 worker 的最后一次进度、失败或成功提交仍需走
Catalog RPC。所有阶段共享 server 的绝对 monotonic shutdown deadline；后续阶段只能获得剩余预算。

server 先通过 local control RPC 发送 drain；到达 child grace deadline 后发送 `SIGTERM`；只在总
deadline 耗尽且 child 仍未退出时发送 `SIGKILL`。结构化 shutdown outcome 必须记录未完成的
request/job、child PID、最后 lease、使用的终止方式和 deadline exhaustion，不能记录“假成功”。

## 6. Maintenance local RPC

### 6.1 传输与认证

Unix 默认使用 owner-only Unix domain socket。Windows 在具备等价 named pipe ACL 与 peer identity
前不得启用 server-owned maintenance。协议要求：

- length-delimited Protobuf 或同等级严格 schema，不使用可任意扩展的无版本 JSON map。
- 每个 request 携带 `protocol_version`、`server_boot_id`、`child_instance_id`、`request_id` 和
  deadline。
- Unix peer credential、owner-only socket 权限和 per-boot bearer token 三者同时校验。
- token 通过继承的只读 file descriptor 或 owner-only 临时文件传递，不出现在 argv、环境 dump、
  status 或日志中。
- server 为 RPC server 和 Catalog authority；worker 只作为受限客户端，不能反向暴露公网端口。

首版最小 RPC：

```text
Handshake(capabilities, resource_limits)
Heartbeat(instance_id, running_jobs, usage)
ClaimJob(kinds, available_budget)
RenewJobLease(job_id, lease_token, progress)
GetJobInput(job_id, lease_token)
CommitCompaction(job_id, lease_token, expected_snapshot, outputs)
CompleteJob(job_id, lease_token, result)
FailJob(job_id, lease_token, retry_class, error)
ReleaseJob(job_id, lease_token)
```

所有 mutation 以 `request_id + payload_hash` 幂等。相同 request ID 携带不同 payload 必须明确拒绝。

`STALE_LEASE` 是本设计拟议的 Maintenance RPC 稳定错误码，当前共享 `ErrorCode` **尚未定义**该
值。进入 Phase D 前必须新增 `ErrorCode::StaleLease`、typed lease error details、wire 映射、客户端
兼容和 retry-policy contract test；在这些内容落地前，任何实现或文档都不能声称服务已返回
`STALE_LEASE`。

### 6.2 Job 与 lease 契约

当前 `zippy-types` 的逻辑 `CatalogCommand` V1 DTO 只定义了
`Pending -> Running -> Succeeded/Failed/Cancelled` 状态机，以及 Running 状态的 `lease_owner` 和
`lease_expires_at_ns` 字段。这只是 wire/domain 类型，不代表 durable storage 已实现：当前 SQLite
physical schema v1 只有 migration metadata，没有 `maintenance_jobs` 表，也没有实现 `Catalog`
authority。

首版 Maintenance RPC 复用上述状态语义，不额外引入可观察的 `Leased` 状态；但在 Phase D 前必须
先引入新的版本化 command/descriptor 契约和 SQLite physical schema migration，补齐 token hash、
fencing generation、索引及事务约束。不得静默改变已冻结的 V1 wire。完成这些前置工作后，
`ClaimJob` 才能在一个 Catalog transaction 中完成 `Pending -> Running` 和 lease 持久化。

worker 内部可使用 `claimed -> preparing -> executing -> committing` 细分执行阶段，但不得让进程内
阶段成为恢复真相。契约如下：

1. 目标 server 生成不可猜测的 `lease_token` 和递增 fencing generation；升级后的 Catalog 只持久化
   token hash、owner、generation 和 expiry，不持久化明文 token。
2. `RenewJobLease` 必须比较 job ID、owner、generation 和 token。目标协议在任何一项不匹配时返回
   `STALE_LEASE`。
3. lease 过期或 server boot 改变后，旧 worker 可以清理自己的 tmp 文件，但不能提交 output、更新
   job 或删除 source。
4. server recovery 将 expiry 已过的 Running job 转为 Failed，再按显式 retry policy 回到 Pending；
   attempt、上次错误和 orphan output 保留可审计。
5. retry 只对确定的 transient error 生效，使用 capped exponential backoff 和最大 attempt。数据
   校验、schema mismatch、fencing 或权限错误不自动无限重试。

### 6.3 Compaction 提交与文件安全

worker 不能打开 `catalog.db`，也不能直接修改 registry、manifest 或可见文件集合。一次 compaction
按以下步骤执行：

1. server 基于固定 `expected_snapshot_version` 选择 immutable input file，并为长任务持久化 snapshot
   lease。
2. worker 只读取 server 授权的 input locator，在 server 分配的 tmp 目录写 UUID output。
3. worker fsync output，计算 size、row count、schema、offset range 和 checksum，将 descriptor 提交
   给 server。
4. server 重新校验 lease/fencing、input 仍属于 expected snapshot、output 位于受管目录且 checksum
   正确。
5. server 通过类型化 `CommitCompaction` 在一个 Catalog transaction 中原子发布新 snapshot；重复
   request 返回同一个已提交 snapshot。
6. commit 成功后 source file 只变为 obsolete。GC 必须等待所有相关 query/backup snapshot lease
   释放且 retention 条件满足，才执行物理删除。
7. commit 前 crash 留下的 output 是 orphan，由后续 orphan scan 核对 identity 后回收；commit 后
   响应丢失通过幂等 request 查询原结果，不重复生成可见文件。

普通进程内短查询 lease 由 server 内存 snapshot registry 维护；跨进程 maintenance/backup lease 才
写 Catalog。这样既保护文件可见性，也不把逐查询更新引入 Catalog 热路径。

## 7. 资源预算与低延迟隔离

Maintenance 的独立进程不是无限制使用机器资源的理由。server-owned ResourceManager 是预算
authority，worker 只能使用握手授予的额度：

| 预算 | Admission 行为 | 运行期行为 |
| --- | --- | --- |
| CPU core slots | 无 slot 时 job 留在 Pending | worker pool 和 Parquet 并行度不能超过 grant |
| Memory bytes | 估算峰值超过额度则拒绝/拆分计划 | Arrow allocator 到硬上限即取消 job |
| IO bytes/sec | token bucket 不足则延后读取 | 分块读取并周期续租，禁止突发无界 prefetch |
| In-flight IO bytes | 读取前预留 | batch 完成即归还，防止 page cache/queue 膨胀 |
| Temp bytes | 空间不足不启动 job | 每个 output/spill 记账，越界立即停止 |
| Open files | 按 input/output 预留 | 使用有界 file set/window，禁止一次打开全分区 |
| Concurrent jobs | 没有 permit 不 claim | 默认 1，避免多 compaction 互相放大 IO |

优先级至少分为 `interactive`、`normal`、`maintenance`。Maintenance 不得借用为
interactive/ingest 保留的 CPU、内存或 IO 额度。系统资源压力、ingest lag 或 control latency 超过
阈值时，server 可撤回未使用 grant、暂停发放新 job，并要求 worker 在下一个可取消边界 yield；不能
在任意文件 rename/Catalog commit 中点强制中断。

“交易期”和“收盘期”采用显式 profile 或外部调度切换：

- profile 只改变预算和 admission，不改变 compaction 正确性语义。
- 配置使用明确时区，切换事件进入 audit log。
- 默认 `trading` profile 使用保守额度；`closing` profile 必须由配置或管理命令显式启用，不能按
  本机时间硬编码猜测市场阶段。

性能 gate 应同时观测 ingest/control p99、query p95、maintenance throughput 和 page-cache/RSS，不能
只以 compaction 用时缩短判定优化成功。

## 8. WebUI 安全与故障策略

### 8.1 Admin API 边界

目标 WebUI 是无状态展示层，只能调用 versioned Admin API。Admin API 可以提供 health、runtime
inventory、metrics、受限日志查询、query/job 状态和表数据 preview，不得暴露任意文件路径、任意
master URI、shell/PM 命令或原始 Catalog 访问。

当前实现的下列行为需要在接管前替换：

| 当前行为 | 目标行为 |
| --- | --- |
| HTTP query 覆盖 `uri` | server 固定注入不可覆盖的 local Admin endpoint |
| 读取 `master-registry.json` | 调用 typed Catalog/RuntimeRegistry Admin view |
| 递归扫描 JSONL 日志 | 调用有范围、分页、脱敏和 byte limit 的 log API |
| `pgrep` 发现 master | 使用 server-owned component inventory |
| 直接访问 RSPM task API | server 汇总自己的 child 与顶层 supervisor facts |
| WebUI 内执行普通 table collect | server 创建有 deadline/budget/snapshot 的 preview query |

### 8.2 网络与应用安全

- 默认只绑定 `127.0.0.1`，并使用 boot-scoped local token。
- 非 loopback 监听必须配置 TLS、用户认证、最小 RBAC、trusted proxy 和审计；缺任一项 fail closed。
- 默认 read-only。未来 mutation endpoint 必须使用独立权限、CSRF 防护、幂等 request ID 和 audit
  record，不能复用 dashboard GET。
- CORS 默认拒绝跨 origin；设置严格 CSP、`X-Content-Type-Options`、frame policy 和安全 cookie。
- 限制 request body、response bytes、并发数、preview rows 和处理 deadline；客户端断开立即 cancel
  下游 query。
- asset path 必须 canonicalize 后校验位于 build manifest root，继续防止 path traversal。
- child 继承 sanitized environment，只获得 Admin API scoped token，不能继承 Gateway、Catalog、云端
  或数据源 secrets。
- 日志中对 token、Authorization、password、secret 和 credential 做字段级及 header 级脱敏；错误响应
  不返回本机绝对路径或 child argv。

### 8.3 故障策略

WebUI crash 不触发核心 server shutdown。server 将其标记 degraded，按有界 backoff 重启；超过
circuit breaker 阈值后停止重启并保留诊断。Admin API 变慢或不可达时，WebUI 必须快速返回明确的
503，不得无限等待并耗尽 Uvicorn worker。

WebUI 不能成为核心 health 的唯一入口。CLI 和本地 control RPC 必须能在 WebUI 故障时读取同一份
component health；UI 只渲染该事实，不自行推断系统健康。

## 9. 分阶段接入

### Phase A：契约与关闭态基线

1. 添加本文档、配置 schema proposal 和 component health DTO。
2. 为 disabled 配置建立回归测试，证明无 child、socket、port、token file 和额外 thread。
3. 明确 RSPM 只管理顶层 server，禁止默认 PM 生成重复辅助任务。

完成标准：运行行为与当前版本一致。本轮停在此阶段，不自动启动任何辅助服务。

### Phase B：通用 child supervisor 与 Admin API

1. 实现 server-owned child supervisor、parent-death、绝对 deadline、backoff 和 circuit breaker。
2. 实现 boot-scoped credential provider 和 component health registry。
3. 提供只读、versioned local Admin API，替代 WebUI 的文件、`pgrep`、PM 和 URI 旁路。
4. 先用 fake child 做 crash/startup/shutdown contract tests，不接真实 WebUI/maintenance。

完成标准：fake child 的所有 failpoint 均有确定状态和清理结果，且核心热路径 benchmark 无回归。

### Phase C：WebUI opt-in 接管

1. 增加 `--managed-child` 模式，禁止 reload、任意 URI 和自行进程发现。
2. 接入 scoped Admin API、`/internal/live`、`/internal/ready` 和 asset manifest probe。
3. 实现 loopback token、安全 header、并发/bytes/deadline 限制。
4. 仅在 `server.webui.enabled = true` 时由 server 启动；保留 `zippy webui` 手工兼容入口。

完成标准：UI/API golden test 对等，server 重启无 orphan WebUI，WebUI crash 不影响 ingest/query。

### Phase D：Maintenance shadow worker

前置条件是 SQLite Catalog authority、manifest v2、snapshot lease 和 typed maintenance job 已可用。

1. 实现 `zippy-maintenance` binary、local RPC 和资源 grant。
2. 只生成 compaction/GC 计划、读 input metadata 并计算 diff，不写 output、不 commit、不删除文件。
3. 与 legacy planner 在固定 snapshot 上对比 input set、排序、offset/schema 和预估资源。
4. 强杀 worker、server 和 RPC，验证 lease expiry、restart fencing 与 orphan 清理。

完成标准：shadow mismatch 在规定窗口为 0，压力测试不突破 ingest/control latency gate。

### Phase E：Compaction authority 切换

1. 先开放 output write + checksum，但保持 Catalog commit shadow。
2. 开放 `CommitCompaction` authority，legacy scheduler 同事务关闭，禁止双调度。
3. 保持 `delete_obsolete_files = false` 观察 snapshot consistency 和 orphan 指标。
4. 最后接入 lease-aware GC；按表和 job kind 提供 kill switch。

完成标准：所有 crash matrix、长查询并发 compaction 和幂等重放测试通过；禁用 worker时基础读写以
degraded 状态继续。

### Phase F：运维收口

1. 更新 PM helper，使默认任务只有 `zippy server run --foreground`。
2. 更新 runbook、examples 和 WebUI 术语，legacy master/gateway/compaction 标记 deprecated。
3. 在两个发布周期稳定后删除 legacy compaction scheduler；手工 WebUI 入口根据兼容承诺另行决定。

## 10. 验收测试矩阵

### 10.1 配置与所有权

- 默认配置及显式 `enabled = false` 不产生 child、socket、port、credential 或额外后台负载。
- 非 loopback WebUI 缺 TLS/auth 时配置失败，不发生 bind 后再报错。
- 同一组件同时声明 server/external ownership 时配置失败。
- RSPM 重启顶层 server 后只有一棵 child tree，不存在重复 worker 或孤儿 PID。

### 10.2 启动与健康

- child bind 成功但依赖 probe 失败时不能 ready。
- required child 超时触发完整 startup rollback；optional child 超时只进入 degraded。
- boot/instance/protocol/capability mismatch 明确拒绝，并记录无 secret 的结构化错误。
- restart 达到阈值后 circuit breaker 生效，核心服务继续按策略提供服务。

### 10.3 关闭与恢复

- shutdown 停止新 HTTP request/job claim，并在一个绝对 deadline 内 drain。
- maintenance 正在读取、写 tmp、fsync、commit 前、commit 后响应前分别强杀，恢复结果可确定。
- timeout outcome 列出未完成 request/job/lease/PID，进程退出码反映 incomplete shutdown。
- server 强杀后 child 自动退出；旧 child 不能用旧 token/lease 向新 server 提交。

### 10.4 Compaction 正确性

- 相同 job/request 重放只产生一个可见 snapshot。
- 长查询固定旧 snapshot 时，并发 compaction 不重不漏，input file 不被 GC。
- expected snapshot 已变化时 commit 返回 conflict，output 进入可审计 orphan 流程。
- checksum/schema/offset range 任一不匹配时 Catalog 不发布 output。
- worker 无法直接打开 Catalog，无法删除 server 未授权路径或 active segment。

### 10.5 资源与性能

- CPU、memory、IO、in-flight bytes、temp、open files 和 concurrent jobs 均有独立硬 gate。
- 无预算时 job 留在 Pending，不以无界 queue、retry 或 prefetch 消耗内存。
- trading profile 下 compaction 压力不突破既有 ingest/control p99 gate。
- closing profile 提高吞吐时仍满足 query snapshot 和资源硬上限。
- WebUI slow client、大 preview 和并发刷新不能耗尽 query/admission 资源。

### 10.6 WebUI 安全

- 任意 URI、path traversal、跨 origin、无认证远端访问和 oversized request 均被拒绝。
- child 环境、status、错误页和日志中不出现 token 或其他 server secret。
- 所有 future mutation 经 RBAC、CSRF、幂等和 audit；read-only 模式无 mutation route。
- WebUI 不可用时 CLI/local RPC 仍能返回相同 component health。

## 11. 实施门槛总结

只有同时满足以下条件，才能把某个辅助服务从“设计”改为“可 opt-in”：

- 生命周期所有者唯一，禁用态没有副作用。
- readiness 是真实依赖 probe，不是 PID/端口猜测。
- 关闭和 crash 都有结构化、可重放、可审计结果。
- Maintenance 不直接访问 Catalog authority，文件可见性由 snapshot/lease/fencing 保护。
- 所有预算在 admission 前生效，辅助负载不能侵占实时 ingest/control 保障额度。
- WebUI 只依赖稳定 Admin API，具备 loopback 默认与非 loopback fail-closed 安全策略。
- 完成对应阶段的 contract、crash、security 和 performance gates。

在这些门槛完成前，`zippy server run` 必须继续保持本轮行为：**不自动启动 WebUI，也不自动启动
`zippy-maintenance`。**
