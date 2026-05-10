# Zippy 审计问题记录

日期：2026-05-08

范围：本文件记录本轮只读审计中已经定位或复现的问题，以及对应修复建议。本文不包含代码修改。

结论：当前不能对项目达到 100% 信心。问题集中在控制面权限、持久化文件路径信任、gateway 远端查询语义、生命周期清理、snapshot/lease 一致性、测试/构建基线和数值确定性。

关账状态：本报告已在 `docs/audit_closure_2026-05-10.md` 中逐项关闭或标记后续处理。
架构/性能型后续项记录在 `ROADMAP.md`，不作为本轮审计关账阻塞项。

编号按发现和补记顺序递增，不代表严重程度或修复顺序。

## 优先级说明

- P0：可能导致越权读写、错误交易/研究结果、数据损坏或生产不可恢复状态。
- P1：会导致运行时失败、状态漂移、资源泄漏、错误健康判断或重要 API 行为不一致。
- P2：影响可维护性、可诊断性、工具链或边界清晰度。

## 一、基线构建与测试

### F001 `zippy-perf` 构建失败

级别：P1

现象：`zippy-perf` 依赖的 `publish_table` 符号缺失，导致基线构建不稳定。

影响：性能/回归测试链路不可作为发布门禁，容易让底层 API 漂移长期存在。

修复建议：

- 明确 `publish_table` 是否仍是公开 API。
- 若已废弃，更新 `zippy-perf` 调用方。
- 将 `zippy-perf` 纳入常规 workspace build gate。

### F002 `zippy-python` Cargo test 链接失败

级别：P1

现象：`zippy-python` Cargo tests 出现 PyO3 / Python C API undefined symbol 链接失败。

影响：Rust/Python bridge 的单元测试无法提供真实保护，native 变更容易绕过验证。

修复建议：

- 明确测试链接方式，区分 extension-module 与 embedding 测试配置。
- 为 PyO3 test profile 配置正确的 Python lib 链接。
- 在 CI 中单独保留 `zippy-python` native test gate。

### F003 Python 全量 pytest 当前不通过

级别：P1

现象：Python 全量 pytest 有多项失败。

影响：无法证明 Python API、gateway、session、pipeline、持久化路径处于一致状态。

修复建议：

- 先冻结当前失败列表，分为环境失败、测试假设失败、真实回归。
- 对真实回归逐项补最小复现测试。
- 不应在 pytest 未恢复前宣称发布可用。

### F004 Python CLI 入口缺失

级别：P2

现象：`python -m zippy` 没有 `zippy.__main__`。

影响：用户按 Python 包常规方式运行 CLI 会失败；文档或工具如果依赖模块入口会失效。

修复建议：

- 增加 `python/zippy/__main__.py`，转发到现有 CLI main。
- 在 CLI smoke test 中覆盖 `python -m zippy --help`。

### F005 工具链门禁不完整

级别：P2

现象：black、ruff、mypy、`py.typed` 等 Python 包质量门禁缺失或不可运行。

影响：公开 Python API 缺少类型和格式稳定性保障，IDE 体验和 downstream 类型检查弱。

修复建议：

- 补齐 `pyproject.toml` 中 black/ruff/mypy 配置。
- 发布包包含 `py.typed`。
- 将 `uv run black --check`、`uv run ruff check`、`uv run mypy` 加入 CI。

## 二、控制面权限与破坏性操作

### F006 `replace_persisted_files` 无进程与 owner 授权

级别：P0

证据：

- `ReplacePersistedFilesRequest` 没有 `process_id`：`crates/zippy-core/src/bus_protocol.rs`
- `MasterClient.replace_persisted_files()` 不要求注册进程：`crates/zippy-core/src/master_client.rs`
- master server 直接调用 registry replace：`crates/zippy-master/src/server.rs`
- registry replace 没有 writer/source/sink owner 校验：`crates/zippy-master/src/registry.rs`

复现结果：

```text
replace_without_process_ok /tmp/.../outside_owner.parquet
gateway_read_injected_rows {'instrument_id': ['SECRET'], 'last_price': [999.0]}
```

影响：任意 master client 可替换某个 stream 的 persisted metadata，并让 gateway 读取指定 parquet 路径。

修复建议：

- `ReplacePersistedFilesRequest` 增加 `process_id`。
- 要求请求者是 writer/source/sink owner 或显式 admin capability。
- 校验 metadata `writer_epoch`。
- 对路径做 canonicalize，并限制在受控 data root。

### F007 `drop_table` 无进程与 owner 授权

级别：P0

证据：

- `DropTableRequest` 没有 `process_id`：`crates/zippy-core/src/bus_protocol.rs`
- `MasterClient.drop_table()` 不要求注册进程：`crates/zippy-core/src/master_client.rs`
- master server 直接删除 registry/bus 和 persisted files：`crates/zippy-master/src/server.rs`

复现结果：

```text
attacker_drop_result {'table_name': 'ticks', 'dropped': True, ..., 'persisted_files_deleted': 1}
victim_exists_after_drop False
```

影响：任意 client 可删除任意表，并删除 metadata 指向的文件。

修复建议：

- `drop_table` 必须鉴权。
- 将 destructive ops 放入 admin API，普通数据 client 不应有该能力。
- 删除文件前校验路径属于该表的持久化根目录。

### F008 persisted `file_path` 被直接信任

级别：P0

证据：

- `normalize_persisted_file()` 仅检查 `file_path` 是非空字符串。
- gateway `persisted_file_record_batches()` 直接 `File::open(path)`。
- Python `_read_persisted_parquet_file()` 直接 `pq.read_table(file_path)`。
- master `delete_persisted_files()` 直接 `fs::remove_file(file_path)`。

影响：配合 F006/F007，可变成任意 parquet 读取和任意文件删除。

修复建议：

- master 接收 metadata 时进行路径规范化和根目录约束。
- metadata 中存相对路径或受控 file id，而不是任意绝对路径。
- 所有读/删路径再次做 defense-in-depth 校验。

### F009 `drop_table(drop_persisted=True)` 非原子

级别：P0

证据：master 先 drop registry/bus 并写 snapshot，然后才删除 persisted files。

复现结果：

```text
drop_error RuntimeError ... failed to delete persisted table file ... Is a directory
stream_after_failed_drop_error RuntimeError ... stream not found
```

影响：接口返回 error，但表已经消失，进入不可恢复或难恢复状态。

修复建议：

- 先预校验所有待删除文件。
- 或采用 two-phase：quarantine/rename 成功后再提交 registry 变更。
- 若部分成功，返回明确 `partial_success`，不能用普通 error 掩盖已提交变更。

### F010 persisted metadata 中 `stream_name` / `schema_hash` 可伪造

级别：P1

证据：`normalize_persisted_file()` 对 `stream_name`、`schema_hash` 使用 `or_insert`，已有错误值不会覆盖。

复现结果：

```text
stored_stream_name other_table
stored_schema_hash wrong_hash
```

影响：metadata source-of-truth 混乱，审计、compaction、schema 校验和跨表隔离都可能失效。

修复建议：

- master 强制写入当前 stream 的 `stream_name` 和 `schema_hash`。
- 若调用方传入不同值，应拒绝请求并记录错误。
- 补充 parquet 文件实际 schema/hash 校验。

### F011 `UpdateStatus` 无 owner 校验

级别：P0

证据：

- `UpdateRecordStatusRequest` 没有 `process_id`。
- master server `UpdateStatus` 直接调用 `set_source_status` / `set_engine_status` / `set_sink_status`。

复现结果：

```text
attacker_update_status_returned_ok
```

影响：任意 client 可把别人的 source/engine/sink 标成 failed/lost/running，并注入任意 metrics。

修复建议：

- `UpdateStatus` 增加 `process_id`。
- 校验记录 owner；admin 才能跨进程改状态。
- metrics 字段需要 size/type 限制，避免控制面膨胀。

### F012 raw control request 可伪造 `process_id` 注册 source/engine/sink

级别：P0

证据：

- `RegisterSourceRequest`、`RegisterEngineRequest`、`RegisterSinkRequest` 都携带调用方提供的 `process_id`。
- master server 直接把 request 中的 `process_id` 传给 registry。
- registry `register_source()`、`register_engine()`、`register_sink()` 没有调用 `validate_process_alive()`。

复现结果：未注册进程的 raw control client 直接发送 JSON 请求，使用 `process_id="ghost_proc"`，master 返回成功：

```text
source_resp {"SourceRegistered":{"source_name":"spoof_source"}}
engine_resp {"EngineRegistered":{"engine_name":"spoof_engine"}}
sink_resp {"SinkRegistered":{"sink_name":"spoof_sink"}}
```

影响：任意能连到 master control endpoint 的 client 可以伪造不存在或他人的 process_id，污染控制面 registry；source 注册还会参与 active writer source / writer_epoch 语义，可能阻塞真实 writer 或扰乱 descriptor 授权。

修复建议：

- register source/engine/sink 前必须验证 `process_id` alive。
- 更稳妥地，control connection 不应信任 payload 中的 `process_id`；应使用已认证的 session/process identity。
- raw JSON control protocol 和 Python/Rust client 必须走同一套身份校验。

### F057 raw `UnregisterProcess` 可注销他人进程并造成 registry/bus 分叉

级别：P0

证据：

- `UnregisterProcessRequest` 只有调用方提供的 `process_id`。
- master server 直接调用 `registry.unregister_process(&request.process_id)`。
- `registry.unregister_process()` 只清 registry 中的 process、writer_process_id、reader ids 和 leases。
- 该路径没有像 lease reaper 那样同步 `bus.detach_writer()` / `bus.detach_reader()`。

复现结果：攻击者发送 raw JSON 注销 owner 的 `proc_1`：

```text
attacker_unregister_resp {"ProcessUnregistered":{"process_id":"proc_1"}}
after_unregister_stream ... 'writer_process_id': None, 'status': 'registered'
owner_heartbeat_error RuntimeError ... process not found process_id=[proc_1]
old_writer_close_error RuntimeError ... writer not owned ... owner_process_id=[None]
other_write_to_error_after_schema_cache RuntimeError ... writer already attached ... writer_process_id=[proc_1]
```

影响：任意 client 可注销别人的 alive process；registry 显示 writer 已清理，但 bus 仍保留旧 writer，导致新 writer 无法 attach，旧 writer 又无法正常 close，形成控制面/数据面分叉。

修复建议：

- `UnregisterProcess` 必须使用已认证 connection/session 的 process identity，不能从 payload 任取。
- 只有 owner 本人或 admin 可以注销 process。
- unregister process 应复用 `expire_process_attachments()` 的 bus + registry 原子清理逻辑。
- snapshot 写失败时需要完整恢复 registry 和 bus 状态，避免半注销。

### F058 raw `Heartbeat` 可替他人 process 续租

级别：P0

证据：

- `HeartbeatRequest` 只有调用方提供的 `process_id`。
- master server 直接调用 `registry.record_heartbeat(&request.process_id)`。
- registry 只检查该 process 当前是否存在且 alive，不校验 heartbeat 来自原 owner connection。

复现结果：未拥有 `proc_1` 的 raw control client 发送 heartbeat：

```text
attacker_heartbeat_resp {"HeartbeatAccepted":{"process_id":"proc_1"}}
```

影响：任意 client 可替已经失联或即将失联的 process 续租，阻止 lease reaper 回收 writer/reader/segment lease/source/engine/sink 状态；也可配合其它控制面漏洞长期维持污染状态。

修复建议：

- heartbeat 必须绑定注册时建立的 session/token/connection identity。
- 不接受裸 `process_id` 作为续租凭证。
- lease reaper 应记录 heartbeat source，发现身份漂移时拒绝并报警。

### F059 `RegisterStream` 无身份校验且 stream name 可逃逸 bus root

级别：P0

证据：

- `RegisterStreamRequest` 没有 `process_id`。
- master server 注册 stream 时不要求已注册 process 或 admin capability。
- `validate_register_stream_request()` 只检查 buffer/frame size 和已有 stream config/schema。
- bus shm path 使用 `root_dir.join(format!("{}_{}.mmap", stream_name, instance_id))`，没有清理 `/`、`..` 等路径片段。

复现结果：raw control client 发送 `stream_name="../zippy_escape_stream_path_audit"`：

```text
register_stream_resp {"StreamRegistered":{"stream_name":"../zippy_escape_stream_path_audit"}}
escaped_files ['/tmp/zippy_escape_stream_path_audit_1778236159604470285.mmap']
```

影响：任意 client 可创建 stream 和共享内存文件；特殊 stream name 可让 bus mmap 文件逃出 `/tmp/zippy-master-bus`。这会导致资源耗尽、路径污染，也会影响后续 remove_stream 的删除边界。

修复建议：

- `RegisterStream` 要求 process/admin 身份。
- stream name 必须限制为明确字符集，例如 `^[A-Za-z0-9_.:-]+$`，并拒绝 `/`、`\`、`..`、空白控制字符。
- bus 文件名必须使用独立 safe token 或 hash，不直接拼接原始 stream name。
- remove_stream 删除前也必须 canonicalize 并确认路径仍在 bus root 下。

### F060 raw `WriteTo` / `ReadFrom` 可用不存在的 process attach writer/reader

级别：P0

证据：

- `WriteTo` / `ReadFrom` 请求携带调用方提供的 `process_id`。
- master server 直接把 request 中的 `process_id` 传给 bus 和 registry。
- `registry.attach_writer()` / `registry.attach_reader()` 不调用 `validate_process_alive()`。

复现结果：未注册任何进程的 raw control client 使用 `process_id="ghost_proc"`：

```text
ghost_write_resp {"WriterAttached":{... "process_id":"ghost_proc" ...}}
after_ghost_write ... 'writer_process_id': 'ghost_proc', 'status': 'writer_attached'
ghost_read_resp {"ReaderAttached":{... "reader_id":"ticks_reader_1","process_id":"ghost_proc" ...}}
after_ghost_read ... 'reader_count': 1, 'status': 'reader_attached'
```

影响：不存在的进程也能占用 writer 单例、增加 reader_count、改变 stream 状态，并可能阻塞真实 writer/reader 生命周期。

修复建议：

- `WriteTo` / `ReadFrom` 前必须验证 process alive。
- control connection 应绑定已认证 process identity，不从 payload 信任 `process_id`。
- bus attach 和 registry attach 都应做 defense-in-depth 校验，任一层拒绝 ghost process。

### F061 raw `CloseWriter` / `CloseReader` 可冒充 owner 关闭他人句柄

级别：P0

证据：

- `CloseWriter` / `CloseReader` 请求携带调用方提供的 `process_id` 和 writer/reader id。
- master server 用 payload 中的 `process_id` 做 owner 校验。
- writer id 固定为 `ticks_writer`，reader id 形如 `ticks_reader_1`，容易猜测。

复现结果：攻击者发送 raw JSON，填入 owner 的 `proc_1`：

```text
attacker_close_writer_resp {"WriterDetached":{"stream_name":"ticks","writer_id":"ticks_writer"}}
owner_writer_close_after_attack_error RuntimeError ... writer not owned ... owner_process_id=[None]
attacker_close_reader_resp {"ReaderDetached":{"stream_name":"ticks","reader_id":"ticks_reader_1"}}
owner_reader_close_after_attack_error RuntimeError ... reader not owned ... owner_process_id=[None]
```

影响：任意 client 只要知道或猜到 process/handle id，就能关闭别人的 writer/reader，使真实 owner 后续 close 失败，并打断 live 数据流。

修复建议：

- close 请求必须使用连接/session 上的真实身份，不接受 payload identity。
- writer/reader id 应使用不可预测 token，且绑定创建连接或 capability。
- owner 校验成功后，bus detach 与 registry detach 需要事务化处理，避免半关闭。

### F062 raw `Publish*` 可冒充 writer owner 发布伪造 descriptor 和持久化元数据

级别：P0

证据：

- `PublishSegmentDescriptor`、`PublishPersistedFile`、`PublishPersistEvent` 都携带调用方提供的 `process_id`。
- registry 会验证该 `process_id` 是否是 stream writer/source/sink owner，但不验证请求来自 owner connection。
- `PublishSegmentDescriptor` 只拆出 `sealed_segments` 并写入 active descriptor，没有校验 shm identity 是否真实存在。

复现结果：真实 owner 持有 writer 时，攻击者 raw JSON 填入 owner 的 `proc_1`：

```text
attacker_publish_file_resp {"PersistedFilePublished":{"stream_name":"ticks"}}
attacker_publish_event_resp {"PersistEventPublished":{"stream_name":"ticks"}}
attacker_publish_descriptor_resp {"SegmentDescriptorPublished":{"stream_name":"ticks"}}
after_publish ... 'active_segment_descriptor': {'segment_id': 999, 'shm_os_id': 'fake', ...}
after_publish ... 'persisted_files': [{'file_path': '/tmp/.../attacker.parquet', ...}]
after_publish ... 'persist_events': [{'persist_event_type': 'spoofed', ...}]
```

影响：攻击者可替活跃 writer 注入任意 persisted file、persist event 和 fake segment descriptor。该问题可直接污染 gateway 查询结果、破坏 replay/snapshot 语义，并让 reader 指向不存在或恶意构造的 shm 描述。

修复建议：

- publish 类接口必须使用 authenticated session identity。
- descriptor 需要校验 segment id、generation、row_capacity、shm path/id、writer_epoch 与当前 bus writer 的真实状态一致。
- persisted file/event 应绑定受控 writer/sink capability，并限制路径、schema、source segment identity。
- publish 成功前后应维护审计日志，记录真实 connection identity 与 logical process identity。

### F013 reader lease 可对不存在 segment 创建

级别：P0

证据：`registry.acquire_segment_reader_lease()` 只验证 process alive，然后写入 lease JSON，不验证 descriptor/segment 是否存在。

复现结果：对尚不存在的 `("ticks", 1, 0)` 创建 fake lease 后，retention 会保留对应 sealed segment；释放 fake lease 后才继续回收。

影响：任意进程可阻塞 retention，造成磁盘/共享内存资源泄漏。

修复建议：

- acquire lease 时验证 segment identity 属于 active/sealed/persisted 中的真实条目。
- lease 应绑定 reader id 或 query snapshot id。
- 对 lease 加 TTL 和最大数量限制。

### F014 lease acquire/release snapshot 写失败无完整回滚

级别：P1

证据：acquire/release 先 mutate registry，再写 snapshot；snapshot 写失败时没有像 persisted file/event 那样完整恢复 previous state。

影响：内存状态与持久化 snapshot 分叉，重启后 lease 状态回退，运行中状态继续污染 retention。

修复建议：

- acquire/release 采用 previous state rollback。
- 或统一用事务结构：clone -> mutate -> write snapshot -> swap。

## 三、生命周期、状态漂移与健康判断

### F015 `Pipeline.stop()` 留下 ghost writer 和 active descriptor

级别：P0

证据：`Pipeline.stop()` 只停 engine 并 unregister source；master `unregister_source` 只清 active writer source，不清 `writer_process_id`、stream status、active descriptor。

复现结果：

```text
before_stop writer_attached proc_1 True
after_stop writer_attached proc_1 True
read_table(...).collect() -> shared memory error: No such file or directory
```

影响：控制面显示 writer_attached，但 shm 已不存在；后续读表、健康检查和 wait ready 都会误判。

修复建议：

- source unregister 时，如果该 source 是 active writer source，应 detach writer 并清 active descriptor。
- stop runtime 前后要发布明确 terminal descriptor 或 tombstone。
- health 必须检查 shm 可达性。

### F016 `table_health()` 对 ghost table 返回 ok

级别：P1

证据：health 只检查 stale/error/failed 或 missing active descriptor；ghost descriptor 指向不存在 shm 时仍 `status=ok`。

复现结果：

```text
status=ok
stream_status=writer_attached
alert_count=0
```

影响：监控会把不可读表判断为健康。

修复建议：

- health 增加 active descriptor shm open/read preflight。
- 对 writer_attached 但 process/source 已消失的状态报警。

### F017 `_wait_for_table_ready()` 会把 ghost table 当 ready

级别：P1

证据：ready 条件为 `status != stale` 且有 active descriptor。

影响：`wait=True` 不能保证后续 collect 可用。

修复建议：

- ready 必须验证 active segment 可打开并有一致 control snapshot。
- 对 stale/ghost/lost writer 分别给出不同等待或失败原因。

### F018 `RemoteGatewayWriter.close()` / `GatewayServer.stop()` 不清 master writer

级别：P0

证据：Python remote writer close 只 flush 并置 `_closed=True`；gateway shutdown 只 `materializer.on_stop()` 和清 writers，没有 close/detach request。

复现结果：writer close / gateway stop 后 master stream 仍为 `writer_attached`，再读表出现 shared memory error。

影响：远端 gateway 关闭会留下 ghost writer，污染读路径和监控。

修复建议：

- gateway writer close 发送 explicit close/detach。
- gateway stop 遍历 writers，按 stream detach writer，并发布终止状态。
- master 对 gateway process unregister 时自动清理其 writer/source。

### F019 `Pipeline.stream_table()` 失败留下部分注册状态

级别：P1

现象：`stream_table()` 失败后留下已注册 stream/source，后续重试持续失败。

影响：一次启动失败会把环境卡在半初始化状态。

修复建议：

- stream/source/engine 注册使用事务式补偿。
- 失败路径明确 unregister 本次创建的资源。
- 测试覆盖 register_stream 成功但后续失败的重试场景。

### F020 `MasterClient.write_to()` 依赖同 client schema cache

级别：P1

证据：另一个 client 可 `get_stream()` 看到已有 stream，但 `write_to()` 会因为本地 schema cache 缺失失败。

影响：多进程、多 client 场景下 API 行为违反控制面 source-of-truth 预期。

修复建议：

- `write_to()` 如果本地 schema cache 缺失，应从 master stream metadata 恢复 schema。
- Python binding 的 `get_stream/list_streams` 应暴露 `writer_epoch`、`write_seq` 等诊断字段。

### F021 `EngineHandle.stop()` 持锁调用外部 source stop

级别：P1

现象：stop 路径在持有内部锁时调用外部 source stop。

影响：外部 source 如果回调 runtime 或阻塞，可能死锁或放大 shutdown 延迟。

修复建议：

- 缩小锁范围。
- 先交换出 handle/source，再在锁外 stop。
- stop 超时和异常需独立记录。

### F022 `start_runtime_engine()` 启动失败泄漏 archive worker

级别：P1

现象：Python 启动 runtime engine 时，archive worker 已启动后若后续失败，worker 未完整清理；同时 engine 可能被 consume。

影响：失败重试会留下后台线程和资源，造成重复写入或进程无法退出。

修复建议：

- 启动流程分阶段，失败时按逆序 cleanup。
- engine ownership 在 start 成功前不应不可恢复转移。

### F023 native source sink capsule 生命周期存在 UAF 风险

级别：P0

现象：native source bridge 中 sink capsule 生命周期与 Rust/Python 对象绑定不够清晰，存在 use-after-free 风险。

影响：native callback 可能访问已释放 sink，导致崩溃或数据破坏。

修复建议：

- capsule 持有 Arc/owned handle，不使用裸生命周期假设。
- stop/drop 时明确取消 callback，再释放 sink。
- 用 sanitizer 或 stress test 覆盖 source start/stop/restart。

### F024 closed/stale bus writer/reader 仍可访问 mmap

级别：P1

现象：close/stale 后 Writer/Reader 对象仍持有 mmap/ring 能力。

影响：控制面已 detach 不等于数据面访问失效，可能继续读写旧共享内存。

修复建议：

- close 后所有 read/write 方法检查 closed 并拒绝。
- detach 后必要时让 ring descriptor 失效或校验 generation。

### F025 bus frame-level instrument filter 可能返回混合 instrument

级别：P1

现象：过滤以 frame 为单位，若一个 frame 包含多个 instrument，过滤结果可能包含非目标 instrument。

影响：订阅过滤语义不可靠，交易/行情下游可能收到不该处理的标的。

修复建议：

- filter 应在 row-level 生效。
- 若只能 frame-level，则 writer 必须保证单 frame 单 instrument，并在 schema/协议中声明。

## 四、持久化、replay 与 compaction

### F026 Python `Pipeline` / `replay()` writer_epoch 不匹配

级别：P0

证据：

- SegmentStore 默认 writer_epoch 是本地 `store_id`。
- Rust materializer 支持 explicit writer_epoch。
- Python binding 使用 `new_with_row_capacity` / `new`，未传 master epoch。
- gateway 使用正确模式：从 master 取 `writer_epoch` 后传给 materializer。

复现结果：

```text
zippy.replay() -> segment descriptor publisher not authorized
```

影响：Python 高层 replay/pipeline 发布 descriptor 会被 master 拒绝，或在 epoch 漂移时行为不稳定。

修复建议：

- Python Pipeline/ParquetReplayEngine 从 master 获取 stream writer_epoch，并传入 materializer。
- 失败时清理部分注册资源。
- 增加跨进程 replay 持久化回放测试。

### F027 persisted snapshot 不持有 lease，固定 snapshot 会悬空

级别：P0

证据：`Query.snapshot()` 只在读取 active control 时创建 lease set，返回前 lease 已 drop；`Table.snapshot()` 缓存的是 dict。

复现结果：

```text
snapshot active segment (1,0)
leases_after_snapshot []
... later collect -> shared memory error: No such file or directory
```

影响：`snapshot=True` 不能保证可重复读取，查询边界可能指向已删除 segment。

修复建议：

- snapshot 对象应持有 lease guard，直到 query 完成或 snapshot 释放。
- Python `Table` fixed snapshot 应关联 native lease handle。

### F028 remote `snapshot=True` 未被 gateway 执行

级别：P1

证据：Python remote collect 发送 `"snapshot": bool(snapshot)`；gateway `handle_collect` 未使用 snapshot 字段。

复现结果：

```text
first collect -> 1 row ['IF2606']
write another row
second collect on same snapshot table -> 2 rows ['IF2606', 'IF2607']
```

影响：本地/远端 snapshot 语义不一致，研究复现实验会漂移。

修复建议：

- gateway 支持 snapshot id / snapshot pinning。
- 或 Python 侧对 remote snapshot 明确报错，不假装支持。

### F029 segment live subscriber 只有单个 descriptor update slot

级别：P1

现象：live subscriber descriptor 更新为单槽，快速连续轮转可能跳过 descriptor。

影响：订阅者可能漏读 sealed segment 或切换到不完整边界。

修复建议：

- descriptor update 使用队列或 generation catch-up。
- reader 检测 generation gap 后主动从 master 拉全量 sealed/active snapshot。

### F030 `TimeSeriesEngine.on_flush()` 有重复窗口风险

级别：P1

现象：flush 边界处理可能重复处理上一个窗口数据。

影响：因子结果、PnL 或回测指标可能重复计数。

修复建议：

- 明确 watermark 与 processed offset。
- flush 幂等化，增加重复 flush 测试。

### F031 `ReactiveStateEngine` stateful factor 缺少事务/rollback

级别：P1

现象：stateful factor 更新过程中如果后续输出/发布失败，内部状态已前进。

影响：重试会产生不可重复结果。

修复建议：

- batch 级状态变更先写 staging。
- 输出发布成功后 commit；失败 rollback。

### F032 `record_batch_from_table_rows()` 全长 fast path 忽略非 identity order

级别：P1

现象：全长 fast path 在存在非 identity order 时仍可能直接返回原 batch。

影响：排序/重排后的 record batch 行顺序错误。

修复建议：

- fast path 必须同时检查 row selection 是 identity。
- 增加全长 reorder 测试。

### F033 compaction `delete_sources=True` 直接 unlink metadata 路径

级别：P0

证据：`ops.compact_table()` 从 stream persisted metadata 收集 `file_path`，metadata replacement 后直接 `Path.unlink()`。

影响：若 metadata 已被污染，compaction 也可删除任意文件。

修复建议：

- compaction 只处理受控 data root 下文件。
- replacement 与 source deletion 使用事务和路径校验。

### F034 `_scan_persisted()` 直接用 metadata 路径构造 dataset

级别：P1

证据：Python `_scan_persisted()` 直接收集 `persisted_files[].file_path` 并传给 `ds.dataset()`。

影响：路径污染会影响本地 dataset 扫描；错误 metadata 可绕过 table 边界。

修复建议：

- 统一通过 validated persisted file resolver。
- 不允许用户态直接信任 metadata 中的绝对路径。

## 五、gateway 远端查询语义

### F035 gateway collect 未执行 `snapshot=True`

级别：P1

同 F027，单列为 gateway 查询语义问题。

修复建议：实现 remote snapshot pinning 或拒绝 remote snapshot。

### F036 gateway `head/slice + filter` 改变执行顺序

级别：P0

证据：gateway 把 row-range 作为 prefix 下推，同时把其后的 leading filter 也下推到扫描阶段。

复现结果：

```text
head(2).filter(instrument_id == "A")
expected {'instrument_id': ['A'], 'seq': [1]}
remote   {'instrument_id': ['A', 'A'], 'seq': [1, 3]}

slice(1, 2).filter(instrument_id == "A")
expected {'instrument_id': ['A'], 'seq': [3]}
remote   {'instrument_id': ['A', 'A'], 'seq': [3, 4]}
```

影响：远端查询返回错误数据，不只是性能问题。

修复建议：

- 只有位于 row-range 之前且可交换的 filter 才能下推。
- 增加 plan optimizer 的算子可交换性规则。
- residual plan 必须保留原始顺序。

### F037 gateway 盲目下推不支持的 filter 表达式

级别：P1

证据：`collect_plan_leading_filter_count()` 只看 op 名；gateway filter evaluator 只支持列-常量比较。

复现结果：

```text
filter((col("seq") + 1) > 2)
remote_error native gateway filter requires column-literal comparison
```

影响：本地可执行的表达式在远端失败，API 能力不一致。

修复建议：

- 增加 `is_filter_pushdown_supported()`。
- 不支持的表达式留在 residual plan，或 gateway 实现完整表达式 evaluator。

### F038 Python `select()` 支持表达式，但 gateway 只支持裸列

级别：P1

证据：Python `Table.select()` 接受表达式；gateway `apply_select()` 只接受 `kind == "col"`。

复现结果：

```text
select((col("seq") + 1).alias("seq_plus"))
remote_error native gateway select currently supports column expressions
```

影响：同一 API 本地可用、远端不可用。

修复建议：

- gateway 支持完整 select 表达式。
- 或 Python 在远端模式提前做 capability 检查并明确报错。

### F039 gateway projection/filter 顺序脆弱

级别：P1

证据：gateway scan pushdown 先 project batch，再 apply filter。虽然当前 projection collector 会收集 filter/select/sort 列，但对 unsupported expr、rename/drop/with_columns 等组合缺少完整证明。

影响：复杂计划可能因 filter 列被投影掉而失败，或 residual plan 缺列。

修复建议：

- 先 filter 后 project，或 projection 必须由严格的 required-column 分析产生。
- 对每个 plan op 写 required input columns / produced columns 分析。

### F040 gateway `with_columns` 不支持

级别：P1

证据：Python `_query_plan_to_json()` 会发送 `with_columns`；gateway `apply_collect_plan()` 不支持 `with_columns`。

影响：远端 query 使用 `with_columns()` 会运行时失败。

修复建议：

- gateway 实现 `with_columns` 表达式。
- 或 Python 侧远端模式提前拒绝，并提示 fallback 到本地。

### F041 gateway `join` 不支持但 Python API 暴露同一 Table 接口

级别：P2

证据：Python `_query_plan_to_json()` 对 join 直接抛出 "remote query join is not supported"。

影响：远端/本地 Table 能力不一致，用户直到 collect 才发现。

修复建议：

- 在构造 join 时若 executor 是 remote，提前给出 capability error。
- 文档列出 remote gateway 支持的 plan subset。

### F042 remote subscribe 暴露 filter 参数但 gateway 未实现

级别：P1

证据：Python `subscribe()` / `subscribe_table()` 发送 filter；gateway 收到非空 filter 直接报错。

影响：API 形态承诺了 filter，但远端订阅不能用。

修复建议：

- Python 侧 remote mode 提前拒绝 filter。
- 或 gateway 实现 subscribe filter，并复用 collect filter capability。

### F043 gateway 认证前读取完整 frame，且无大小限制

级别：P0

证据：`handle_client()` 先 `read_frame()`，再 `authorize()`；`read_frame()` 信任 header_len/payload_len 并分配 Vec。

复现结果：带 token 的 gateway，错误 token + 1MB payload 会被完整读取后才返回 unauthorized。

影响：未授权连接可消耗内存/线程，存在 OOM/slowloris 风险。

修复建议：

- 先读取并限制 header，认证通过后再读 payload。
- 增加 header/payload 最大大小、read timeout、connection limit。
- 避免每连接无界 `thread::spawn`。

### F044 gateway 每连接无界线程

级别：P1

证据：gateway accept 后直接 `thread::spawn` 处理连接。

影响：大量连接可耗尽线程资源。

修复建议：

- 使用 bounded thread pool 或 async runtime。
- 设置最大并发连接数和 per-IP 限制。

## 六、数值、表达式和数据确定性

### F045 `TsReturn` 除零产生 inf

级别：P1

现象：`TsReturn` 对 previous value 为 0 的情况未显式处理，可能产生 `inf`。

影响：非有限值进入因子链和下游风控。

修复建议：

- 除零返回 null/NaN/错误需明确配置。
- 默认拒绝产生 `inf`，并记录指标。

### F046 非有限 float 会污染 reactive/TS

级别：P1

现象：NaN/inf 可进入状态引擎并持续影响后续结果。

影响：同一输入序列可能因单个坏值导致长期不可恢复状态。

修复建议：

- 输入层统一 validate finite。
- factor spec 明确 null/NaN/inf 策略。

### F047 `Log` 允许 NaN 通过

级别：P1

现象：log 对负数/非法输入的 NaN 处理不严格。

影响：隐藏数据质量问题。

修复建议：

- 对非法 domain 输入返回 null 或抛出受控错误。
- metrics 记录 invalid_domain_count。

### F048 `CastSpec` 静默 cast NaN/inf

级别：P1

现象：cast 可能静默处理非有限浮点。

影响：数值语义不透明，研究结果不可审计。

修复建议：

- cast 增加 strict mode。
- 默认对 NaN/inf 到整数/布尔等危险转换报错。

### F049 Python factor spec 暴露 `id_column` 但构建忽略

级别：P1

现象：API 参数存在但实际 build 不使用。

影响：用户以为按某 id 分组，实际结果可能按默认列或无分组计算。

修复建议：

- build 阶段使用 spec `id_column`。
- 若不支持，移除参数或显式报错。

### F050 `id_filter` 接受空字符串

级别：P2

影响：空 id 可能被当作真实 instrument/key，过滤语义不清。

修复建议：

- normalize 时拒绝空字符串和纯空白。
- 错误信息包含参数名和非法值。

## 七、API 暴露与诊断

### F051 Python `get_stream/list_streams` 缺少 `writer_epoch` 和 `write_seq`

级别：P2

证据：Rust `StreamInfo` 包含 writer_epoch/write_seq，server 也设置；Python dict 转换遗漏关键字段。

影响：用户无法从 Python 诊断 epoch mismatch、ghost writer、write seq 漂移。

修复建议：

- Python binding 暴露完整 StreamInfo。
- 对控制面字段做 snapshot 测试，防止遗漏。

### F052 `Table._scan_persisted()` 暴露低层路径信任

级别：P1

同 F033。

修复建议：经 validated resolver 解析，不直接暴露 metadata 路径。

### F053 remote/local API 能力没有 capability 描述

级别：P2

现象：同一个 `Table` API 在 local native 和 remote gateway 下能力差异大，但没有显式 capability。

影响：用户只有运行到 collect/subscribe 时才知道失败。

修复建议：

- `Table.explain()` 增加 executor capability 和 unsupported ops。
- 构造 query 时可选择 strict/fallback 策略。

## 八、共享内存与 descriptor 一致性

### F054 stream metadata `buffer_size` 与 active descriptor `row_capacity` 不一致

级别：P2

观察：gateway 写入测试中，stream metadata 显示 `buffer_size=64`，active descriptor `row_capacity=65536`。

影响：诊断信息容易误导；如果用户用 buffer_size 估算 retention/容量，会得出错误结论。

修复建议：

- 区分 bus ring buffer size 与 segment table row capacity，字段命名不要混用。
- 在 StreamInfo 中显式暴露 segment row capacity。

### F055 active descriptor / shm 路径信任不足

级别：P1

现象：控制面记录 descriptor 后，读路径信任其 shm path；ghost descriptor 会导致 shared memory error。

影响：descriptor 与实际 shm 生命周期脱节。

修复建议：

- descriptor 发布时校验 writer_epoch、process、source owner 和 shm 可达性。
- descriptor 读取时区分 missing shm、stale descriptor、permission error。

### F056 stale/closed bus reader writer 状态没有统一 generation 校验

级别：P1

影响：旧句柄可能继续访问新语义下已失效的共享内存。

修复建议：

- 每次 read/write 校验 descriptor generation/writer_epoch。
- master detach 后句柄应进入不可用状态。

## 九、建议修复顺序

### 第一阶段：P0 控制面和文件系统安全

1. 修复 `replace_persisted_files` 授权、writer_epoch 校验和路径边界。
2. 修复 `drop_table` 授权、路径边界和事务语义。
3. 修复 `UpdateStatus` owner 校验。
4. 修复 raw register source/engine/sink 的 process 身份伪造。
5. 修复 raw `UnregisterProcess` 注销他人进程和 registry/bus 分叉。
6. 修复 raw `Heartbeat` 替他人 process 续租。
7. 修复 `RegisterStream` 无身份校验和 stream name 路径逃逸。
8. 修复 raw `WriteTo` / `ReadFrom` ghost process attach。
9. 修复 raw `CloseWriter` / `CloseReader` 冒充 owner 关闭他人句柄。
10. 修复 raw `Publish*` 冒充 owner 发布 descriptor 和持久化元数据。
11. 修复 fake lease 和 snapshot lease 生命周期。
12. 修复 gateway 认证前读取 payload、无大小限制和无界线程。

验收标准：

- 未注册 client 不能 replace persisted metadata。
- 非 owner 不能 drop table / update status。
- 未注册或非 owner client 不能伪造 process 注册 source/engine/sink。
- 非 owner 不能注销其他 process；注销 process 后 registry 和 bus 状态一致。
- 非 owner 不能替其他 process heartbeat。
- 未授权 client 不能注册 stream；stream name 不能影响 bus root 外路径。
- 未注册 process 不能 attach writer/reader。
- 非 owner 不能关闭其他 writer/reader。
- 非 owner 不能发布 segment descriptor、persisted file 或 persist event。
- 任意绝对路径、`..`、非 data root 路径均被拒绝。
- drop 删除失败不会造成 registry 已删除但接口报错的状态。
- gateway 错误 token 不读取大 payload。

### 第二阶段：P0/P1 查询正确性

1. 修复 gateway row-range/filter 下推顺序。
2. 增加 filter/select/with_columns/sort/drop/rename capability 检测。
3. remote snapshot 要么真实实现，要么提前报错。
4. 对 local/remote 同一查询做 golden parity tests。

验收标准：

- `head().filter()`、`slice().filter()`、`tail().filter()` 结果与本地一致。
- unsupported expression 不会被盲目下推。
- remote `snapshot=True` 不再 silently drift。

### 第三阶段：生命周期和健康判断

1. `Pipeline.stop()`、`RemoteGatewayWriter.close()`、`GatewayServer.stop()` 清理 writer/source/descriptor。
2. health 和 wait ready 增加 shm preflight。
3. 注册失败路径全部补偿回滚。

验收标准：

- stop 后 stream 不再显示 ghost writer_attached。
- health 能识别 missing shm。
- 失败重试不会留下半注册资源。

### 第四阶段：数值和 factor 确定性

1. 明确 NaN/inf/null 策略。
2. 修复 TsReturn/Log/CastSpec 非有限值行为。
3. stateful factor 增加事务/rollback。
4. `id_column` 参数要么生效，要么报错。

验收标准：

- 非有限输入有明确、可测的输出策略。
- 状态引擎失败重试 deterministic。
- factor 参数和实际执行一致。

### 第五阶段：工具链和发布门禁

1. 恢复 Cargo / pytest / Python 工具链基线。
2. 增加 `python -m zippy` smoke test。
3. Python package 加 `py.typed`。
4. CI 覆盖 Rust workspace、Python API、gateway parity、安全负例。

验收标准：

- `cargo test`、`uv run pytest`、black/ruff/mypy 均可作为发布门禁。
- 所有 P0/P1 修复都有回归测试。

## 十、需要补充的审计方向

这些方向尚未完成穷尽验证，不能据此认为没有问题：

- master snapshot restore 对污染 metadata 的恢复行为。
- 多 gateway 同 stream 写入时 writer_epoch 和 descriptor ownership。
- shared memory 文件权限、清理、跨用户隔离。
- parquet schema evolution 与旧 persisted files 的兼容策略。
- Windows 路径和 TCP master 场景下的权限边界。
- 高并发 register/drop/replace/read 的锁顺序和死锁风险。
- gateway subscription 的断线重连补数据语义。
- 性能测试是否覆盖真实 tick/IPC/多进程链路。
