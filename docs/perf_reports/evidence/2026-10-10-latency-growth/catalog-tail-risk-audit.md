# 阶段 1.7：Catalog revision 尾部风险审阅

本次只做源码与既有证据审阅，没有测量 Catalog 排队等待，没有新增基准或插桩。因此不能将候选 v1 的尾部尖峰归因于 revision，也不建议据此改变生产调度。

可确认的调用变化：旧、新 live 刷新的第一次表解析都进入 control 读队列（`crates/zippy-catalog/src/service.rs:671`、`:687`）；旧第二次完整快照进入 snapshot 读队列（`:900`），新第二次 revision 进入 control 读队列（`:913`、`:915`）。新调用点为 `crates/zippy-server/src/live_log_snapshot.rs:136`。对已追上、无重试的一次刷新，变化是“一次 control + 一次 snapshot”变为“两次 control”，不是新增一个总请求。revision 的实际 SQL 只按主键读取 Catalog 元信息（`crates/zippy-catalog/src/layout.rs:379`、`:386`）。

不能称为“revision 排在 background commit/fsync 后”：writer 与 read endpoint 使用不同发送器（`crates/zippy-catalog/src/service.rs:1633`、`:1639`、`:1653`、`:1677`）；writer actor 打开自身连接（`:2262`、`:2268`），control 和 snapshot 分别创建读线程与独立连接（`:1724`、`:1735`、`:1863`）。control 消息枚举不包含写提交（`:1524`），提交的 manifest finalization 在 writer actor 执行（`:2519`、`:2520`）。只读连接为 READ_ONLY、query_only，并验证 WAL 模式（`crates/zippy-catalog/src/lib.rs:464`、`:481`、`:484`）；writer 的 FULL 同步配置见 `:581`。源码没有让 revision 在同一 FIFO 中直接等待写提交的依赖。

静态上仍存在的风险是 control 队列内部的串行等待：`control_read_loop` 取出一条消息后同步完成 backend 调用，才取下一条（`crates/zippy-catalog/src/service.rs:1896`、`:1901`、`:1910`）。因此 revision 可以等待前面的 resolve、list、read_schemas、inspect 或 replay read（`:1918`–`:1983`），执行中的 SQLite 调用也占用该线程。新实现增加了对这个读队列的一次访问；原 resolve 已有同类暴露。队列满时 `try_send` 返回 ResourceExhausted，未满时接受后等待响应（`:1829`–`:1845`）。只读连接也设置 busy_timeout，默认五秒（`crates/zippy-catalog/src/lib.rs:76`、`:472`）；独立队列不能排除共享磁盘、SQLite 锁或 CPU 调度的间接影响，但现有 CPU/边界数据不能区分这些机制。

若后续需要定位，最小临时方案是在 `/tmp` 源码副本构建的诊断二进制中，为单次刷新加入同一关联编号，记录 resolve/Actor/revision 的 await 前后时间，并在 control/snapshot worker 记录对应请求入队、开始 backend、完成 backend 和响应交付时间。使用同一 monotonic 时钟、固定容量内存缓冲，保留慢请求及固定比例普通请求，消费结束后写盘；不改变请求顺序、完整性检查或预算。由此分别计算队列等待、SQLite/backend 执行和异步恢复等待。另记录 writer transaction commit 的起止，只有与慢读时间重叠还不足以证明 fsync 因果；需要实际同步系统调用或 off-CPU 证据进一步区分。所有临时诊断需与无插桩运行分开标注，不能替代本次原基准验收。

当前应先完成修正版的原基准与既定功能验证。候选 v1 的首轮四格和十分钟数据应按各自二进制身份保留；在缺乏排队实测时，把 control lane 变化列为后续测量候选即可，不据此豁免失败或调整调度。
