# Control Plane Lifecycle Runbook

本文档定义 zippy master、子节点、stream writer source 的生命周期处理口径。

## 正常退出

1. master 收到 graceful shutdown 请求。
2. master 进入 `shutdown_requested`，停止接受新 process 注册。
3. 子节点通过 `Watch(resource=Shutdown)` 或 heartbeat response 收到关闭事件。
4. 写入方先停止接收新数据，再 flush active segment / persist worker。
5. 写入方完成安全落盘后调用 `unregister_process()`。
6. master 观察到所有 alive process 已 unregister 后退出。

正常退出不应把未退出 process 标记为 `lost`，也不应抢占 writer ownership。

## 超时退出

master 等待子节点 unregister 超过 timeout 时：

- 返回 `timed_out_processes`；
- master 保持运行；
- 不把未退出 process 标记为 `lost`；
- 不释放 active writer source；
- 不写入“假成功”的 shutdown snapshot。

这保证 master 不会在不确定子节点是否完成 flush/persist 时主动制造数据一致性风险。

## 强杀或异常退出

如果 master 被强杀：

- 已写入 snapshot 的 stream/source/persist metadata 会在下次启动恢复；
- active segment 仍由写入方进程持有；
- 未完成 unregister 的 process/source 会以 snapshot 中的状态恢复；
- 新 writer 接管前必须经过 writer source fencing。

如果子节点被强杀：

- lease reaper 会在 process lease timeout 后把 process 标记为 expired；
- 该 process 关联的 source/engine/sink 标记为 `lost`；
- 对应 stream 进入 stale 语义；
- 新 source 可接管该 stream，并获得新的 `writer_epoch`。

## Writer 接管

同一 stream 同一时间只能有一个 active writer source。

接管流程：

1. 旧 source 状态变为 `lost`，或旧 process lease expired。
2. 新 source 注册到同一 output stream。
3. master 为 stream/source 分配新的 `writer_epoch`。
4. 新 writer 发布 descriptor/persist metadata 时必须携带当前 epoch，或由 master 注入当前 epoch。
5. 旧 writer 后续携带旧 epoch 发布 descriptor/persist metadata 时会被拒绝。

## Remote Gateway Writer

Gateway writer 不是独立信任源，它必须从 master 获取 writer epoch：

1. Gateway 注册 stream。
2. Gateway 注册 `gateway.<stream>` source。
3. Gateway 读取 `StreamInfo.writer_epoch`。
4. Gateway 使用该 epoch 初始化本地 segment writer。
5. Gateway 后续 descriptor publish 由 master 验证 epoch。

这样可以避免远程 writer 用本地 `SegmentStore` 的 store id 作为 writer epoch。

## 验收命令

```bash
cargo test -p zippy-master expired_source_writer_cannot_publish_after_new_source_takeover -- --nocapture
cargo test -p zippy-gateway native_gateway_binds_writer_epoch_to_master_source_epoch -- --nocapture
```
