# 第二十七批：计算输出恢复与持久批次精确重放

日期：2026-09-22。推进 F08/F13/F22/F29；原审计仍为 **25/34 已关闭、9 项开放**。
本批补齐同一个本地计算内核跨存储服务重启后的输出延续，不将存储服务恢复描述为计算进程的持久 checkpoint。

## 恢复原 producer，而不重新分配批次序号

原生恢复能找回 producer、epoch、持久批次序号和回执，但原活动连接已经消失。此前用新的
DoPut 请求重新打开 writer，会走 Catalog AcquireProducer 并推进 epoch、重置序号，导致仍然
存活的计算任务无法继续原输出序列。

现在只在没有活动连接、producer 与恢复身份一致、调用方明确给出当前 epoch 时恢复原 writer。
恢复仍经过逐表 transition gate、actor fence 和 WAL 打开流程，保留既有序号与持久回执。
实际测试曾暴露直接返回 WriterInfo 会缺少可写 WAL 段，现已复用完整安装流程修复。
原始 AcquireProducer 请求的精确重放仍保留幂等性；这是正确性契约，不是旧版本兼容分支。
其他连接接管后会推进 epoch，旧 writer 必须失败，不能借恢复绕开隔离。
恢复次数计入 `writer_resumes_total`，不冒充新 producer acquisition。

## 重试输出，不重复执行内核

服务公布 `ingest.resume_after_restart.v1`。`FlightTransport.write_table()` 增加显式
`max_reconnect_attempts=0..10`，默认仍为 0。启用恢复必须使用 WAL/Persisted，并传入
`expected_table_id`，防止重试写入同名新表。

恢复模式每个有界 Arrow batch 使用独立 DoPut，同一重试保留 put ID、producer/connection ID、
批次序号、原 epoch、schema 和完整输出数据，因而服务端得到相同幂等键与指纹。
只在传输中断、会话失效或服务端明确允许重试时重新认证并退避重试；身份、schema、writer epoch
冲突以及协议损坏直接终止。重试次数和每次 RPC 均有界；单批独立 DoPut 增加 RPC 开销，
本批未声称恢复模式与持久流复用有相同性能。

`ComputePipeline` 对 WAL/Persisted 输出默认采用 5 次重连，可用 `output_reconnect_attempts`
调整。内核仅处理一次输入，重试发生在其已生成的输出上。Memory 输出不启用该恢复。
重试耗尽或结果仍无法确认时任务继续保持 terminal failure，并保存已确认回执；调用方不能用新身份
盲目重写整批。未实现计算进程重启后的算子状态恢复，也没有把 durable consumer ACK 当作内核 checkpoint。

## 验证与边界

- Rust 回归同时验证原始 acquisition 精确重放、新请求恢复原 epoch、继续下一个批次、恢复计数以及
  新连接接管后旧 writer 拒绝；完整工作区和 Clippy 门禁通过。
- 客户端故障测试检查丢回执前后 put ID、序号、producer/connection ID 和数据完全一致，重认证发生，
  下一批使用不同请求身份；不可重试冲突只尝试一次，Memory 和不安全配置明确拒绝。
- 真实服务中保留同一个 `ComputePipeline` 与 Rust 内核，输入价格 1、3、5；跨存储服务重启后
  输出均值仍为 null、2、4。append/frame/KV 三类输出的 producer epoch 和批次序号均延续。
- 在真实 durable commit 后、计算任务接收回执前注入断连。append 额外在这个窗口正常停服并重启，
  再从恢复后的持久回执重放；这不是 SIGKILL 或断电模拟。查询和逐行持久日志证明输出没有重复。
- 主包/客户端完整 Python 回归、Arrow 23.0.1/25.0.1 仓库外独立客户端发行及覆盖率门禁通过。
- 配置范围 mypy 通过；全仓扩展检查仍有 978 个错误，不能关闭 F29。

具体结果、命令日志摘要和哈希见[验收证据](perf_reports/evidence/2026-09-22-compute-recovery/summary.json)。
本机验证为 Linux x86_64 / Python 3.11，未执行远端 Windows/macOS CI 或完整主包隔离发行矩阵。

KV 计算状态输入、算子 checkpoint、旧 source/Session/Pipeline、主包查询/回调及旧控制面仍待迁移。
F07/F08/F11/F13/F14/F15/F22/F28/F29 继续开放，旧 Gateway/Legacy/shadow 尚未整体退出。
