# 第三十批：Python source 接入原生计算与写入

后续：本批记录的恢复日志超时已在[第三十一批](convergence-engine-chain-2026-09-22.md)定位为
同 producer epoch 重启后的帧身份复用，并通过确定性回归修复。以下保留当时的验证记录。

日期：2026-09-22。推进 F08/F13/F22/F29；原审计仍为 **25/34 已关闭、9 项开放**。
本批补齐旧 source 插件协议到原生计算的入口，为旧 Session 注册和发布链路退出提供接替路径。
没有删除尚未完成整体迁移的旧 source/Session/Pipeline 或控制面。

`ComputePipeline.source` 现在接受表名或 SourcePlugin。插件输入与 Flight 输入共用同一个
NativeProcessor、严格 schema/完整批次限额、输出身份 guard 和持久回执恢复实现。插件在转移
内核前校验声明 schema，拒绝混用原生日志 options。它不创建 Master source、SHM publisher
或 Gateway writer，也不增加输入预取队列。

`source_input.py` 定义 SourcePlugin/SourceHandle/SourceSink 协议和拥有明确生命周期的输入适配器。
支持 `_zippy_start()` 同步发出数据，hello 校验源身份和协议，Arrow 数据/flush 串行处理，
每次交付等输出确认后才返回采集端。源线程 join 异常、未发终止事件就退出、协议错误以及内核/
输出失败都保持 terminal，并传播首个异常。取消时拒绝迟到数据；源 join 默认最多等待五秒，
插件自己的 stop() 必须及时返回。没有把这一约束描述为可以强制终止任意不合作 Python 线程。

已有 ParquetReplaySource 无需改成旧 Master 注册对象，即可驱动同一个原生计算流水线。
其原契约是读取完成后 flush 并等待外部 stop，示例使用 wait_replay() 后退出上下文；其他插件
可以 emit_stop 后正常退出。采集恢复、顺序与外部缓冲由插件负责，适配器不宣称提供输入游标
持久化或算子 checkpoint。

## 验证

- 原生 source 与计算回归最终 28 passed；计算模块覆盖率 87%、source 模块 91%，合计 89.07%。
  覆盖同步启动发数、输出确认后返回、schema 拒绝前保留内核所有权、协议错误、源 join 失败、
  停止超时、Parquet 输入、输出失败只执行一次，以及取消后迟到输入不写入。
- 完整 Python 回归 854 passed、1 skipped；其后新增的取消竞态测试包含在上述最终 28 项中。
- 真实服务读取 Parquet 的 1/3/5，原生均值为 null/2/4；验证 append/frame/KV 三类持久结果。
  各代重启后重新读取相同结果；frame 传输分片为单行时仍还原为 2 行、1 行两个完整帧。
- 最终矩阵第一次在既有输出恢复的逐条日志检查中超时，未查明原因。补充表名/已读游标诊断后，
  保持原五秒期限和全部断言复验通过；失败与成功日志均保留。这次复验不等于已消除间歇性超时，
  仍归入 F11/F13 的恢复稳定性复核范围。
- source_input 已加入根 mypy 门禁，配置范围从 15 扩为 16 文件；本轮扩展检查通过。
  全仓 mypy 仍有 976 个错误，涉及 12 文件，检查 97 文件，F29 不关闭。
- Black/Ruff/diff 检查通过。本批未修改 Rust 或独立客户端，不重复其上一批 workspace、Clippy
  和独立客户端 Arrow 23/25 发行验收；主包完整隔离发行与跨平台矩阵仍属于剩余项。

日志、哈希及真实矩阵结果见
[验收证据](perf_reports/evidence/2026-09-22-source-input/summary.json)。

旧 Session/Pipeline、多级引擎连接、主包本地/远端订阅、查询 residual、KV 状态因子适配、
算子 checkpoint 和 Gateway/Legacy/shadow 退出仍待完成。
F07/F08/F11/F13/F14/F15/F22/F28/F29 保持开放。
