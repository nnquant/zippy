# 第三十七批：远端诊断统一 Flight，关闭会话回收资源

日期：2026-09-22。推进 F11/F14/F22/F28/F29，累计仍为 **25/34 已关闭，9 项开放**。

删除整个 `zippy gateway` 命令组、`remote_gateway_perf` 辅助模块，以及七个旧 Gateway 远端
示例。手写协议 fallback 和用 Gateway 全局计数推断单客户端交付的路径一起退出。
原生扩展加载失败必须明确失败的测试迁入独立包边界测试，未随旧 smoke 示例删除。

新增两个只依赖 `zippy-client` 的远端示例：有限 persisted 写入，以及固定快照查询与显式
日志起点的行/表回调观察。观察计数来自本句柄，frame/KV 完整原子交付，不为凑够目标行数而
截断帧；最低行数可被最后一个完整单位超过。报告区分首次回调等待与行情事件延迟，批次样本
最多 32 个。用法见[原生远端客户端](native-remote-client.md)。旧平台 runbook 标明入口已退役。

## 实际暴露并修复的问题

首次原生矩阵出现 `Flight session capacity exhausted`。SDK 以前只关闭 Flight 通道，服务端
会话仍占名额直到 TTL；CLI、WebUI 或其他短客户端反复连接即可耗尽容量。

新增 `zippy.close_session.v1`：请求只关闭当前 bearer 所属会话，不能指定他人的会话。服务端
回收名额、未消费 ticket 和固定快照租约，非法版本不会产生回收副作用。SDK 跟踪各角色及
刷新后仍有快照引用的旧会话，关闭时先停流、再逐个撤销并关闭通道；一个清理失败不会阻止
剩余通道关闭。过期会话不重新握手；网络不可达时记录警告，不能宣称远端已立即回收。
已准入的在途 RPC 仍遵循自己的取消和 deadline 契约。

每代真实服务以原有 `max_sessions=32`、TTL 60 秒连续创建并关闭 40 个客户端，各自故意
留下未单独释放的固定快照，再由 Client.close 回收。没有通过调大容量或缩短 TTL 消除失败。
协议与 SDK 同步升级，没有旧服务端 action 回退分支。

## 验证

- 全仓 Python：906 passed、1 skipped；CLI 最终断言更新后另行 32 passed。
- 独立客户端：217 passed，覆盖率 85.10%；Arrow 23.0.1、25.0.1 的纯 Python 归档发行环境
  均为相同结果，且无主包及旧客户端 Gateway 模块。
- Rust 工作区：1779 passed、10 ignored；协议包 51 passed。最终断言写法修正后，会话回收
  回归和 workspace/all-targets Clippy `-D warnings` 通过。
- 仓库服务二进制以及源码归档重建的主包 wheel 均通过四代原生恢复。两种 Arrow 版本的完整
  发行环境由安装后的 CLI 启动，验证三类表、订阅 ACK、计算、固定快照、HTTP、CLI 和新示例。
  示例子进程明确验证未导入 `zippy`；只读凭据写入失败也在真实服务中验收。
- wheel 内 CLI、新 SDK transport 和生成协议逐字节匹配源码，旧诊断模块与命令确实不存在。
- Black 99 文件、Ruff、Rust fmt、协议再生、diff 通过；根 mypy 17 文件、SDK 严格检查 27 文件、
  本批示例/helper 3 文件通过。全仓 `mypy .` 仍有 949 errors / 8 files / 98 checked。

首次容量失败、协议测试编译/fixture 错误和 Clippy 断言失败日志均保留；修复后的成功记录分开
列出。本轮没有 Windows/macOS 实机结果，也没有将全仓类型错误隐藏为通过。

源码、产物及验收日志哈希见[证据](perf_reports/evidence/2026-09-22-flight-diagnostics/summary.json)。
主包 Table/Session/Pipeline 与旧 callback、服务端 Legacy/shadow 和 Gateway 数据权威仍需删除，
F07/F08/F11/F13/F14/F15/F22/F28/F29 保持开放。
