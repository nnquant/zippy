# 第四十三批：删除 Python 扩展旧运行时绑定

日期：2026-09-22。承接[第42批](convergence-native-file-contract-2026-09-22.md)。
累计 **28/34**；F07/F08/F11/F13/F14/F28 保持开放，本批未新增关闭编号。

## 删除与保留

删除扩展中的 19 个旧绑定：独立 Master/Gateway、旧 MasterClient、Query、SHM subscriber/source、
测试 writer、两类表 materializer、五类后台计算包装器，以及 Null/ZeroMQ publisher/source/subscriber。
同步移除 Python 主包导出、类型 stub、旧 native_source_sink.v2 capsule 和仅供这些包装器使用的
WindowType/LateDataPolicy/OverflowPolicy/SourceMode 常量，不保留转发 alias。

Rust `lib.rs` 从 13,079 行减至 1,598 行，删除 323 行 capsule 模块；补充原生过滤校验后，
本批 Rust 总计净减 11,789 行。主包入口由 164 行减至 136 行，扩展 stub 由 728 行减至 240 行。
移除 zippy-python 对 Gateway、IO publisher 和 segment-store 的直接依赖；Server 的传递依赖
仍存在，不能据此声称服务端 Gateway 或旧 registry 已完全退出。

保留 NativeProcessor 的五类计算内核、identity/chain、因子规格、KeyValueState 和正式服务
生命周期。计算发布继续使用 ComputePipeline + Flight，采集插件使用 SourcePlugin；
三类输出表及恢复能力不依赖被删除的 Python 后台包装器。使用方式见[原生计算](native-compute.md)。

## 迁移测试发现的缺口

原生 reactive/timeseries 工厂此前只拒绝空过滤集合，未拒绝集合内空字符串或纯空白值。
旧测试迁移后复现四个失败，现已在原生入口补齐校验。合法标识仍按原字符串精确匹配，
不自动 trim；新增正向测试区分 `A` 和 ` A `，并检查未选中的 `B` 不进入结果。
另一个失败来自测试仍传旧截面构造参数，已按当前接口修正，保留非法因子类型的拒绝断言。
首次失败日志保留。

20 个旧计算语义测试转到 NativeProcessor，覆盖因子自己的状态键、表达式、全部因子规格、
全部聚合规格、前后置表达式、flush 结果、过滤错误与插件 bar profile。日志测试改为验证
保留的 log_info 入口。其余 11 个回放/发行/服务/Row 合同测试保留。
退役 64 个 Python 测试名称和 43 个 Rust 测试名称，均绑定已删除的后台配置、数据协议、
SHM lease/切换或旧 materializer；新增 23 个导出边界参数用例和两个精确过滤用例。
完整名称与替代验收见[清单](perf_reports/evidence/2026-09-22-python-native-bindings/test-retirement.json)。

## 验证

- 重建当前扩展后，Python 全量 **550 项通过**；不再把第42批的 595 项当成本批测试数量。
- 扩展 Rust 剩余一个错误码合同测试通过；全 workspace/all-targets Clippy 零警告。
  全 Rust workspace 执行证据属于第42批，本批没有把定向执行描述为再次全量执行。
- 原生真实矩阵复验 append/frame/KV、固定快照、schema/drop、订阅/ACK、计算、采集插件、
  公开示例及重启恢复全部通过。服务端源码与二进制沿用第42批已验收版本，Python 扩展重新构建。
- mypy 91 文件、客户端严格配置 27 文件、Ruff、Black 92 文件、Rustfmt 和 diff 检查通过。
- 当前 Linux 扩展动态库依赖全部可解析。

源码、二进制、日志摘要见[结构化证据](perf_reports/evidence/2026-09-22-python-native-bindings/summary.json)。
Linux/CPython 3.11/Arrow 23.0.1 验收不替代其他平台或重新构建的 Arrow 25 发行验收。
本批没有提交、推送或部署；剩余重点仍是服务端默认 Legacy、持久 registry、Gateway、
memory ingest、重复查询/订阅和 Rust 内部旧 source/materializer，以及 F28 平台矩阵。
