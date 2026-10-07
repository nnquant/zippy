# 第三十八批：删除 Python 旧主干，扩大全仓类型门禁

日期：2026-09-22。本批关闭 **F22/F29**，累计 **27/34 已关闭，7 项开放**。

主包 `__init__.py` 从 9,915 行缩为 164 行，仅导出服务器、计算能力和同一个独立客户端。
删除全局默认 Master、heartbeat/watch/shutdown hooks、旧 Ops/文件 compaction、Table、本地
Parquet 扫描与 Python residual、Gateway writer/query/subscriber、旧 SHM 回调、Session/Pipeline
注册与物化逻辑、旧描述符和 shard 解释器。没有把它们移动到另一个兼容模块。
`factors.py` 承载纯因子/策略配置，`replay_compare.py` 只比较显式 Arrow Table；不再接受表名、
Master 或任意对象转换回退。CLI 的 URI 解析复用独立客户端，补齐明确文件路径的识别规则。
SDK control 中无人使用的旧 list_streams/get_stream 也已删除。

三类表、远端 callback、五类计算内核、顺序 chain、有限 Python/Parquet 源和计算结果恢复
继续存在。18 个依赖旧入口的示例删除，由 7 个原生示例接替；已迁移的 06/08 示例保留。
新示例覆盖 append 建表写读、frame/KV 持久变化、固定查询、两种 callback、三段本地 chain、
三类表驱动计算和等待建表。指南明确 schema、排序、UTC 纳秒、生命周期、绝对游标与确认边界。
旧 Session/Pipeline 表发布设计标记为历史，当前文档不再推荐旧入口。

## 真实验收中修复的问题

- 新状态示例初次误用 transport 的 write_table 名称，改用 Client 公开 write_frame、
  upsert_key_value 和只携带键列的 delete_key_value，三类数据在重启后逐值验收。
- 时序聚合要求 float64，输入 volume 为 int64。示例通过明确的表达式 CAST 转换后聚合，
  保留输入表整数 schema；不依赖隐式截断或修改原数据。
- 缺表等待耗尽 RPC deadline 时，服务端返回不可重试的 DEADLINE_EXCEEDED。SDK 原先先
  判断 retryable 而提前抛 ServerError，现仅在调用方明确设置等待预算时转为 TimeoutError，
  保留 cause。无界等待不吞掉该错误；真实缺表超时与先等待后建表均覆盖。
- 旧 `_internal.pyi` 把 Python Duration/策略值写成了不存在的原生类型；现引用实际实现，
  不同策略种类使用不同类型，错误种类仍由运行时拒绝测试验证。
- 回放比较在统计差异时也忽略字段描述元数据。修复前归档包将 `[1,1]` 与 `[1,2]` 的
  不同字段 metadata 误计为两行缺失/新增，现共用同一 schema 编码，正确各计一行；最终
  wheel 在修正后重新构建。

## 测试退出与类型检查

旧 API 退出时同步删除 310 个旧主包测试函数，另有 8 个旧控制/描述符 fixture 测试退出。
它们不是为清空类型错误而删除；生产实现先删除，再按行为核对接替路径。96 个不依赖旧主干的
本地内核与主包测试函数保留。按函数的删除理由、所属旧入口、原生测试接替位置见
[退役清单](perf_reports/evidence/2026-09-22-main-python-trunk/test-retirement.json)。
旧文件 compaction 和逻辑 shard descriptor API 明确退役，不冒充新原生实现已提供相同 API。
服务端相关旧权威与内部 materializer 测试仍保留，等待下一步整体删除。

默认 mypy 从生产文件白名单扩大到 python、pytests、scripts、examples、客户端源码及测试；
当前全仓 91 文件通过，独立客户端自身严格配置 27 文件通过。修正实际 stub 身份、容器类型
及接口替身声明；故意传错参数的负向测试使用明确的动态调用边界，保留原异常类型/消息断言。
没有新增 ignore、排除文件或降低配置严格程度。mypy 的成功不等同于动态运行行为已验证，
因此同时保留真实服务、覆盖率与发行门禁。

## 验证结果与剩余边界

- 全仓 pytest：595 passed；计算与回放拆分模块定向 137 passed，覆盖率 90.41%，比较模块 100%。
- 独立客户端源码归档安装：Arrow 23.0.1/25.0.1 各 219 passed，覆盖率 85.19%。
- 仓库二进制、源码归档重建主包和安装后 CLI 执行原生四代恢复；三类表、查询、callback、
  ACK、计算、WebUI、CLI 和公开示例共同通过。
- Black、Ruff、全仓及严格客户端 mypy、Rust fmt、diff 通过。Rust 实现未变，本批没有重复
  第 37 批已通过的工作区/Clippy；发行主包确实从无 Git 的归档重新编译。

详细命令结果、失败后的修正记录、源码与 wheel 哈希见
[验收证据](perf_reports/evidence/2026-09-22-main-python-trunk/summary.json)。
保留的直接 Rust 引擎封装仍可能包含内部旧 source/materializer 绑定；它们与服务器旧目录、
Gateway memory ingest、旧查询/订阅和 Legacy/shadow 一起继续退出。本批不把 Python 收敛
等同于整个服务器已只有一套权威；F07/F08/F11/F13/F14/F15/F28 仍开放。
