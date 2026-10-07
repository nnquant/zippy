# 第三十一批：原生内核串联与恢复帧身份

日期：2026-09-22。推进 F08/F11/F13/F22/F29；原审计仍为 **25/34 已关闭、9 项开放**。
本批补齐原生顺序多级计算和无因子生产者，并修复真实恢复验收发现的帧身份复用。

## 内核连接与普通采集

Rust EngineChain 接管多个现有内核，完整 Arrow 批次在各级之间直接传递；Python 仅在链的
输入和最终输出处转换。相邻 schema、所有句柄及预算全部验证后才转移所有权，构造失败不
丢弃已有内核状态。每级输出按构造时冻结的 schema 检查；flush/stop 按上游到下游传播尾批。
运行错误使整链终止，不把有状态内核的部分执行当作可重试事务。

每级输出集合以及合并的 flush/finish 结果都受字节限额约束；共享 buffer 重复计费。
限额不包含内核状态或调用内的瞬时分配，不宣称是 RSS 上限。顺序链不等于任意 DAG，
也不提供持久算子 checkpoint。

IdentityEngine 允许无 symbol、无时间列的 Arrow source 走同一原生流水线。完整批次、
顺序、重复值、空值和 buffers 保留，不需要虚构因子或旧 Master publisher。
真实验收脚本将 Parquet 输入依次交给 identity、两期均值、表达式内核，写三类原生输出；
另有无键/时间字段的普通 append 生产者。

## 帧恢复缺陷

第三十批曾出现日志超时。本批再次复现，诊断游标为 `[2, 3]`，三个独立单行 frame 被当作
两个事件交付。原因是 materializer 重启后仍从 segment 1、generation 0 分配，而输出恢复
有意保留同一 producer epoch；新帧复用了已持久帧的 source lineage identity。相邻身份被
日志重组器合并；非相邻身份触发 `frame source identity reappears after another frame`。

新增固定 checkpoint、原 producer epoch、历史文件与新 live 帧混读的确定性 Rust 回归，
修复前触发上述 DataCorruption，修复后保持所有独立帧边界，并覆盖再次 checkpoint/重启。
原生 materializer 现在以逻辑下一行位置初始化 generation：每个非空 segment 至少占一行，
因此新 generation 超过已持久前缀使用的 generation；空批次不创建/封存原生 segment。
物理 descriptor 与 Catalog lineage 共用这一身份，没有只修改日志分组来掩盖冲突。

底层分区验证重新打开时初始 generation 一致；rollover 使用 checked arithmetic，耗尽时
明确失败并保留原 segment。producer epoch、批次序号和幂等回执保持原契约。修复不尝试
自动还原之前已经产生歧义的持久 lineage，也不改写用户数据目录。

## 验证状态

Python 回归另发现 detached CLI 停服竞争：进程退出后，一次清理锁获取失败被直接判为出现
替代进程。状态探测也会短暂持有同一个排他锁，不能据此推断进程身份变化。现在最多额外等待
一秒获取清理锁，等待时检查 PID/nonce，获得锁后再执行原身份与 clean outcome 校验。
真正替代进程立即拒绝清理；锁持续忙则明确报锁未释放，不删除状态、不发送额外信号。
54 项生命周期测试通过，包含真实 detached 启停、确定性暂占锁、锁超时及替代进程保护。

- Rust workspace 最终 1781 passed、10 ignored；Clippy 全目标/全 features 通过。
- 仓库默认 `pytest -q` 最终 862 passed、1 skipped，包含主包与独立客户端。
- 最新服务/扩展上的真实矩阵通过：Parquet 三段串联写三类持久结果、单行传输仍还原完整帧、
  无键/时间字段的普通生产者、同一内核跨服务重启、丢回执重试及原 frame `[1, 2, 3]` 游标。
- Black 全仓 99 文件、Ruff、Rust fmt、diff 检查通过；根 mypy 16 文件及新增/修改范围通过。
  扩展 `mypy .` 仍为 976 errors / 12 files / 98 checked，F29 不关闭，未新增忽略规则。
- 构建并安装最终扩展；maturin 的既有缺少 patchelf/rpath 警告仍在，本机导入及全量回归通过。
  本批没有新增 Windows/macOS 实机或主包隔离发行证据，F28 保持开放。

修复前失败日志、确定性回归、最终验证及源码哈希见
[验收证据](perf_reports/evidence/2026-09-22-engine-chain/summary.json)。

旧 Session/Pipeline、主包订阅与查询、KV 状态因子适配、Gateway/Legacy/shadow 退出、
完整发行/平台矩阵和全仓类型门禁仍需完成。F07/F08/F11/F13/F14/F15/F22/F28/F29 保持开放。
