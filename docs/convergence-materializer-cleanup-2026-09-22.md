# 第五十一批：物化与状态消费只保留原生路径

日期：2026-09-22。承接[第50批](convergence-source-runtime-retirement-2026-09-22.md)。
累计 **32/34**，F08/F28 保持开放。

## 删除旧物化策略

删除 KeyValueTableMaterializer、ShardedStreamTableMaterializer、ShardRouter/Config/Value、
旧快照句柄 KeyValueSnapshotHandle、KV changelog/快照描述符双别名、跨 shard 失败缓存。
对应两个 Criterion 基准退出；MD5 仍用于现行计算 golden hash，不删除这一独立用途。

StreamTableMaterializer 保留原生 append/frame 的真实共享内存写入、批次原子性、rollover、
writer epoch、live segment 与读者 pin。删除 descriptor publisher/forwarding、回退分支、
旧 replacement/in-place KV 写入及独立 retention 配置；回收只接受原生 manifest 的 durable prefix。
frame 与 rollover 测试改为检查当前真实描述符，原生持久前缀的 pin 保护测试保留。
旧 Source 专属延迟追踪代码及配置退出。性能工具与 CI 删除 forwarding profile，不保留别名。

## 原生 KV 状态语义

KeyValueState 仍是 Python NativeProcessor/客户端可用的增量状态消费器。测试改用现行
zippy_types::key_value::change_batch 编码：同批重复键的每条事件都进入原生日志，快照按键
合并。覆盖事件分片的原子提交、游标/版本、重复投递、缺口失效、内存预算、表身份隔离、
删除重插和混合 mutation 拒绝。真实服务的原子快照与重启恢复继续由原生矩阵验证。

LatestColumnarState 删除只服务旧物化器的快照行号索引和新槽位 API；现行内核仍保留稳定槽位、
删除复用、按键确定性排序和显式快照。排序测试直接检查增量与完整快照的键和值。

## 验证与证据

全 Rust workspace **1206 次通过记录、7 个原有手动性能忽略项**，含子进程输出记录。
Clippy 精简单元素性能测试后，zippy-perf 定向回归再次通过。
workspace/all-targets/all-features Clippy -D warnings、Rustfmt、diff 检查通过。
重建后 Python 全量 **543 项通过**；mypy 91 文件、客户端自身配置 27 文件，Ruff/Black 92 文件通过。
原生矩阵验证三类表、固定快照、实时订阅/ACK、计算、schema/drop、断连与重启恢复全部通过。

本批 Rust 净减 **3716 行**；36 个旧专属测试退役、5 个测试名称随现行语义迁移，
同名 frame/rollover/排序测试保留并改用当前数据路径。
[结构化证据](perf_reports/evidence/2026-09-22-materializer-cleanup/summary.json)与
[测试退役清单](perf_reports/evidence/2026-09-22-materializer-cleanup/test-retirement.json)
保存源码、二进制和验证日志摘要。

继续复查发现 segment-store 仍有只供旧 API/测试使用的持久 worker/队列，actor 仍有只读兼容
启动器，描述符仍允许缺少 payload_version_offset，core 仍导出旧并行度常量别名。
这些未完成项继续计入 F08，不因主业务已切换而提前关闭。F28 仍缺 Windows/macOS 实机与
当前发行矩阵；本机 Linux CPython 3.11/Arrow 23.0.1 仓库构建不能替代。未提交、推送或部署。
