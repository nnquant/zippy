# 第五十二批：共享内存描述符只接受完整当前契约

日期：2026-09-22。承接[第51批](convergence-materializer-cleanup-2026-09-22.md)。
累计 **32/34**，F08/F28 仍开放。

## 修复解析与读取的契约矛盾

旧 envelope 解析允许省略 payload_version_offset，但原生 reader 和固定快照读取已要求该字段。
这使相同描述符在一个入口被接受、在另一入口失败；底层一致性读取还保留无版本直接读取分支。

现在 envelope 与 ActiveSegmentDescriptor 都使用必填 usize；缺失、null、错误类型或错误偏移
在解析时拒绝。一致性读取始终检查版本。SegmentControlSnapshot 的 payload_version 也改为
必有的 u64。固定快照钉住某个版本、普通实时读取观察当前版本的差异属于现行语义，继续保留。
当前合法描述符的序列化内容和物理布局没有变化，不新增迁移入口。

原“缺字段仍接受”测试改为缺失/null/错误类型/错误偏移全部拒绝，并验证内存中篡改的描述符
在附着时被拒绝。保留奇数版本拒绝、固定快照版本变化失效、descriptor generation/writer epoch
身份校验、跨进程可见性和 frame 原子写入检查。

同时删除 DEFAULT_MAX_PARALLELISM/MAX_PARALLELISM Rust 常量别名；只有 CPU budget 命名。
旧 ZIPPY_MAX_PARALLELISM 环境变量的明确拒绝测试继续保留。

## 验证与证据

core、segment-store、ingest、server 四个直接受影响 crate **476 次通过记录、5 个原有手动
性能忽略项**。全 workspace/all-targets/all-features Clippy -D warnings、Rustfmt、diff 检查通过。
重建服务端和扩展后 Python 全量 **543 项通过**，原生三类表、固定快照、实时订阅、计算与
重启恢复矩阵通过。mypy 91 文件、客户端自身配置 27 文件、Ruff 和 Black 92 文件通过。
全 Rust workspace 最近一次完整运行见第51批；本批执行上述四个 crate 与全目标编译检查。

[结构化证据](perf_reports/evidence/2026-09-22-descriptor-contract/summary.json)保存源码、
二进制和验证日志摘要。两个测试迁移，没有删除当前读取保护用例。

F08 还需删除底层 segment-store 旧持久 worker、只读 actor 兼容入口及无用验证包装。
F28 尚缺 Windows/macOS 实机与当前发行矩阵；Master 当前仍直接读取 /dev/urandom 并在失败时
回退到非安全随机 token，这一跨平台问题已确认，尚未在本批修改。
未提交、推送、部署或迁移旧数据。
