# SQLite Catalog

2026-09-22 更新。SQLite 当前承接原生 append/frame/key-value 的建表、schema 变更、
删除、manifest 发布、查询快照和持久消费位点。显式部署仍需要 `catalog.backend="sqlite"`
与 native storage；默认 Legacy 权威的删除是剩余收敛工作，见
[当前能力](../../docs/current-capabilities-2026-09-21.md)。

## 唯一当前布局与请求格式

仅接受物理布局 **v11**。空目录初始化一次；v1–v10 和未来版本在启用 WAL 前拒绝，
不改写旧目录。布局版本与命令版本独立，当前命令及回执只使用 v1。
七张迁移表、V2 导入、V3 reconcile、V4 projection、对应 service 句柄和共享编解码均已删除。
没有在线升级链、导入窗口或影子同步。

初始化与重新打开校验规范化 `sqlite_schema` 指纹、布局 checksum、SQLite 完整性与外键。
Catalog 使用 WAL、`synchronous=FULL`、禁用自动 checkpoint，SQLite 连接由所属 worker 独占。
有界队列、命令内存预算和独立读取通道约束资源使用。

## 原生持久契约

- 名字只约束未删除的表；同名重建使用新 ID。历史 schema、文件、快照及回执仍绑定原 ID。
- 建表与 schema 变更校验共享的 Arrow schema 契约。schema IPC 有大小、字段数、递归深度、
  元数据和解码预算；逻辑指纹不受 IPC 对齐或元数据键顺序影响。
- manifest 提交在同一事务内推进快照、水位、Catalog revision 和幂等回执。
  文件来源保留精确 segment identity 与连续 offset lineage。append/frame 原生 compaction
  支持精确替换；KV compaction 仍明确拒绝。
- drop 封锁 writer epoch、发布空保留快照并退役可见文件；历史 pin 和 lease 继续保护文件。
  Server 协调 ingest、actor、materializer 与日志退出后才确认公开删除。
- 持久 ACK 校验表身份、schema、上一位点 CAS 与已持久水位。身份验证、组成员互斥和交付确认
  由 Server SubscriptionService/Flight 承担，不能仅凭 Catalog 位点写入宣称端到端消费成功。
- 回执重放与原始提交结果绑定；请求内容变化不会被当作同一次重试。

## 查询和物理文件边界

跨表快照在单次读取事务内捕获描述、schema、manifest 与文件 lineage，并校验选择器、
身份、schema 和可见性。默认最多 4,096 个选择器、64 MiB 响应、65,536 个文件和
262,144 条 lineage；精确限制以配置与源码为准。

`read_pinned_snapshot_at` 返回持有生命周期的能力对象。计划读取不含路径的 manifest 元数据，
扫描任务通过 `PinnedDataFile` 打开 reader；reader 与任务共享 pin/data-root 所有权。
托管文件沿稳定目录描述符逐级 `NOFOLLOW` 打开，再核对常规文件、尺寸、owner、link count
与权限；有界定位读取不改变顺序读取游标。

旧外部路径、legacy provenance、额外排序列、Import 快照及 reader 分支已删除。
共享位点只保留 Empty/Known，Catalog/WAL/Flight 拒绝旧 unknown 标签。布局只允许
managed/native 文件，原始与固定快照统一按 file_id 排序；每个文件必须有精确 offset lineage。

## 发布、清理与停服

`CatalogPersistence` 的共享发布许可与独占清理许可绑定实例和生命周期。取消等待不释放
已进入队列的提交所有权；清理必须持有独占权限并重新证明文件引用及物理身份。
历史文件清理还需满足 pin/lease 和安全回收状态机。

健康检查确认 writer/control-read/snapshot-read worker 存活；确认死亡是终止故障。
Server 按依赖顺序关闭请求与 ingest，再排空 storage/Catalog，共用绝对截止时间；
未退出的 worker 保留数据目录所有权。SQLite VM 步数超时不等于任意文件系统 I/O
都能被立即中断，目录锁和资源保留负责保护这一边界。

最新删除记录见[第42批](../../docs/convergence-native-file-contract-2026-09-22.md)。
