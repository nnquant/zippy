# 单主干收敛第四批：Persisted 确认与文档归档

后续进展见[第五批记录](convergence-frame-2026-09-22.md)；以下保留第四批时点结果。

日期：2026-09-22。原 34 项审计累计关闭 **20 项**；本批关闭 **F17、F33**。
剩余 **14 项**：F07–F16、F22、F26、F28、F29。项目尚未完成单一主干切换。
上一批的范围与测量保留在[第三批记录](convergence-implementation-2026-09-22.md)。

## F17：真正的 Persisted 请求确认

- Flight/客户端接受显式 `persisted`；服务端只有实际持有 checkpoint sink 才公布
  `ingest.append.persisted.v1`。能力不足在写入前拒绝，不降低 durability。
- 原生写入沿统一 WAL 主干，等待 checkpoint 的 Parquet/manifest 覆盖目标水位，然后将
  包含 snapshot 版本的回执同步归档，最后才返回成功。空批次不伪造行 offset 或额外数据文件。
- 持续持有同一写入操作与 pending-ACK 配额，阻止中途 producer 切换；请求取消不取消
  已接受的服务端任务，shutdown 等待排空。后台 checkpoint 与前台 Persisted 共用串行入口。
- 已确认的 Persisted 回执按原请求身份、原 snapshot 版本保存。热缓存逐出或服务重启后，
  精确重试仍返回原回执，不改成当前更高 snapshot。manifest 未覆盖时拒绝成功并保留 WAL 回执。
- Rust 与 Python 均要求 Persisted snapshot 版本为正数；只有 WAL 的回执不能满足 Persisted 请求。

## 新发现 F37：跨 writer checkpoint 的错误约束

真实 Python 验收暴露：先后打开两个 writer 后，checkpoint 包含多个 writer epoch 的 segment，
旧 `PersistRequest` 从来源 lineage 推导提交 epoch，并拒绝混合来源，导致持久化和停服失败。

现在请求显式传入当前提交 authority epoch，TableActor 继续校验 fencing；历史 segment 的
来源 epoch 保持原值，只要求不超过提交 authority。不会伪改来源信息或放松旧 writer 的提交权限。
存储回归验证混合历史 epoch 可提交、未来来源 epoch 被拒绝；真实跨语言测试覆盖 writer 交接、
Persisted 确认、正常停服和重启查询。F37 是审计后发现的新问题，不计入原 34 项。

## F33：当前事实与历史记录分开维护

- 根 ROADMAP 只给出获批的收敛顺序，旧内容归档；当前能力表作为状态入口。
- ADR 0004/0005/0006 保留接受时的阶段结论，显式链接当前能力及 ADR 0007 的新契约。
- 旧审计、Windows 设计/验收说明增加日期边界和当前入口，不重写历史测量。
- 原重复客户端指南改为指向唯一 README，删除仍宣称模块级旧 API 兼容的示例；同步 Flight
  与 ingest 说明中的过期 Memory/Persisted 陈述，修正能力表中已完成在线建表仍被列为缺口的问题。

## 验证与证据

- Rust 全 workspace：1711 passed、10 ignored；随后增加正 snapshot 版本校验，类型库单跑
  100 passed，其中包括新增用例。两个结果分开记载，未将局部复验冒充第二次全 workspace。
- Rust Clippy：全 workspace/all-targets/all-features 严格检查通过；fmt 与 diff whitespace 检查通过。
- Python 客户端完整套件：92 passed，覆盖率 81.39%；mypy 覆盖客户端源码、测试和真实验收脚本，
  共 25 文件通过；本轮 Python 文件 Black/Ruff 通过。
- 定向 Flight 测试先得到 26 passed，但因只运行单文件未满足全包覆盖率门槛；随后完整套件
  达到 80% 门槛。未调低门槛或排除模块。
- `scripts/check_native_client.py`：空目录 → 创建 market namespace/append 表 → writer 1 WAL=42 →
  writer 2 Persisted=43 → 查询 → 停服 → 重启 → 幂等创建重试 → 恢复 [42, 43]，Arrow 23.0.1。
- 首次跨 writer 失败保留在 `/tmp/zippy-persisted-native-client.log`；修复后的成功结果在
  `/tmp/zippy-persisted-native-client2.log`。结构化证据与日志哈希见
  [summary.json](perf_reports/evidence/2026-09-22-persisted/summary.json)。

本批未重跑主 Python 包全套或全仓 mypy；上一批全仓 mypy 的 982 errors/11 files 仍是待处理
基线，不能据此声称全项目门禁已通过。未提交、推送、部署或操作生产目录。

## 下一步

优先补齐 frame/key-value 原生 materializer、删除、持久化恢复及快照游标，继而接入 Flight
订阅和本地计算输出；补齐 alter/drop 后再删除旧数据权威、在线桥接及重复查询实现。
发行边界、平台矩阵和全仓类型检查仍须验收，旧有效能力继续保留。
