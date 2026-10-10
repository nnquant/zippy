# 阶段 1.7 证据索引

仓库基线为 `e1e67b569d1f5ffb48f6a093c242f271608a8265`，分支为
`feature/public-path-latency-bench`。实现未提交；未切换分支、commit 或 push。
完整报告见 `../../2026-10-08-public-path-e2e-baseline.md` 的“可见延迟随时间增长”。

## 数据身份

- `control-a-before`：e1 生产代码，无背景，至少三个完整分钟。
- `control-b-before`：e1 生产代码，背景 100,000 行/秒，至少三个完整分钟；临时
  Python 边界/资源诊断仅在此运行启用。`diagnostic-metadata.json` 记录脚本身份。
- `history-refresh-before.log`：生产文件与 e1 一致时，新增工作量回归编译成功后
  1 passed / 1 failed；对应 SHA 与测试补丁分别在 `history-refresh-before-metadata.json`
  和 `history-regression-test.patch`。
- `baseline-after`、`paired-candidate`、`soak-after`：补齐宽表计费之前的候选 v1。
  这些原名没有覆盖或重命名；`soak-candidate-v1-trends.json`、
  `soak-candidate-v1.md` 明确归属 v1，不能当作最终版本的数据。
- `paired-e1`：同期重新构建旧生产的短对照。构建/恢复记录在
  `paired-build-metadata.json`；没有切换 Git 分支。
- `soak-final`、`control-a-final`、`baseline-final`：补齐每个 live span 的布局与
  SHM 定位字符串计费后的最终候选。精确命令/退出码在 `final-benchmark-commands.json`；
  源码和 release server/extension 身份在 `final-implementation-metadata.json`。
- `final-validation.json`：最终 11 条验证/构建命令的 argv、环境、时间、退出码和日志 SHA；
  全部退出 0。开发期的编译错误及子代理环境 EPERM 日志单独保留，没有冒充旧版回归失败。

## 统计与验收口径

新增分钟趋势以首条计量的实际 `sent_ns` 为原点，按 60 秒左闭右开桶聚合；不足一分钟
的尾桶展示但不参与 OLS 或首末比例。趋势同时要求 p50 斜率不大于 20µs/分钟、末/首
不大于 1.15，并有完整样本。任务另要求 soak 至少 10 个、无背景对照至少 5 个完整分钟。

基准原 C/D 门限、分钟趋势门限、相对 e1 四格不退化要求分别判断。主脚本退出 1 不等于
数据不完整，也不能被功能测试通过豁免。最终四格仍有 visible 尾部分位高于 e1 的格，
不能宣称全部性能验收通过。完整数值及判定见最终 comparison 和验收 JSON。

原始逐行数据位于 `/tmp/zippy-perf-raw/2026-10-10-latency-growth/`，不放入仓库。
comparison 记录输入路径、SHA、大小及元信息，并从 raw 重算核对原 summary。
历史测量的 dirty SHA 保持原值，归档提交标签不冒充测量时的 clean checkout。

## 校验

本目录每个证据文件最多 300,000 字节。`SHA256SUMS` 包含本目录除其自身外的文件：

```bash
cd /home/jiangda/develop/zippy/docs/perf_reports/evidence/2026-10-10-latency-growth
sha256sum -c SHA256SUMS
```

大 raw 的身份由各 comparison 单独记录；本目录的 SHA 清单不意味着 raw 已被复制进仓库。
原有历史 evidence 目录和报告历史前缀未改写。

- `supervisor-ab-summary.json`：监督方 ABBA 对照（e1/final 各两轮，每轮 3 分钟）和 10 分钟浸泡的逐分钟 visible 分位数与斜率；原始数据在 `/tmp/zippy-perf-raw/2026-10-10-latency-growth/supervisor-ab/`。
