# 配置单主干：嵌套未知字段拒绝

日期：2026-09-23。原审计仍为 33/34；F28 跨平台实机验收按用户要求延期。

顶层配置已拒绝未知字段，但几个嵌套配置仍使用 Serde 的默认行为：
`catalog.runtime_snapshot_pins`、`query.resources`、`query.canary`、`log` 的 TOML/JSON
overlay 会忽略未识别键；直接反序列化完整 `ZippyConfig` 时，Flight 等子配置也会忽略。
例如写入 `[query.resources] old_memory_limit = 1` 会成功启动，实际仍使用默认内存限额。
这与“旧配置不再兼容、配置错误必须显式失败”的主干契约矛盾。

现在所有配置子结构与对应的部分配置解析结构都拒绝未知字段。缺省字段继续按既有默认值
解析；已删除或拼错的键在 TOML、JSON overlay 和完整配置反序列化中都返回错误。
定向测试先证实旧代码静默接受，再验证新行为。旧目录拒绝路径也经源码和现有测试复核：
无版本且非空的数据根目录被拒绝，旧 Master registry 文件被拒绝且不读取或改写。

本改动不迁移旧配置或数据。`cargo test --workspace --quiet` 完整通过；
`cargo clippy --workspace --all-targets --all-features -- -D warnings`、
`cargo fmt --all --check` 与 `git diff --check` 通过。Windows/macOS 实机验收仍暂缓。
用当前源码重建的 `target/debug/zippy-server` 运行空目录原生矩阵：49 项布尔检查
全部通过、0 项失败；日志在 `/tmp/zippy-strict-config-native-matrix.log`。

客户端配置复核发现 `resolve_control_endpoint()` 仍会优先读取旧
`ZIPPY_MASTER_URI` / `ZIPPY_MASTER_ENDPOINT`，即使已设置现行
`ZIPPY_CONTROL_ENDPOINT`。现已删除该优先级分支，任何旧变量存在时均明确报错；
省略 URI 时只读取 `ZIPPY_CONTROL_ENDPOINT`。故障先由测试复现，再验证修复：
端点路由 43 项、仓库 Python 测试 554 项及独立客户端隔离发行 230 项通过，
后者覆盖率 85.21%。Black、Ruff、mypy 与 diff 检查均通过。

继续对照 Rust 与 Python 的控制端点解析：Rust 拒绝 `tcp://host:port/path`，Python 曾把路径、
用户信息、查询串和片段静默丢弃，仍尝试连接。现已统一为仅接受主机和端口，并确保拒绝用户信息时
错误文本不会回显凭据。四种无效 URI 在修复前均由测试复现；修复后仓库 Python 560 项、
独立客户端隔离发行 236 项通过，后者覆盖率 85.17%；Black、Ruff、mypy 和 diff 检查通过。
Rust 控制端点也增加同组拒绝测试，并移除无效 URI 错误中的原始输入，避免回显密码。
`cargo test --workspace --quiet` 在允许本地 listener 的环境中完整通过；
`cargo clippy --workspace --all-targets --all-features -- -D warnings` 和
`cargo fmt --all --check` 通过。沙箱内同一 Rust 测试曾因禁止创建本地 TCP listener
收到 `Operation not permitted`，随后在允许 loopback 的环境中复验通过。
随后发现远端 `zippy://host:port/<profile>` 在 Rust 与 Python 中都丢弃 profile，
任意名称实际连接同一端点。现仅允许省略路径或显式 `/default`；两端的反向测试均先失败、
修复后通过，避免看似指定了另一配置却仍连接默认服务。最终仓库 Python 563 项、
独立客户端隔离发行 239 项通过，覆盖率 85.18%；Rust workspace tests、全目标全 feature Clippy、
Rust/Python 格式检查、Ruff 和 mypy 均通过。F28 Windows/macOS 实机验收继续暂缓。
最后从当前源码分别构建主包和客户端 sdist，在仓库外无 `.git` 的解包目录构建 wheel，
独立安装后由安装版 CLI 启动服务并重跑原生恢复矩阵：49 项布尔检查全部通过，
客户端隔离测试 239 项通过。发行验收日志保存在
`/tmp/zippy-profile-full-dist.log`，本地源码服务端矩阵日志在
`/tmp/zippy-profile-native-matrix.log`。两次矩阵均使用 Arrow 23.0.1 的本机 Linux 环境；
它们不替代延期的 Windows/macOS 实机验收。
补充平台分支源码复核：Rust 在非 Unix 平台拒绝 Unix socket 路径，Python 曾仍生成 Unix 端点。
现 Python 明确拒绝 `unix://`、`file://` 和路径形式，默认逻辑 URI 仍解析到 TCP；
通过本机模拟分支的定向测试验证，不能据此宣称已完成 F28 的 Windows 实机验收。
修复后仓库 Python 568 项、独立客户端隔离发行 244 项通过，后者覆盖率 85.25%；
Black、Ruff、mypy 与 diff 检查通过。此批没有改动 Rust 代码，沿用上文的 Rust 全量验收。
后续非 Unix 分支复核又发现 Rust 对未知本地逻辑名及裸主机端点静默回退默认地址；
Python 对 `zippy://default/other` 也忽略额外路径。现两端只把空名或 `default` 指向
默认 TCP，其他本地名称明确失败，裸 `host:port` 使用显式地址而非默认地址。
这由可在 Linux 运行的 Rust 纯逻辑测试及 Python 模拟分支测试覆盖，仍不替代 F28 实机验收。
本批最终 `cargo test --workspace --quiet`、全目标全 feature Clippy、Rustfmt 通过；
仓库 Python 571 项、独立客户端隔离发行 247 项通过，后者覆盖率 85.34%；
Black、Ruff、mypy 和 diff 检查通过。
