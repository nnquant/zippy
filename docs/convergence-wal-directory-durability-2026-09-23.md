# WAL 首次建表目录的持久性复核

日期：2026-09-23。跨平台实机验收 F28 按用户要求延期；原审计关闭状态仍为 33/34。

`persisted` 请求确认沿原生写入、WAL `sync_data`、Parquet/Catalog manifest 提交和
SQLite 回执归档返回。复核首次建表发现：新表 WAL 目录创建后，原路径只同步表目录及其
文件，未同步 `wal` 父目录中的表目录项。断电后即使文件自身已同步，这个新目录项仍可能
丢失，影响首次写入的 WAL 恢复承诺。

现在 WAL segment 创建在写入文件前同步 `wal` 目录；回执数据库初始化也在返回前同步
其父目录，再同步表目录。受管数据根目录创建 `wal` 子目录时已有父目录同步。
这些操作只影响新版本原生持久链路，不读取或迁移旧格式目录。非 Unix 平台仍按现有
平台实现处理目录同步；Windows/macOS 的实机断电与恢复验收未执行，不计为通过。

验证：`cargo test -p zippy-ingest` 为 56 passed、2 ignored；
`PYO3_PYTHON=.venv/bin/python cargo test --workspace --quiet` 完整通过；
`cargo clippy -p zippy-ingest --all-targets --all-features -- -D warnings`、
`cargo fmt --all --check` 和 `git diff --check` 通过。现有 WAL 恢复、回执归档重开
与取消后 Persisted 完成用例均在该定向套件中运行。

额外尝试 `cargo test --workspace --all-targets --all-features` 时，PyO3 的
`extension-module` 特性使 `zippy-python` 的 Rust 测试可执行文件缺少 Python 链接符号；
这是测试特性组合的链接失败，未进入测试断言。项目 CI 的完整测试命令是
`cargo test --workspace`，`--all-features` 用于 Clippy 检查。
