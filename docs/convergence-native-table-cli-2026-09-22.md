# 第三十五批：表管理 CLI 转入原生目录

日期：2026-09-22。推进 F08/F14/F22/F29，累计仍为 **25/34 已关闭，9 项开放**。

删除重复的 cli_stream.py、stream 命令注册及表命令的旧 Master/ops 依赖。table ls/info/schema
使用共享 Client 和原生 Flight；namespace 参数贯穿元数据读取，Client.list_tables 增加精确
namespace 过滤。列表不再显示旧 buffer/frame 容量或用本机段数量猜测行数。

table info 在固定快照内读取 schema 和原生查询元数据，避免两次独立查询遇到 schema/drop/
同名替换时拼出矛盾描述；零行查询只取元数据，不读取整张表。Arrow metadata 的任意字节采用
显式 base64 编码，不以 UTF-8 容错解码丢失信息。

drop 改用不可变 UUID、预期 snapshot version 和稳定 request UUID。确认取消不连接服务；
原生版本冲突直接失败，相同请求重试保持幂等。删除旧 keep-persisted、Master token 和
control-endpoint 隐藏别名，不再从旧 registry 解析本机 Parquet 路径或手工删文件。

原先 3 个 stream 和 5 个旧 table 集成测试迁出。14 个新契约测试覆盖 namespace、连接关闭、
schema 的时区/可空性/非 UTF-8 metadata、确认取消、身份重试、旧参数拒绝和权限错误传播。
真实原生矩阵通过子进程 CLI，跨四代服务检查 append/frame/KV 的元数据与 schema；同时
实际验证错误版本不删除数据、正确删除及同一请求重试。旧目录字段的断言随旧契约退出。

新 CLI 文档见[使用说明](native-table-cli.md)。根 mypy 门禁从 cli_stream 改为 cli_table，
新实现没有保留协议分支或通过 ignore 回避类型约束。

隔离发行第一次完整验收遇到后台 checkpoint 与 schema 变更竞争，服务按设计返回
`RESOURCE_EXHAUSTED` 且 retryable=true。原生 SDK 的管理操作现在最多发送三次，只重试这一
明确可重试的背压错误；退避为 50/100 ms，所有尝试共享单调时钟总预算，且复用完全相同的
序列化请求、请求 ID、身份和预期版本。非重试错误、版本冲突、次数或时间耗尽直接保留原错误。
五个确定性用例覆盖暂时竞争、持续竞争、不可重试、冲突及不足退避时间，不用修改共享 time
模块的方式影响其它测试线程。服务端锁和错误契约没有改变。

## 最终验证

- 全仓 pytest：878 passed、1 skipped；CLI 定向测试 14 passed，覆盖率 91.11%。
- 客户端完整测试与仓库外 Arrow 23.0.1 纯 wheel 测试各 208 passed，覆盖率均为 85.19%。
- 主包从仓库外源码归档完成编译和安装。首次安装后矩阵在上述可重试背压处失败，原日志保留；
  修复后重建客户端 sdist/wheel 并替换该隔离环境中的客户端，再次运行安装后 CLI 服务矩阵通过。
  主包源码没有在两次运行之间改变，未通过源码路径覆盖已安装模块。
- 两次原生验收均含三类表的实际 CLI 操作与重启；最终安装后矩阵保留 frame/KV、回调 ACK、
  计算、固定快照与恢复断言。CLI 删除冲突和相同请求重试直接访问真实服务。
- 全仓 Black（101 文件）、Ruff、Rust fmt、diff 通过；根 mypy 17 文件、本批 4 文件、
  客户端严格配置 27 文件通过；全仓类型检查仍为 958 errors / 9 files / 100 checked。
- Rust 实现未改，不重复 workspace/Clippy；本批未重新执行 Arrow 25 或跨平台实机矩阵。

验证结果与源码哈希见[验收证据](perf_reports/evidence/2026-09-22-native-table-cli/summary.json)。
WebUI、主包 Table/Session/Pipeline 和旧数据权威仍未退出，整体收敛项目继续开放。
