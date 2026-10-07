# 第三十四批：客户端统一 Flight，删除 Gateway 数据路径

日期：2026-09-22。推进 F13/F14/F22/F28/F29，原审计仍为 **25/34 已关闭、9 项开放**。

独立客户端的数据面现在只有 Flight。删除 GatewayTransport、GatewayWriter、独立帧协议、
Gateway endpoint/认证/协议版本解析、两种 Gateway transport、Gateway SQL AST 解释器及
按旧 Master stream descriptor 等待数据的路径。缺少 Flight endpoint 时在连接阶段明确失败。
所有查询、写入、订阅及快照共用原生 Catalog 和 Flight；本地／远端 transport 仅约束 endpoint
位置，不选择另一套实现。控制连接仍用于发现和可选进程租约，其剩余旧目录方法另行退出。

能力信息来自 Flight 握手与 capability RPC；旧 Gateway 配置和旧 subscribe_resume_cursor
广告不再影响客户端能力。支持 WAL/Persisted 的 listener 无需额外公布 Memory 才能取得 writer。
writer schema 查询的权限及目录错误直接传播，不再吞掉错误后尝试无 schema 写入。
Table.collect/profile 的无效 stream 参数也已删除；传输固定为 Flight 批次流。

## 迁移时发现并修复的真实错误

新增原生回归使用 scalar writer 写入 `[3, 1, 2]`，执行排序、slice(1,1)、重命名和删列，
原 SQL 实际返回 `[3]`，预期为 `[2]`。共享编译器把 ORDER BY 放在子查询、LIMIT/OFFSET
放在外层，未将行选择与排序绑定在同一查询层。head/slice/tail 现在生成同层排序与截取；
升序／降序、有限与开放 slice、重命名后截取、head 和 tail 都在真实 DataFusion 及每次
服务重启后校验具体结果。错误重现日志保留在验收证据中。

删除的 Gateway 集成测试有明确接替：惰性查询与缓冲写入由原生服务矩阵执行；两个 Client
独立生命周期由真实 Flight peer 验收；固定快照克隆和多表 scope 由既有原生快照矩阵验收。
仅覆盖已删除帧编码、Gateway 认证与 Gateway-only 能力的测试随协议退出。原独立客户端
子进程 smoke 移到原生服务，仍验证真实写入／过滤／投影及不导入 zippy 主包。

发行验证增加 Python -I 下的显式辅助脚本目录，修复隔离运行时找不到验收模块的问题。
完整发行验证以已安装 CLI 启动服务，不再调用仓库 target/debug 二进制；指定 Arrow 版本
同时用于纯客户端与完整包环境。SDK 仍从安装的 wheel 导入，没有把仓库 Python 源码加入路径。

## 验证与边界

- 最终全仓 pytest：867 passed、1 skipped。相比第 33 批少 9 项参数化用例；原 Gateway
  专属协议用例随删除退出，查询／写入／快照／独立生命周期的有效断言按上述映射保留。
- 客户端完整测试 203 passed，覆盖率 85.15%；SQL 修复后的仓库外 Arrow 23.0.1／25.0.1
  纯客户端 sdist/wheel 构建安装各 203 passed，覆盖率均为 85.14%，不存在 Gateway 模块和旧协议。
- 主包从 Git 仓库外的源码归档重建 CPython 3.11 wheel；Arrow 23.0.1 干净环境完成安装、
  CLI 启动和完整恢复矩阵。同一 wheel 在另一 Arrow 25.0.1 环境重复原生矩阵通过。
  两次均实际使用已安装 CLI 服务，无仓库二进制参与；三类表、订阅 ACK、固定快照、计算和
  有限回放的原断言全部通过。
- 全仓 Black（100 文件）、Ruff、Rust fmt、diff 通过；根 mypy 17 文件、本批 30 文件、
  客户端自身严格配置 27 文件通过。扩展全仓仍为 961 errors / 10 files / 99 checked，
  没有通过新增 ignore 隐藏类型债务。
- 本批未修改 Rust，未重复此前 workspace/Clippy；隔离发行确实从归档重新编译了主包扩展。

最终验证数据见[验收证据](perf_reports/evidence/2026-09-22-flight-only-client/summary.json)。
本批不关闭主包旧 Table/Session/Pipeline、服务端 Gateway adapter、Legacy/shadow 等整体项目。
Windows/macOS 实机矩阵和全仓类型错误仍需处理；单一客户端数据面不等于整个服务器已切换。
