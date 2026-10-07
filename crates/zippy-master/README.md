# zippy-master

2026-09-22 更新。此 crate 只承载原生 Server 的控制监听器、配置发现和进程租约。
表、schema、写入、持久文件、订阅和消费位点由 SQLite/native storage/Flight 负责。

控制协议只接受 RegisterProcess、Heartbeat、UnregisterProcess、GetConfig 和 Watch；
Watch 只接受 Process/Shutdown。旧表与 source/engine 注册、文件发布、段描述符与读者租约请求
已经删除，不能通过控制监听器建立第二份表目录或修改持久文件。

ProcessRuntimeRegistry 与进程 lease 共用一把锁和启动身份，保留 CAS revision、状态转换、
ABA 防护、停服 admission 与进程过期回收。没有旧表/source/engine/connection 镜像。
请求大小、绝对读取期限、连接限额和有界停服继续有效。

Master 持久快照、旧 Catalog/query adapter、compaction、shadow 和迁移功能均已删除。
托管目录出现 `catalog/legacy-master-registry.json` 时明确拒绝且保留原字节，不导入或改写。
旧 `[table]`、`master.token`、`ZIPPY_TABLE*` 和 `ZIPPY_MASTER_TOKEN` 配置明确拒绝；
Flight 独立的读、写和管理员凭据不受影响。

原生功能与验收状态见[当前能力](../../docs/current-capabilities-2026-09-21.md)，
运行顺序见[单节点运行时](../../docs/single-node-server-runtime.md)。
