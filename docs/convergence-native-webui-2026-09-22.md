# 第三十六批：WebUI 转入原生目录

日期：2026-09-22。推进 F08/F14/F22/F29，累计仍为 **25/34 已关闭，9 项开放**。

WebUI 的表列表、详情和数据预览全部使用共享 Client 和原生 Flight/Catalog。删除旧 registry
快照、pgrep 进程推断、Gateway 配置/metrics、逐表 ops 查询、旧描述符行数和读者/段/文件统计。
Dashboard 只做一次目录列表查询；Catalog 可访问性与本机 PM 状态分别显示，不冒充全服务健康。

CLI 与 WebUI 共用新增的 native_inspection：固定快照内读取表身份、逻辑 Arrow schema 和
零行查询的 snapshot metadata。预览也固定在单一快照内，其返回的 schema 用于当前网格列类型。
请求完成或失败均关闭客户端、派生查询和快照租约，没有保留旧数据读取分支。

前端选择身份改为 namespace/name 二元组，解决跨命名空间同名表混淆。默认 head；tail 必须
显式指定排序列，支持多列及升降序，并显示排序优先级。多取一行精确判断截断，避免恰好等于
上限时误报。纳秒时间戳、大整数及非有限浮点数转换为明确的 JSON 展示格式；浏览器不会再把
大整数字符串转回不精确的 Number。列显隐、复制、搜索、CSV/JSON 导出、换行和全屏保留。

切表与修改查询参数会取消旧浏览器请求；迟到响应不能覆盖新表。自动刷新在当前预览结束后
等待五秒。视觉检查发现窄屏工具栏裁切，已修正分组换行并加入浏览器边界断言。

旧的三个表预览/详情测试、两个 Gateway/旧表健康测试由原生契约替代；静态布局断言同步新
状态模型。新增 20 个测试覆盖命名空间、固定元数据、资源释放、精确截断、显式排序、错误与
数值精度。真实 HTTP 验收集成到四代服务恢复矩阵，三类表及同名异命名空间均实际读写。

## 验证结果

- 全仓 pytest：893 passed、1 skipped。
- WebUI/CLI 定向：70 passed；WebUI 与共享检查模块合计覆盖率 84.70%，共享模块 100%。
- 仓库二进制与仓库外 sdist/wheel 的安装后 CLI，均完成原生与 HTTP 四代服务恢复矩阵。
  发行环境独立客户端 208 passed，覆盖率 85.19%，Arrow 23.0.1。
- Chrome + 已安装 Playwright 验证实际打包界面：同名表选择、多列排序、tail 前置条件、迟到
  响应取消、结果 schema、精确整数显示及 JSON 导出。宽屏/窄屏截图已人工检查。
  浏览器使用受控 HTTP 响应验证竞态，真实数据与恢复由独立 HTTP 原生矩阵验收。
- 主包 wheel 中 native_inspection、WebUI 和全部静态资源逐字节匹配当前源码及构建产物。
- Black 全仓 104 文件、Ruff、Rust fmt、diff、前端 TypeScript/Vite 和资源一致性通过。
  配置内 mypy 18 文件与本批 4 文件通过；全仓 mypy 仍为 954 errors / 9 files / 103 checked。
- Rust 实现及客户端实现未改，不重复 workspace/Clippy；本批未重跑 Arrow 25 或跨平台实机。

初次定向检查发现新预览把 Arrow schema 属性误当方法调用，修复后完整复验通过；首次浏览器
脚本错误地按 tab 角色定位已有按钮，修正定位后通过，未以绕过交互的方式消除失败。初始日志
与最终结果分别记录在证据中。

用法与边界见[原生 WebUI](native-webui.md)，源码和产物哈希见
[验收证据](perf_reports/evidence/2026-09-22-native-webui/summary.json)。主包 Table/Session/Pipeline、
旧回调及服务端 Legacy/shadow 数据权威仍存在，本批没有关闭上述整项审计问题。
