# 原生目录 WebUI

`zippy webui --uri <control-endpoint>` 启动独立 FastAPI/Uvicorn 进程。控制端点只用于发现
Flight，表列表、详情和预览使用共享 `zippy-client.Client`。认证使用客户端的 Flight 环境配置；
读取表只需 read 权限。服务不会自动接管 WebUI 的进程生命周期。

Dashboard 的 Catalog 状态表示本次原生目录查询成功或失败，不等于全部运行组件健康。
Processes 来自本机 PM，日志来自 `--log-dir`，两者与远端 Catalog 的状态分别显示。旧
registry 快照、pgrep 进程推断、Gateway 配置/metrics、reader/sealed/persisted 计数已删除。
没有把未提供的原生健康指标填成零。

表的选择身份是 `(namespace, name)`；详情显示固定快照内的表 UUID、快照版本和 Arrow schema。
`table_type` 是 Catalog 对象类型（例如 TABLE），不是 append/frame/key-value 的业务种类。
详情使用零行查询提取快照元数据，不同步扫描每张表统计行数。每次请求结束关闭客户端和快照。

数据页默认 head，最多 1000 行。尾部预览必须选择排序列，支持多列与统一升/降序；多列优先级
按勾选顺序排列，并显示在工具栏下方。要固定排序键相同的行顺序，请将唯一列加入排序键。
未排序的 head 不承诺最新或稳定顺序。额外读取一行判断是否截断；恰好等于上限不会误报。
frame 预览是行预览，可能截断帧，不代替原子帧订阅。

预览结果自身携带 schema 和 snapshot metadata，因此列类型不会借用另一次请求的旧 schema。
纳秒时间戳按 Arrow 精确字符串显示；超出 JavaScript 安全整数范围的值以十进制字符串返回，
显示及 JSON 导出均保留原值。非有限浮点数以 `inf`、`-inf`、`nan` 字符串表示，二进制为 hex。
这些是 JSON 展示编码；需要原始 Arrow 类型请使用 Client 查询。

HTTP 接口：

- `GET /api/dashboard`：`catalog`、`pm`、`tables`、`tasks`、`totals`、`log_tail`。
- `GET /api/table/{name}?namespace=market`：`ok`、`table`，失败返回 `error`。
- `GET /api/table/{name}/data?namespace=market&mode=tail&limit=100&order_by=dt&order_by=id`：
  显式排序预览，可加 `descending=true`；`namespace` 默认 `default`。
- 无效 mode 或超出 1–1000 的 limit 返回 422；查询失败返回 `ok=false`，界面展示错误。

切表、改排序、改行数和手动刷新会取消旧浏览器请求，已取消的响应不能覆盖当前表。
自动刷新在上次预览完成后等待五秒，避免持续打断慢查询。数据搜索、列显隐、复制、CSV/JSON
导出、换行与全屏保留。

可复验命令：

```bash
uv run --no-sync pytest pytests/test_native_webui.py pytests/test_native_table_cli.py pytests/test_python_cli.py
npm --prefix app/zippy-console run build
uv run --no-sync python scripts/check_frontend_bundle.py
uv run --no-sync python scripts/check_native_client.py
node scripts/check_webui_browser.cjs /path/to/playwright-core /path/to/chrome /tmp/zippy-webui
```

原生矩阵通过真实 HTTP 验证 append/frame/KV、跨命名空间同名表、精度与四代服务重启。
浏览器脚本使用受控 HTTP 响应验证取消竞态和导出，产出宽屏/窄屏截图；它不代替原生数据验收。
