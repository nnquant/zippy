# Zippy Windows 可用性设计

> 日期：2026-05-02
>
> 目标：推进 Zippy 在 Windows 原生环境下的控制面和 live segment 跨进程链路可用性。
> 本文只定义设计边界和实施策略，不包含具体实现代码。

## 1. 背景

当前 Zippy 的主链路主要面向 Linux/WSL2：

- control plane 使用 Unix domain socket；
- master URI 默认解析到 `~/.zippy/control_endpoints/<name>/master.sock`；
- segment store 使用 Linux `mmap`、`posix_fallocate`、Unix fd 和 futex；
- Python CLI、`connect()`、`MasterClient`、subscriber/query 都默认假设 Unix socket；
- 部分测试也直接构造 `.sock` 路径或使用 `std::os::unix`。

这意味着 WSL2 能验证 Linux 后端，但不能证明 Windows 原生可用。Windows 可用性必须在真实
Windows 环境或 Windows CI 上验证。

## 2. 目标与非目标

### 2.1 本轮目标

本轮目标覆盖两个层级：

1. Windows 原生控制面可用。
   - `zippy.connect(uri="default")` 能连接 Windows master；
   - master/client/register/heartbeat/list/query descriptor 等控制面请求可用；
   - URI 仍保持用户友好，不让用户直接处理底层 socket/pipe/path。

2. Windows 原生 live segment 全链路可用。
   - writer 进程能创建并发布 active segment；
   - reader/subscriber/query 进程能 attach active segment；
   - rollover、sealed segment、reader lease、drop table 生命周期行为保持一致；
   - Python 层接口尽量不暴露平台差异。

### 2.2 非目标

- 不把 WSL2 视为 Windows 原生验证。
- 不在第一阶段追求 Windows 低延迟指标等同 Linux。
- 不重启旧 POSIX shm fallback。
- 不为 Windows 重新设计 Python 用户 API。
- 不承诺所有 OpenCTP vendor 动态库立即跨平台可用；插件侧可单独验证。

## 3. 设计原则

1. 用户接口稳定，平台差异下沉到底层。
   Python 用户继续使用 `zippy.connect()`、`zippy.read_table()`、`zippy.subscribe()`、
   `Session`、`Pipeline` 等接口。

2. 先抽象边界，再实现 Windows 后端。
   Linux 当前路径应作为一个明确 backend 保留，不为了 Windows 改坏 Linux 低延迟链路。

3. 控制面和数据面分层推进。
   control transport、segment mapping、segment notification 三个问题分开解决。

4. 错误要显式。
   在尚未支持的平台组合上，应返回清晰错误，而不是 import 失败、panic 或 obscure IO error。

5. 验证必须覆盖真实跨进程。
   Windows 可用性的最终验收不能只靠单进程单元测试。

## 4. 架构方案

### 4.1 平台抽象边界

新增或重构出三个平台边界：

| 抽象 | Linux 当前实现 | Windows 目标实现 | 职责 |
| --- | --- | --- | --- |
| `ControlTransport` | Unix domain socket | TCP loopback 或 named pipe | master/client 控制面请求响应 |
| `SegmentMapping` | file-backed mmap | Windows file mapping | 创建、打开、映射、释放共享 segment |
| `SegmentNotifier` | futex / spin wait | Windows event 或 condition primitive | writer commit 后唤醒 reader/subscriber |

这三个抽象不应该泄漏到 Python facade。Python 层最多接触 `uri`、table name、stream name 和
高层配置。

### 4.2 控制面 transport

控制面建议优先采用 TCP loopback 作为 Windows 第一版后端：

- 优点：Rust 标准库支持稳定，测试和 CI 简单，跨平台一致性好；
- 缺点：相比 named pipe，语义不是本机 IPC 专用，需要明确只绑定 `127.0.0.1`；
- 安全边界：默认只监听 loopback，不开放公网地址。

后续如果需要更强的本机 IPC 语义，可增加 named pipe backend，但不建议作为第一版阻塞项。

URI 解析建议演进为 endpoint descriptor：

```text
zippy://default
  Linux   -> unix://~/.zippy/control_endpoints/default/master.sock
  Windows -> tcp://127.0.0.1:<allocated-or-configured-port>
```

`~/.zippy/control_endpoints/default/` 在 Windows 上仍可作为 endpoint metadata 目录，但里面不再
必须存在 `master.sock`。可以保存 `endpoint.toml`，记录当前 master 的 transport、address、pid、
started_at 等信息。

### 4.3 Segment mapping

segment store 需要把当前 `ShmRegion` 拆成平台实现：

```text
SegmentMapping
  create(name, size) -> mapped region
  open(descriptor) -> mapped region
  ptr(), len()
  flush_or_noop()
  close()
```

Linux 后端继续使用当前 file-backed mmap；Windows 后端使用 Windows file mapping。descriptor 中
需要能表达平台无关的 segment identity：

- stream/table name；
- segment id；
- generation；
- row capacity；
- schema/layout version；
- mapping name 或 backing path；
- platform backend kind；
- committed row count metadata location。

为了降低第一版复杂度，Windows 可以优先使用 file-backed mapping，而不是 anonymous named mapping。
这样能更接近当前 `.mmap` 语义，也便于进程重启、调试和清理。

### 4.4 Segment notification

当前 Linux reader 可以用 spin + futex 等待 writer commit。Windows 第一版建议采用两层策略：

1. 低延迟订阅：短 spin loop 读取 committed row count；
2. 非 xfast 或空闲路径：等待 Windows event。

event identity 需要从 segment descriptor 派生，确保 reader 打开新 active segment 时能找到对应
notifier。rollover 时，publisher 更新 descriptor；reader 发现 segment end 后重新请求 descriptor
或通过后续低延迟 IPC v2 获取更新。

### 4.5 Python API 影响

Python 顶层 API 不增加平台特定入口：

- `zippy.connect(uri="default")` 继续作为默认入口；
- `zippy.read_table("ticks")`、`zippy.query/read_table`、`subscribe()` 行为保持一致；
- CLI 的 `--uri` 仍接受 `zippy://default`、`default`、显式 `tcp://...`；
- `control_endpoint` 旧参数不再作为主概念扩展，继续向 `uri` 收敛。

如果平台 backend 未启用，应给出明确错误：

```text
windows segment backend is not available in this build feature=[segment-windows]
```

## 5. 实施阶段

### M6.6.1 Windows compile baseline

- 给 Unix-only 模块补 `cfg(unix)` 边界；
- 让 Windows 编译错误收敛成少量明确的 unsupported backend；
- Python import 不因 Unix-only symbol 直接失败；
- CI 增加 Windows `cargo check` 或最小 `cargo test`。

验收：

- Windows 上 Rust workspace 可编译到明确边界；
- `import zippy` 不因为 Unix-only import 失败；
- 不影响 Linux 测试。

### M6.6.2 Cross-platform control transport

- 抽出 `ControlTransport`；
- Linux 使用 Unix socket backend；
- Windows 第一版使用 TCP loopback backend；
- URI 解析支持 `zippy://default` 到平台默认 endpoint metadata；
- master/client roundtrip 测试跨平台复用。

验收：

- Windows 上可启动 master；
- Windows 上 `MasterClient` 可 register process、heartbeat、list streams；
- Python `zippy.connect()` 可用。

### M6.6.3 Cross-platform segment mapping

- 抽出 `SegmentMapping`；
- Linux 后端保持当前 mmap 行为；
- Windows 后端实现 file-backed mapping；
- descriptor 扩展 backend kind 和 mapping identity；
- active segment writer/reader 最小跨进程读写可用。

验收：

- Windows writer 写入 active segment；
- Windows reader/query 读取 committed rows；
- rollover 后 descriptor 能正确指向新 segment。

### M6.6.4 Cross-platform segment notification

- 抽出 `SegmentNotifier`；
- Linux 后端保持 futex/spin；
- Windows 后端实现 spin + event wait；
- subscriber 在 Windows 上能等待新 row；
- xfast 与普通模式语义保持一致。

验收：

- Windows `subscribe()` 可收到 live rows；
- writer 暂停时 reader 不应忙等占满 CPU，除非显式 xfast；
- writer 退出后错误和重连语义清晰。

### M6.6.5 Windows e2e and packaging

- 增加 Windows CI；
- 增加 Python wheel/import smoke；
- 增加 master -> stream table -> query/subscriber 跨进程 e2e；
- 文档说明 Windows 当前性能边界和推荐使用方式。

验收：

- Windows 原生环境可跑最小 live e2e；
- Python 用户不需要接触底层 transport/mapping；
- Linux 现有性能 smoke 不退化。

## 6. 测试策略

| 测试层级 | 内容 |
| --- | --- |
| compile check | Windows/Linux feature gate 和平台模块可编译 |
| unit test | URI 解析、endpoint metadata、descriptor encode/decode |
| control roundtrip | master/client register、heartbeat、list、descriptor request |
| segment unit | create/open/map/read/write committed rows |
| cross-process e2e | writer 进程写 stream table，reader 进程 query/subscribe |
| lifecycle | rollover、drop table、process expired、mapping cleanup |
| performance smoke | Windows 延迟和吞吐记录为 baseline，不与 Linux 指标强行对齐 |

## 7. 风险与处理

### 7.1 Windows 句柄生命周期

Windows file mapping 和 event handle 的释放语义与 Unix fd 不同。实现时要把 owner 和 reader 的关闭
路径拆清楚，避免 writer 退出导致 reader 立即失效，或 reader 泄漏句柄导致文件无法删除。

### 7.2 endpoint metadata 一致性

如果 `zippy://default` 依赖 `endpoint.toml`，master 非正常退出后可能留下 stale endpoint。client
连接失败时应能给出明确提示，并允许新 master 覆盖 stale metadata。

### 7.3 低延迟指标波动

Windows 调度、event wait 和 Python GIL 行为都可能使尾延迟高于 Linux。第一版关注功能可用和指标
可观测，后续再单独做 Windows 低延迟优化。

### 7.4 插件生态

zippy core Windows 可用不等于每个插件立即可用。OpenCTP 插件还依赖 vendor CTP 动态库、路径配置和
绑定生成方式，应作为插件级验收单独推进。

## 8. 推荐结论

推荐按 `M6.6.1 -> M6.6.2 -> M6.6.3 -> M6.6.4 -> M6.6.5` 推进。

短期不要直接在 Windows 上边改边摸索全部实现。更稳妥的方式是：

1. 在 Linux 主开发环境中先抽象平台边界，并保持现有 Linux 行为和性能不退化；
2. 在 Windows 原生环境中实现 control transport 和 segment backend；
3. 通过 Windows CI 和真实跨进程 e2e 验收；
4. 最后再考虑 Windows 专项低延迟优化。

