# Benchmark Environment

采集日期：2026-07-19

本文件记录 `2026-07-19-flight-gateway-transport` 原始基准证据对应的运行环境。
基准只使用本机 loopback，不包含账号或凭据。

## Platform

- kernel：`Linux 5.15.153.1-microsoft-standard-WSL2 x86_64`
- operating system：`Ubuntu 20.04.6 LTS (Focal Fossa)`
- virtualization：`wsl`，Microsoft full virtualization
- architecture：`x86_64`
- CPU：`AMD Ryzen 9 9950X 16-Core Processor`
- vCPU：32
- threads per core：2
- cores per socket：16
- sockets：1

## Rust Toolchain

```text
rustc 1.86.0 (05f9846f8 2025-03-31)
host: x86_64-unknown-linux-gnu
LLVM version: 19.1.7
cargo 1.86.0 (adf9b6ad1 2025-02-28)
```

## Benchmark Artifact

- release test binary：`server_runtime-f77a511a073d5bb0`
- binary size：111,416,656 bytes
- binary SHA-256：`dc684fbc907640d69c5e2f5d29960c7292c21a6d211ff196849231164991886a`
- target directory：`/tmp/zippy-target-stage7`
- final report commit：`9967f226b1866ba98a3106a3ac219cb5125ca288`
- `before.json` SHA-256：`a672185323a9fd9d2e49a62c7874a0a8c6fa909fd784170b84a5813d40abd3f9`
- `after.json` SHA-256：`bc1ae9571e6f056d32073d376b03b6367239f3bfc2e717343620e58d0213fb82`

采集时 `/tmp` 与工作区位于同一文件系统，可用空间约 828 GiB。所有 Cargo 产物均定向到
`/tmp/zippy-target-stage7`；仓库根目录未保留 `target/`。
