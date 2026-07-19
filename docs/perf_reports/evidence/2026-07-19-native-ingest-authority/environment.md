# Benchmark Environment

采集日期：2026-07-19

本文件记录 Stage 7.1 server-owned native ingest authority 回归基准的运行环境。测试只使用本机
loopback，不包含账号或凭据。

## Platform

- kernel：`Linux 5.15.153.1-microsoft-standard-WSL2 x86_64`；
- architecture：`x86_64`；
- CPU：`AMD Ryzen 9 9950X 16-Core Processor`；
- vCPU：32；
- cores：16；
- threads per core：2；
- virtualization：Microsoft full virtualization，WSL2。

## Rust Toolchain

```text
rustc 1.86.0 (05f9846f8 2025-03-31)
host: x86_64-unknown-linux-gnu
LLVM version: 19.1.7
cargo 1.86.0 (adf9b6ad1 2025-02-28)
```

## Benchmark Artifact

- release test binary：`server_runtime-44abd533838ee165`；
- binary size：111,562,128 bytes；
- binary SHA-256：`ad10922e8038c1d96a8ab745bc49c054140cd9ec0f8ae673d3a102742278d9e9`；
- target directory：`/tmp/zippy-target-stage7`；
- report commit：`ac5999dfafea8be62e1148694c7a815846132d7d`；
- `stage7-1.json` SHA-256：`b60adba7b28bd0dbd35c27df9f8718f142502606107194b10bdc7aa0ed927086`。

Cargo 产物全部定向到 `/tmp/zippy-target-stage7`，仓库根目录没有生成 `target/`。原始 JSON 在
提交前从 `/tmp/zippy-transport-stage7-1-final.json` 原样复制，性能测试退出码为 0。
