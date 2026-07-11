# Benchmark Environment

采集日期：2026-07-11

本文件记录 `2026-07-11-low-latency-performance-gates` 原始基准证据对应的运行环境。
内容不包含用户账号或凭据。

## Kernel

`uname -a`：

```text
Linux <hostname-redacted> 5.15.153.1-microsoft-standard-WSL2 #1 SMP Fri Mar 29 23:14:13 UTC 2024 x86_64 x86_64 x86_64 GNU/Linux
```

机器 hostname 已从 `uname -a` 输出中脱敏；其余 kernel 字段保持原值。

## Operating System

`/etc/os-release` 关键字段：

```text
NAME="Ubuntu"
VERSION="20.04.6 LTS (Focal Fossa)"
ID=ubuntu
PRETTY_NAME="Ubuntu 20.04.6 LTS"
VERSION_ID="20.04"
VERSION_CODENAME=focal
```

## CPU And Virtualization

- architecture：`x86_64`
- CPU model：`AMD Ryzen 9 9950X 16-Core Processor`
- vCPU：`32`
- threads per core：`2`
- cores per socket：`16`
- sockets：`1`
- CPU virtualization capability：`AMD-V`
- hypervisor vendor：`Microsoft`
- virtualization type：`full`
- `systemd-detect-virt`：`wsl`
- WSL 可见：是，kernel release 包含 `microsoft-standard-WSL2`
- 虚拟化可见：是，`lscpu` 报告 Microsoft hypervisor

## Rust Toolchain

`rustc -Vv`：

```text
rustc 1.86.0 (05f9846f8 2025-03-31)
binary: rustc
commit-hash: 05f9846f893b09a1be1fc8560e33fc3c815cfecb
commit-date: 2025-03-31
host: x86_64-unknown-linux-gnu
release: 1.86.0
LLVM version: 19.1.7
```

`cargo -V`：

```text
cargo 1.86.0 (adf9b6ad1 2025-02-28)
```

## Benchmark Target

- report target：`linux-x86_64`
- Rust host：`x86_64-unknown-linux-gnu`
- shared memory temporary directory：`/dev/shm`
