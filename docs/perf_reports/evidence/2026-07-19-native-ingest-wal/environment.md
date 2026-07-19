# Stage 7.2 native ingest WAL benchmark environment

- Git revision: `7e9a9142d3f859c89c0898fe53e4ccd7dabb2243`
- Zippy version: `0.5.0`
- Kernel: `Linux 5.15.153.1-microsoft-standard-WSL2 x86_64`
- CPU: `AMD Ryzen 9 9950X 16-Core Processor`, 16 cores / 32 threads visible
- Rust: `rustc 1.86.0 (05f9846f8 2025-03-31)`, LLVM 19.1.7
- Cargo: `cargo 1.86.0 (adf9b6ad1 2025-02-28)`
- Filesystem reported for `/tmp`: `ext2/ext3`, block size 4096
- Cargo profile: `release`
- Tokio benchmark worker threads: 2
- Warmup samples per scenario: 20
- Measured samples per scenario: 200
- WAL policy: one awaited WAL ACK per batch; no concurrent group coalescing
- Build target: `/tmp/zippy-target-stage7-2` (removed after verification)
