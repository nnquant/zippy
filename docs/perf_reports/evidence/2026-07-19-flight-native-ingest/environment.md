# Flight native ingest benchmark environment

- Date: 2026-07-19
- OS: WSL2 Linux 5.15.153.1-microsoft-standard-WSL2, x86_64
- Rust: rustc 1.86.0 (05f9846f8 2025-03-31)
- Cargo: cargo 1.86.0 (adf9b6ad1 2025-02-28)
- Profile: release
- Transport: localhost Arrow Flight, no TLS
- Gateway: disabled
- Catalog: SQLite authoritative
- Open-file limit: 1024
- Warmup/measured samples per scenario: 10/100
- Base commit: 0d81c0a53a8e4d75df6f1d9d52ad03135559fc5b
- Worktree: dirty with the Stage 7.3 implementation and low-latency revision under measurement
- Cargo targets: `/tmp/zippy-target-stage7-3`, `/tmp/zippy-target-stage7-4`

All benchmarks reuse one authenticated Flight client and Writer session. `stage7-3.json` opens a
new DoPut logical producer connection and acquires a new epoch for every operation.
`stage7-4-short-put.json` opens a new DoPut per operation but resumes one stable producer,
connection, and epoch. `stage7-4-persistent-put.json` keeps one DoPut open per durability and waits
for each batch receipt before sending the next batch. TCP connect and Handshake latency are
excluded from scenario samples.
