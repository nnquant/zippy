# zippy-openctp Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在主仓库下预留一个被忽略的 `plugins/zippy-openctp` 目录，并把它初始化为独立 git 仓库，包含面向 Python 用户的最小插件骨架。

**Architecture:** 主仓库只维护设计文档和 ignore 规则，不将插件代码纳入当前仓库版本控制。插件仓库先搭建独立 Rust + Python 双入口结构，预留 `Source`、schema 和 PyO3 绑定边界，但暂不实现真实 OpenCTP 连接逻辑。

**Tech Stack:** git, Rust workspace, PyO3, maturin/pyproject, Python package skeleton

---

### Task 1: 记录主仓库 bootstrap 计划并忽略插件目录

**Files:**
- Create: `docs/superpowers/plans/2026-04-08-zippy-openctp-bootstrap.md`
- Modify: `.gitignore`

- [ ] **Step 1: 写入 bootstrap 计划文档**

```markdown
# zippy-openctp Bootstrap Implementation Plan

Goal: 在主仓库中忽略插件目录并初始化独立 repo
```

- [ ] **Step 2: 把插件目录加入主仓库 ignore**

```gitignore
/plugins/zippy-openctp/
```

- [ ] **Step 3: 验证主仓库只追踪计划文档与 `.gitignore`**

Run: `git status --short`
Expected: 仅新增计划文档和 `.gitignore` 修改；插件目录内容不出现在父仓库未跟踪文件中

- [ ] **Step 4: 提交主仓库变更**

```bash
git add docs/superpowers/plans/2026-04-08-zippy-openctp-bootstrap.md .gitignore
git commit -m "docs: add zippy-openctp bootstrap plan"
```

### Task 2: 创建插件目录并初始化独立仓库

**Files:**
- Create: `plugins/zippy-openctp/`
- Create: `plugins/zippy-openctp/.gitignore`
- Create: `plugins/zippy-openctp/README.md`
- Create: `plugins/zippy-openctp/Cargo.toml`
- Create: `plugins/zippy-openctp/pyproject.toml`

- [ ] **Step 1: 创建目录结构**

```text
plugins/zippy-openctp/
```

- [ ] **Step 2: 初始化独立 git 仓库**

Run: `git init -b main`
Expected: `Initialized empty Git repository` under `plugins/zippy-openctp/.git/`

- [ ] **Step 3: 写入仓库级 `.gitignore`**

```gitignore
target/
.venv/
dist/
build/
__pycache__/
.pytest_cache/
python/zippy_openctp/*.so
```

- [ ] **Step 4: 写入 README，固定插件定位**

```markdown
# zippy-openctp

Python-first OpenCTP market data plugin for zippy.
```

- [ ] **Step 5: 写入最小顶层 `Cargo.toml` 和 `pyproject.toml`**

```toml
[workspace]
members = ["crates/zippy-openctp-core", "crates/zippy-openctp-python"]
resolver = "2"
```

```toml
[project]
name = "zippy-openctp"
version = "0.1.0"
requires-python = ">=3.11"
```

- [ ] **Step 6: 提交插件仓库初始化**

```bash
git add .
git commit -m "feat: bootstrap zippy-openctp plugin repo"
```

### Task 3: 写入最小插件骨架

**Files:**
- Create: `plugins/zippy-openctp/crates/zippy-openctp-core/Cargo.toml`
- Create: `plugins/zippy-openctp/crates/zippy-openctp-core/src/lib.rs`
- Create: `plugins/zippy-openctp/crates/zippy-openctp-core/src/schema.rs`
- Create: `plugins/zippy-openctp/crates/zippy-openctp-core/src/source.rs`
- Create: `plugins/zippy-openctp/crates/zippy-openctp-core/src/metrics.rs`
- Create: `plugins/zippy-openctp/crates/zippy-openctp-python/Cargo.toml`
- Create: `plugins/zippy-openctp/crates/zippy-openctp-python/src/lib.rs`
- Create: `plugins/zippy-openctp/python/zippy_openctp/__init__.py`
- Create: `plugins/zippy-openctp/python/zippy_openctp/_internal.pyi`
- Create: `plugins/zippy-openctp/python/zippy_openctp/schemas.py`

- [ ] **Step 1: 写入 core crate 清单**

```toml
[package]
name = "zippy-openctp-core"
version = "0.1.0"
edition = "2021"
```

- [ ] **Step 2: 写入 core crate 最小模块导出**

```rust
pub mod metrics;
pub mod schema;
pub mod source;
```

- [ ] **Step 3: 写入 schema/source/metrics 占位 API**

```rust
pub fn tick_data_schema() {}
pub struct OpenCtpMarketDataSourceConfig;
pub struct OpenCtpSourceMetrics;
```

- [ ] **Step 4: 写入 Python crate 与包入口**

```python
from .schemas import TickDataSchema
```

- [ ] **Step 5: 验证目录结构**

Run: `find plugins/zippy-openctp -maxdepth 4 -type f | sort`
Expected: 出现 Rust crate、Python 包和仓库元数据文件

- [ ] **Step 6: 提交插件骨架**

```bash
git add .
git commit -m "feat: add zippy-openctp package skeleton"
```

### Task 4: 验证父子仓库边界

**Files:**
- Verify only

- [ ] **Step 1: 验证主仓库状态**

Run: `git status --short`
Expected: 父仓库不追踪 `plugins/zippy-openctp` 内部文件

- [ ] **Step 2: 验证插件仓库状态**

Run: `git -C plugins/zippy-openctp status --short`
Expected: 工作树干净

- [ ] **Step 3: 记录下一步入口**

```text
next: 在 zippy-openctp 仓库中根据 spec 编写完整实现计划，然后开始对接 ctp2rs 与 Python Source 绑定
```
