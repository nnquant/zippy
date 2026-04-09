# Expression Planner Design

## Overview

This document defines the next-generation expression system for `zippy`.
The goal is to replace the current lightweight `EXPR(...)` helper with a
planner-backed `Expr(...)` API that can evolve into a high-performance,
extensible factor language.

The immediate target is a controlled first phase:

- Replace `zippy.EXPR(...)` with `zippy.Expr(...)`
- Enforce uppercase function names
- Support a planner-backed DAG model
- Allow `TS_*` functions inside `ReactiveStateEngine`
- Keep `TimeSeriesEngine.pre_factors` and `post_factors` restricted to
  column-only expressions

The long-term design priority is execution performance. The planner must be
able to optimize repeated subexpressions and preserve the option to map pure
column subgraphs onto lower-level optimized backends such as Polars-style
expression execution.

## Goals

- Provide a single public Python entrypoint: `zippy.Expr(...)`
- Reject lowercase function names at construction time
- Compile expression strings into a typed DAG instead of directly evaluating
  the parsed AST row by row
- Support the following function families in the first planner phase:
  - column operations: `ABS`, `LOG`, `CLIP`, `CAST`
  - time-series operations: `TS_DIFF`, `TS_STD`, `TS_MEAN`, `TS_DELAY`,
    `TS_EMA`, `TS_RETURN`
- Perform common subexpression elimination inside one expression
- Keep the architecture open for future `AGG_*`, `CS_*`, conditionals, null
  handling, and backend specialization

## Non-Goals

- Do not support `AGG_*` inside `Expr` in this phase
- Do not support `CS_*` inside `Expr` in this phase
- Do not add lowercase compatibility aliases
- Do not keep `zippy.EXPR(...)` as a compatibility shim
- Do not make `TimeSeriesEngine` run stateful `TS_*` expression nodes

## Public API

### Python Entry Point

The public API becomes:

```python
zippy.Expr(
    expression="TS_DIFF(mid_price, 200) / TS_STD(TS_DIFF(mid_price, 200), 200)",
    output="MID_PRICE_DIFF_200_STD_200",
)
```

`zippy.EXPR(...)` is removed with no compatibility bridge.

### Naming Rules

All expression function names must be uppercase.

Supported examples:

- `ABS(price)`
- `LOG(price)`
- `CLIP(price, 0.0, 10.0)`
- `CAST(price, "float64")`
- `TS_DIFF(price, 2)`
- `TS_STD(price, 200)`

Unsupported examples:

- `abs(price)`
- `log(price)`
- `clip(price, 0.0, 10.0)`

Lowercase function names fail at construction time with an explicit error:

```text
function names must be uppercase function=[abs] expected=[ABS]
```

## Execution Model

The new system has three stages:

1. Parser
2. Planner
3. Executor

### Parser

The parser is responsible only for syntax.

It produces an AST that contains:

- literals
- identifiers
- binary operators
- function calls

The parser does not perform execution.

### Planner

The planner lowers the AST into a typed computation DAG.

Each node carries:

- `node_id`
- `output_field`
- `data_type`
- `nullable`
- `dependencies`
- `execution_kind`

The first DAG node kinds are:

- `Input`
- `Literal`
- `ColumnOp`
- `TsOp`

The planner is responsible for:

- schema and type inference
- engine capability checks
- common subexpression elimination
- stable internal naming for intermediate nodes

### Executor

The executor runs the planned DAG instead of interpreting the AST directly.

The execution model is split:

- `ColumnOp`
  - pure column expressions
  - candidates for backend fusion and future Polars-style lowering
- `TsOp`
  - stateful operators
  - mapped onto existing Rust time-series operator implementations

This split is the core long-term performance decision.

## Engine Capability Rules

### ReactiveStateEngine

`ReactiveStateEngine` is the primary target for full planner support.

It accepts:

- `Input`
- `Literal`
- `ColumnOp`
- `TsOp`

So the following are valid in `ReactiveStateEngine`:

- `ABS(x) + 1.0`
- `TS_DIFF(price, 2)`
- `TS_DIFF(price, 2) / TS_STD(TS_DIFF(price, 2), 200)`

### TimeSeriesEngine.pre_factors

`pre_factors` are restricted to:

- `Input`
- `Literal`
- `ColumnOp`

They do not allow `TsOp`.

This keeps `pre_factors` as pure per-row preprocessing before window state.

### TimeSeriesEngine.post_factors

`post_factors` are also restricted to:

- `Input`
- `Literal`
- `ColumnOp`

They do not allow `TsOp`.

This keeps post-window result shaping separate from time-series stateful
calculation.

### Rejected Cases

The following must fail at construction time:

- `Expr("TS_DIFF(price, 2)", ...)` inside `TimeSeriesEngine.pre_factors`
- `Expr("TS_DIFF(close, 2)", ...)` inside `TimeSeriesEngine.post_factors`

The error must clearly state that stateful `TS_*` functions are only supported
inside `ReactiveStateEngine`.

## Performance Requirements

The planner must be built with performance-first execution in mind.

### Common Subexpression Elimination

Repeated subexpressions inside one expression must be computed once.

Example:

```text
TS_DIFF(mid_price, 200) / TS_STD(TS_DIFF(mid_price, 200), 200)
```

`TS_DIFF(mid_price, 200)` must be planned as one shared node.

### Column Expression Optimization

Pure column subgraphs must remain representable as a backend-neutral IR.

This IR should be suitable for:

- current Rust/Arrow-native execution
- future Polars-style expression lowering

The planner must not hardcode the public expression language directly into a
single row-wise evaluator.

### Stateful Operator Reuse

Time-series nodes must reuse the existing Rust stateful operator machinery
instead of introducing a second independent implementation path.

This preserves:

- deterministic semantics
- stable performance characteristics
- reuse of current operator-specific tests

## Migration Rules

### Removal of `EXPR`

`zippy.EXPR(...)` is removed immediately.

All tests, examples, and documentation must use `zippy.Expr(...)`.

### Function Name Standardization

All examples and tests must switch to uppercase function names:

- `ABS`
- `LOG`
- `CLIP`
- `CAST`

No compatibility path is retained.

## Error Semantics

All of the following are construction-time errors:

- unknown function
- lowercase function usage
- unsupported engine capability
- type mismatch detectable from schema and expression graph

Runtime errors are only allowed for cases that cannot be proven statically.

## Implementation Strategy

The implementation is split into the following phases:

1. Replace the Python helper API
   - `EXPR -> Expr`
   - update examples, tests, and docs
2. Extend the parser
   - uppercase-only functions
   - `TS_*` syntax nodes
3. Introduce the planner
   - typed DAG
   - common subexpression elimination
4. Connect `ReactiveStateEngine`
   - full `ColumnOp + TsOp` support
5. Connect `TimeSeriesEngine`
   - `ColumnOp` support only
   - explicit rejection of `TsOp`

## Testing Matrix

At minimum, tests must cover:

1. `Expr("ABS(price)", ...)` succeeds
2. `Expr("abs(price)", ...)` fails
3. `Expr("TS_DIFF(price, 2)", ...)` succeeds in `ReactiveStateEngine`
4. `Expr("TS_DIFF(price, 2)", ...)` fails in `TimeSeriesEngine.pre_factors`
5. repeated `TS_DIFF(...)` in one expression is planned once
6. all examples and docs use `zippy.Expr(...)`

## Future Extension Path

This design intentionally leaves room for future expansion:

- `IF`, `IS_NULL`, `COALESCE`
- `AGG_*`
- `CS_*`
- backend selection for pure column subgraphs
- planner-level optimization passes

The important constraint is that all future extensions must fit the same model:

- parse
- plan into a typed DAG
- execute through optimized backend-aware node execution

