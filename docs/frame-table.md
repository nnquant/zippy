# Frame Table

## 原生主干状态（2026-09-22）

原生 frame 已接入在线建表、Flight 写入、WAL、TableActor、Parquet/manifest 和重启恢复。
`zippy_client.Client.create_frame_table(..., by="dt")` 创建表；`write_frame()` 每次调用传入一帧，
不使用按行缓存的 `get_writer()`。输入 Arrow Table 含多个 chunk 时仍合并为一批；超过服务端
整帧容量时在发送前拒绝，不拆帧。默认 WAL，也可显式请求 Memory 或 Persisted。

frame 与 append 共用 `NativeTableMaterializerSink` 和原生 ingest authority，但 frame 每批
完成后立即封段。TableActor 只接受完整 sealed frame，manifest 提交不得停在帧中间。
空帧不产生可订阅段；空批次仍可通过底层协议得到不分配 offset 的幂等回执。

以下本地引擎/订阅用法仍有效。Flight 实时订阅和本地计算输出统一接入尚未完成，不能把原生
写入与恢复测试视为远端 `subscribe_frames()` 已交付。当前边界见
[能力表](current-capabilities-2026-09-21.md)。


## Purpose

A frame table preserves an upstream business batch as one immutable, ordered delivery unit.
It reuses the append-table storage path, but adds a stronger subscription contract: one
`subscribe_frames()` callback corresponds to one complete upstream frame.

The initial implementation is intended for `CrossSectionalEngine`, where every closed time
bucket contains the full set of instruments available for that cross-section.

## Quick Start

```python
session = (
    zippy.Session(name="stock_selection", master=master)
    .engine(cross_sectional_engine)
    .frame_table("stock_factor_frames", by="dt")
    .publish()
    .start()
)


def on_frame(frame):
    selected = frame.sort_by([("ret_rank", "ascending")]).slice(0, 20)
    rebalance(selected)


subscriber = zippy.subscribe_frames(
    "stock_factor_frames",
    on_frame,
    master=master,
)
```

`by` is a required keyword-only argument and must match the `CrossSectionalEngine.dt_column`.
This keeps the frame boundary explicit at the publish site.

## Choosing A Table Semantic

| Requirement | Publish API | Subscribe API |
| --- | --- | --- |
| Every row is an independent ordered event | `append_table(name)` | `subscribe()` |
| Incremental Arrow batches for throughput | `append_table(name)` | `subscribe_table()` |
| Every callback requires one complete cross-section | `frame_table(name, by="dt")` | `subscribe_frames()` |
| Only the latest value per key matters | `key_value_table(name, by=...)` | `subscribe_table()` |

Use a frame table when correctness depends on seeing all rows in a business bucket before making
a decision. Typical examples include:

- cross-sectional ranking, neutralization, and portfolio construction;
- synchronized multi-instrument risk checks;
- batch model inference where all entities share one evaluation timestamp;
- coordinated allocation or rebalance decisions produced once per closed bucket.

Do not use a frame table when:

- a CTA strategy must react to each factor event independently;
- callback grouping is only a throughput concern;
- intermediate versions may be skipped because only current state matters;
- every frame contains one very small high-frequency event and per-frame rollover cost dominates.

## Runtime Contract

For every non-empty upstream output batch, a frame table:

1. validates that the frame fits in one segment;
2. writes all columns while the segment payload is in mutation state;
3. publishes the committed prefix only after the entire frame succeeds;
4. seals the segment immediately;
5. publishes the next active descriptor while retaining ordered sealed descriptors.

The subscriber drains the current segment before applying the next descriptor. If master metadata
advances across several generations, the watcher restores intermediate retained segments in order.
This prevents a slow callback from merging adjacent frames or splitting one frame across callbacks.

## Query And Persistence

A frame table remains append-only at the logical storage layer. `read_table()` and parquet
persistence use the existing append-table implementation. The `by` column remains in the schema,
so historical queries can group or filter by the original frame boundary.

The frame boundary is a live delivery contract. A normal `subscribe_table()` call is still a
generic incremental Arrow subscription and should not be used when callback boundaries are part of
strategy correctness.

## Operational Limits

- `subscribe_frames()` currently requires a local segment data path. Remote gateway delivery does
  not yet implement the frame protocol.
- Subscription starts from the live tail. Frames produced while the subscriber is disconnected are
  not replayed automatically.
- `table.retention_segments` must cover the worst expected callback lag. A practical lower bound is
  `ceil(max_callback_block_time / frame_interval) + safety_margin`.
- A frame larger than `table.row_capacity` is rejected atomically instead of being fragmented.
- `CrossSectionalEngine` currently rejects late event-time rows. Raw exchange timestamps that can
  arrive out of order need an upstream reorder buffer or watermark policy.
- Frames containing nulls currently use the transactional row writer. Non-null frames use the
  faster atomic columnar path.

These limits are explicit because silently dropping, splitting, or partially publishing a frame
would violate the strategy contract.
