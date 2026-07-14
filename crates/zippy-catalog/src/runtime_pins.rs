//! In-process snapshot-pin admission coordinated with safe garbage collection.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use zippy_types::{ErrorCode, Result, ServerError, TableId};

const RUNTIME_PIN_SHARDS: usize = 64;

/// Current process-local query snapshot pin admission state and cumulative rejections.
///
/// Active values are independently sampled lock-free gauges. Under concurrent admission or drop,
/// one returned sample can briefly observe different instants for the two gauges. Exhaustion
/// counters are monotonic for the lifetime of one [`crate::SqliteCatalog`] instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteCatalogRuntimeSnapshotPinStatus {
    /// Whether the registry still accepts new query pins and GC exclusions.
    pub admission_open: bool,
    /// Query snapshot pin guards currently retained by callers.
    pub active_pins: usize,
    /// Table entries retained by query pins or an active GC exclusion.
    pub active_tables: usize,
    /// Configured maximum number of active query snapshot pin guards.
    pub max_active_pins: usize,
    /// Configured maximum number of retained table entries.
    pub max_active_tables: usize,
    /// Query pin admissions rejected because `max_active_pins` was reached.
    pub pin_budget_exhaustions_total: u64,
    /// Pin or GC admissions rejected because `max_active_tables` was reached.
    pub table_budget_exhaustions_total: u64,
}

#[derive(Default)]
struct TablePinState {
    pins: BTreeMap<u64, usize>,
    pin_count: usize,
    garbage_collection_excluded: bool,
}

pub(crate) struct RuntimeSnapshotPinRegistry {
    closed: AtomicBool,
    shards: Box<[Mutex<HashMap<TableId, TablePinState>>]>,
    active_pins: AtomicUsize,
    active_tables: AtomicUsize,
    max_pins: usize,
    max_tables: usize,
    pin_budget_exhaustions: AtomicU64,
    table_budget_exhaustions: AtomicU64,
    _runtime_ownership: Option<zippy_core::CatalogRuntimeOwnership>,
}

impl RuntimeSnapshotPinRegistry {
    pub(crate) fn new(
        ownership: zippy_core::CatalogRuntimeOwnership,
        max_pins: usize,
        max_tables: usize,
    ) -> Arc<Self> {
        Self::new_internal(max_pins, max_tables, Some(ownership))
    }

    #[cfg(test)]
    fn new_with_limits(max_pins: usize, max_tables: usize) -> Arc<Self> {
        Self::new_internal(max_pins, max_tables, None)
    }

    fn new_internal(
        max_pins: usize,
        max_tables: usize,
        runtime_ownership: Option<zippy_core::CatalogRuntimeOwnership>,
    ) -> Arc<Self> {
        let shards = (0..RUNTIME_PIN_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(Self {
            closed: AtomicBool::new(false),
            shards,
            active_pins: AtomicUsize::new(0),
            active_tables: AtomicUsize::new(0),
            max_pins,
            max_tables,
            pin_budget_exhaustions: AtomicU64::new(0),
            table_budget_exhaustions: AtomicU64::new(0),
            _runtime_ownership: runtime_ownership,
        })
    }

    pub(crate) fn pin(
        self: &Arc<Self>,
        table_id: TableId,
        snapshot_version: u64,
    ) -> Result<SqliteCatalogRuntimeSnapshotPin> {
        if snapshot_version == 0 {
            return Err(invalid("runtime snapshot pin version must be nonzero"));
        }
        let shard = self.shard(table_id);
        let mut tables = shard.lock().map_err(|_| poisoned())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        if tables
            .get(&table_id)
            .is_some_and(|table| table.garbage_collection_excluded)
        {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "runtime snapshot pin admission is excluded by garbage collection",
            )
            .with_table_id(table_id)
            .with_retryable(true));
        }
        let inserted_table =
            if let std::collections::hash_map::Entry::Vacant(entry) = tables.entry(table_id) {
                reserve_counter(
                    &self.active_tables,
                    self.max_tables,
                    &self.table_budget_exhaustions,
                    "runtime snapshot pin table budget is exhausted",
                )?;
                entry.insert(TablePinState::default());
                true
            } else {
                false
            };
        if let Err(error) = reserve_counter(
            &self.active_pins,
            self.max_pins,
            &self.pin_budget_exhaustions,
            "runtime snapshot pin budget is exhausted",
        ) {
            if inserted_table {
                tables.remove(&table_id);
                self.active_tables.fetch_sub(1, Ordering::AcqRel);
            }
            return Err(error.with_table_id(table_id));
        }
        let table = tables
            .get_mut(&table_id)
            .expect("runtime pin table was inserted before pin accounting");
        let count = table.pins.entry(snapshot_version).or_default();
        *count += 1;
        table.pin_count += 1;
        Ok(SqliteCatalogRuntimeSnapshotPin {
            _token: RuntimeSnapshotPinToken {
                registry: Arc::clone(self),
                table_id,
                snapshot_version,
            },
        })
    }

    pub(crate) fn exclude_garbage_collection(
        self: &Arc<Self>,
        table_id: TableId,
    ) -> Result<SqliteGarbageCollectionGuard> {
        let shard = self.shard(table_id);
        let mut tables = shard.lock().map_err(|_| poisoned())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        if tables
            .get(&table_id)
            .is_some_and(|table| table.garbage_collection_excluded)
        {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "garbage collection already excludes runtime snapshot admission",
            )
            .with_table_id(table_id)
            .with_retryable(true));
        }
        let table = if let std::collections::hash_map::Entry::Vacant(entry) = tables.entry(table_id)
        {
            reserve_counter(
                &self.active_tables,
                self.max_tables,
                &self.table_budget_exhaustions,
                "runtime snapshot pin table budget is exhausted",
            )
            .map_err(|error| error.with_table_id(table_id))?;
            entry.insert(TablePinState::default())
        } else {
            tables
                .get_mut(&table_id)
                .expect("runtime pin table entry exists")
        };
        table.garbage_collection_excluded = true;
        let runtime_min_pinned_snapshot = table.pins.keys().next().copied();
        Ok(SqliteGarbageCollectionGuard {
            registry: Arc::clone(self),
            owner_id: Arc::as_ptr(self) as usize,
            table_id,
            runtime_min_pinned_snapshot,
        })
    }

    pub(crate) fn owner_id(self: &Arc<Self>) -> usize {
        Arc::as_ptr(self) as usize
    }

    pub(crate) fn status(&self) -> SqliteCatalogRuntimeSnapshotPinStatus {
        SqliteCatalogRuntimeSnapshotPinStatus {
            admission_open: !self.closed.load(Ordering::Acquire),
            active_pins: self.active_pins.load(Ordering::Acquire),
            active_tables: self.active_tables.load(Ordering::Acquire),
            max_active_pins: self.max_pins,
            max_active_tables: self.max_tables,
            pin_budget_exhaustions_total: self.pin_budget_exhaustions.load(Ordering::Relaxed),
            table_budget_exhaustions_total: self.table_budget_exhaustions.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        for shard in &self.shards {
            drop(shard.lock().unwrap_or_else(|error| error.into_inner()));
        }
    }

    fn release_pin(&self, table_id: TableId, snapshot_version: u64) {
        let shard = self.shard(table_id);
        let mut tables = shard.lock().unwrap_or_else(|error| error.into_inner());
        let remove_table = if let Some(table) = tables.get_mut(&table_id) {
            if let Some(count) = table.pins.get_mut(&snapshot_version) {
                *count -= 1;
                table.pin_count -= 1;
                self.active_pins.fetch_sub(1, Ordering::AcqRel);
                if *count == 0 {
                    table.pins.remove(&snapshot_version);
                }
            }
            table.pin_count == 0 && !table.garbage_collection_excluded
        } else {
            false
        };
        if remove_table {
            tables.remove(&table_id);
            self.active_tables.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn release_garbage_collection(&self, table_id: TableId) {
        let shard = self.shard(table_id);
        let mut tables = shard.lock().unwrap_or_else(|error| error.into_inner());
        let remove_table = if let Some(table) = tables.get_mut(&table_id) {
            table.garbage_collection_excluded = false;
            table.pin_count == 0
        } else {
            false
        };
        if remove_table {
            tables.remove(&table_id);
            self.active_tables.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn shard(&self, table_id: TableId) -> &Mutex<HashMap<TableId, TablePinState>> {
        let value = table_id.as_u128();
        let folded = (value as u64) ^ ((value >> 64) as u64);
        &self.shards[(folded as usize) % self.shards.len()]
    }
}

fn reserve_counter(
    counter: &AtomicUsize,
    maximum: usize,
    exhaustions: &AtomicU64,
    exhausted_message: &'static str,
) -> Result<()> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < maximum).then_some(current + 1)
        })
        .map(|_| ())
        .map_err(|_| {
            increment_saturating(exhaustions);
            ServerError::new(ErrorCode::ResourceExhausted, exhausted_message).with_retryable(true)
        })
}

fn increment_saturating(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

/// RAII guard for one in-process exact snapshot pin.
///
/// Dropping the value releases the pin. The guard does not own the Catalog service and therefore
/// cannot keep SQLite workers alive during shutdown.
pub struct SqliteCatalogRuntimeSnapshotPin {
    _token: RuntimeSnapshotPinToken,
}

impl std::fmt::Debug for SqliteCatalogRuntimeSnapshotPin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteCatalogRuntimeSnapshotPin")
            .finish_non_exhaustive()
    }
}

struct RuntimeSnapshotPinToken {
    registry: Arc<RuntimeSnapshotPinRegistry>,
    table_id: TableId,
    snapshot_version: u64,
}

impl Drop for RuntimeSnapshotPinToken {
    fn drop(&mut self) {
        self.registry
            .release_pin(self.table_id, self.snapshot_version);
    }
}

pub(crate) struct SqliteGarbageCollectionGuard {
    registry: Arc<RuntimeSnapshotPinRegistry>,
    owner_id: usize,
    table_id: TableId,
    runtime_min_pinned_snapshot: Option<u64>,
}

impl SqliteGarbageCollectionGuard {
    pub(crate) const fn owner_id(&self) -> usize {
        self.owner_id
    }

    pub(crate) const fn table_id(&self) -> TableId {
        self.table_id
    }

    pub(crate) const fn runtime_min_pinned_snapshot(&self) -> Option<u64> {
        self.runtime_min_pinned_snapshot
    }
}

impl Drop for SqliteGarbageCollectionGuard {
    fn drop(&mut self) {
        self.registry.release_garbage_collection(self.table_id);
    }
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn unavailable() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "runtime snapshot pin registry is closed",
    )
    .with_retryable(false)
}

fn poisoned() -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        "runtime snapshot pin registry mutex is poisoned",
    )
    .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::RuntimeSnapshotPinRegistry;
    use zippy_types::{ErrorCode, TableId};

    fn table(value: u128) -> TableId {
        TableId::from_u128(value).expect("nonzero table id")
    }

    #[test]
    fn pin_and_table_budgets_are_released_with_raii_guards() {
        let registry = RuntimeSnapshotPinRegistry::new_with_limits(2, 1);
        let initial = registry.status();
        assert!(initial.admission_open);
        assert_eq!(initial.active_pins, 0);
        assert_eq!(initial.active_tables, 0);
        assert_eq!(initial.max_active_pins, 2);
        assert_eq!(initial.max_active_tables, 1);
        assert_eq!(initial.pin_budget_exhaustions_total, 0);
        assert_eq!(initial.table_budget_exhaustions_total, 0);

        let first = registry.pin(table(1), 1).expect("first pin");
        let second = registry.pin(table(1), 2).expect("second pin");

        let pin_exhausted = registry.pin(table(1), 3).expect_err("pin budget exhausted");
        assert_eq!(pin_exhausted.code(), ErrorCode::ResourceExhausted);
        let table_exhausted = registry
            .pin(table(2), 1)
            .expect_err("table budget exhausted");
        assert_eq!(table_exhausted.code(), ErrorCode::ResourceExhausted);
        let exhausted = registry.status();
        assert_eq!(exhausted.active_pins, 2);
        assert_eq!(exhausted.active_tables, 1);
        assert_eq!(exhausted.pin_budget_exhaustions_total, 1);
        assert_eq!(exhausted.table_budget_exhaustions_total, 1);

        drop(first);
        let replacement = registry.pin(table(1), 3).expect("released pin budget");
        drop((second, replacement));
        let other_table = registry.pin(table(2), 1).expect("released table budget");
        drop(other_table);
        let released = registry.status();
        assert_eq!(released.active_pins, 0);
        assert_eq!(released.active_tables, 0);
        assert_eq!(released.pin_budget_exhaustions_total, 1);
        assert_eq!(released.table_budget_exhaustions_total, 1);
    }

    #[test]
    fn garbage_collection_excludes_only_its_table_shard_entry() {
        let registry = RuntimeSnapshotPinRegistry::new_with_limits(4, 4);
        let exclusion = registry
            .exclude_garbage_collection(table(1))
            .expect("gc exclusion");
        let conflict = registry
            .pin(table(1), 1)
            .expect_err("same table pin must conflict");
        assert_eq!(conflict.code(), ErrorCode::Conflict);

        let independent = registry.pin(table(2), 1).expect("other table remains open");
        drop(exclusion);
        let admitted = registry.pin(table(1), 1).expect("exclusion released");
        drop((independent, admitted));
    }

    #[test]
    fn close_rejects_new_admission_without_invalidating_existing_guards() {
        let registry = RuntimeSnapshotPinRegistry::new_with_limits(2, 2);
        let existing = registry.pin(table(1), 1).expect("existing pin");
        registry.close();
        assert!(!registry.status().admission_open);

        let error = registry
            .pin(table(2), 1)
            .expect_err("closed registry rejects new pin");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        drop(existing);
    }
}
