//! Bounded process-local fixed snapshot leases shared by all Flight frontend clones.

use crate::{NativeFixedSnapshot, NativeFixedSnapshotRequest, NativeQueryEngine};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zippy_protocol::fixed_snapshot::FlightFixedSnapshotRequest;
use zippy_protocol::wire::{FixedSnapshotInfo, QueryTableVersion};
use zippy_types::{ErrorCode, QueryId, ResourceLimit, Result, ServerError, SessionId};

const MAX_FIXED_SNAPSHOT_LEASES: usize = 32;

pub(crate) struct FixedSnapshotLease {
    pub(crate) snapshot: NativeFixedSnapshot,
    session_id: SessionId,
    _admission: OwnedSemaphorePermit,
}

pub(crate) struct FixedSnapshotStore {
    admission: Arc<Semaphore>,
    leases: Mutex<HashMap<QueryId, Arc<FixedSnapshotLease>>>,
}

impl FixedSnapshotStore {
    pub(crate) fn new() -> Self {
        Self {
            admission: Arc::new(Semaphore::new(MAX_FIXED_SNAPSHOT_LEASES)),
            leases: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn create(
        &self,
        engine: &NativeQueryEngine,
        request: FlightFixedSnapshotRequest,
    ) -> Result<FixedSnapshotInfo> {
        self.sweep(0, &[]);
        let admission = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "fixed snapshot lease capacity is exhausted",
                )
            })?;
        let owner = engine.owner(request.session_id);
        let snapshot = engine
            .capture_fixed_snapshot(
                owner.as_ref(),
                NativeFixedSnapshotRequest {
                    query_id: request.snapshot_id,
                    tables: request.tables,
                    expires_at_ns: request.expires_at_ns,
                    memory_limit_bytes: ResourceLimit::ServerDefault,
                },
            )
            .await?;
        if snapshot.is_expired() {
            return Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                "fixed snapshot expired during capture",
            ));
        }
        let info = FixedSnapshotInfo {
            protocol_version: zippy_protocol::CURRENT_PROTOCOL_VERSION,
            snapshot_id: request.snapshot_id.into_bytes().to_vec().into(),
            expires_at_ns: snapshot.expires_at_ns(),
            catalog_revision: snapshot.info().catalog_revision,
            tables: snapshot
                .info()
                .snapshots
                .iter()
                .map(|table| QueryTableVersion {
                    table_id: table.table_id.into_bytes().to_vec().into(),
                    snapshot_version: table.snapshot_version,
                    schema_version: table.schema_version,
                })
                .collect(),
        };
        let lease = Arc::new(FixedSnapshotLease {
            snapshot,
            session_id: request.session_id,
            _admission: admission,
        });
        let mut leases = self
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.admission.is_closed() {
            drop(leases);
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "fixed snapshot service is closed",
            ));
        }
        match leases.entry(request.snapshot_id) {
            Entry::Vacant(entry) => {
                entry.insert(lease);
            }
            Entry::Occupied(_) => {
                drop(leases);
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "fixed snapshot identity collision",
                ));
            }
        }
        Ok(info)
    }

    pub(crate) fn get(
        &self,
        session_id: SessionId,
        id: QueryId,
    ) -> Result<Arc<FixedSnapshotLease>> {
        let lease = self
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                ServerError::new(ErrorCode::NotFound, "fixed snapshot is unavailable")
            })?;
        if lease.session_id != session_id {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "fixed snapshot belongs to another session",
            ));
        }
        if lease.snapshot.is_expired() {
            self.release(session_id, id)?;
            return Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                "fixed snapshot expired",
            ));
        }
        Ok(lease)
    }

    pub(crate) fn release(&self, session_id: SessionId, id: QueryId) -> Result<bool> {
        let removed = {
            let mut leases = self
                .leases
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if leases
                .get(&id)
                .is_some_and(|lease| lease.session_id != session_id)
            {
                return Err(ServerError::new(
                    ErrorCode::PermissionDenied,
                    "fixed snapshot belongs to another session",
                ));
            }
            leases.remove(&id)
        };
        let released = removed.is_some();
        drop(removed);
        Ok(released)
    }

    pub(crate) fn len(&self) -> usize {
        self.leases
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    pub(crate) fn sweep(&self, now_ns: u64, expired_sessions: &[SessionId]) -> usize {
        if now_ns == u64::MAX {
            self.admission.close();
        }
        let removed = {
            let mut leases = self
                .leases
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let expired = leases
                .iter()
                .filter_map(|(id, lease)| {
                    (lease.snapshot.expires_at_ns() <= now_ns
                        || lease.snapshot.is_expired()
                        || expired_sessions.contains(&lease.session_id))
                    .then_some(*id)
                })
                .collect::<Vec<_>>();
            expired
                .into_iter()
                .filter_map(|id| leases.remove(&id))
                .collect::<Vec<_>>()
        };
        let count = removed.len();
        // Snapshot destruction can touch QueryService; release the registry lock first.
        drop(removed);
        count
    }
}
