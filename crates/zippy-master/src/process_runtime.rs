//! Boot-scoped runtime records sharing the control process registry lock.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use zippy_types::runtime::{
    RuntimeCasUpdate, RuntimeMetricValue, RuntimeObjectId, RuntimeRecord, RuntimeRegistration,
    RuntimeRegistry, RuntimeSnapshot, RuntimeState,
};
use zippy_types::{ErrorCode, ProcessId, Result, ServerError, SessionId};

use crate::registry::{ProcessRecord, Registry};

const PROCESS_RUNTIME_ID_SCHEME: &[u8] = b"zippy.process_runtime.id.v1";
const PROCESS_RUNTIME_ID_PREFIX: [u8; 4] = *b"ZPR1";
const ADMISSION_INACTIVE: u8 = 0;
const ADMISSION_ACTIVE: u8 = 1;
const ADMISSION_SHUTDOWN: u8 = 2;

/// Typed runtime records and process leases share one mutex and boot identity.
#[derive(Clone)]
pub struct ProcessRuntimeRegistry {
    registry: Arc<Mutex<Registry>>,
    running: Arc<AtomicBool>,
    admission: Arc<AtomicU8>,
}

impl ProcessRuntimeRegistry {
    pub(crate) fn new(registry: Arc<Mutex<Registry>>, running: Arc<AtomicBool>) -> Self {
        Self::new_with_admission(registry, running, ADMISSION_ACTIVE)
    }

    pub(crate) fn new_inactive(registry: Arc<Mutex<Registry>>, running: Arc<AtomicBool>) -> Self {
        Self::new_with_admission(registry, running, ADMISSION_INACTIVE)
    }

    fn new_with_admission(
        registry: Arc<Mutex<Registry>>,
        running: Arc<AtomicBool>,
        admission: u8,
    ) -> Self {
        Self {
            registry,
            running,
            admission: Arc::new(AtomicU8::new(admission)),
        }
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, Registry>> {
        self.ensure_admission_open()?;
        let registry = self.registry.lock().map_err(|_| {
            ServerError::new(
                ErrorCode::Internal,
                "control runtime registry lock is poisoned",
            )
        })?;
        self.ensure_admission_open()?;
        if !self.running.load(Ordering::Acquire) {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "control runtime registry master lifecycle is stopped",
            ));
        }
        Ok(registry)
    }

    fn ensure_admission_open(&self) -> Result<()> {
        match self.admission.load(Ordering::Acquire) {
            ADMISSION_ACTIVE => Ok(()),
            ADMISSION_INACTIVE => Err(ServerError::new(
                ErrorCode::Unavailable,
                "control runtime registry admission is not active",
            )),
            ADMISSION_SHUTDOWN => Err(ServerError::new(
                ErrorCode::Unavailable,
                "control runtime registry admission is closed",
            )),
            _ => Err(ServerError::new(
                ErrorCode::Internal,
                "control runtime registry admission state is invalid",
            )),
        }
    }

    /// Opens an inactive composition adapter exactly once.
    pub fn activate(&self) -> Result<()> {
        let _registry = self.registry.lock().map_err(|_| {
            ServerError::new(
                ErrorCode::Internal,
                "control runtime registry lock is poisoned",
            )
        })?;
        if !self.running.load(Ordering::Acquire) {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "control runtime registry master lifecycle is stopped",
            ));
        }
        match self.admission.compare_exchange(
            ADMISSION_INACTIVE,
            ADMISSION_ACTIVE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(ADMISSION_ACTIVE) => Ok(()),
            Err(ADMISSION_SHUTDOWN) => Err(ServerError::new(
                ErrorCode::Conflict,
                "control runtime registry admission is permanently closed",
            )),
            Err(_) => Err(ServerError::new(
                ErrorCode::Internal,
                "control runtime registry admission state is invalid",
            )),
        }
    }

    /// Irreversibly rejects new runtime registry work for this adapter family.
    pub fn begin_shutdown(&self) {
        let _registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.admission.store(ADMISSION_SHUTDOWN, Ordering::Release);
    }

    /// Returns whether this adapter family currently accepts work.
    #[must_use]
    pub fn is_accepting_work(&self) -> bool {
        self.admission.load(Ordering::Acquire) == ADMISSION_ACTIVE
            && self.running.load(Ordering::Acquire)
    }
}

impl RuntimeRegistry for ProcessRuntimeRegistry {
    fn boot_id(&self) -> SessionId {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .runtime_state()
            .boot_id()
    }

    fn register(&self, registration: RuntimeRegistration) -> Result<RuntimeRecord> {
        self.lock_registry()?.runtime_register(registration)
    }

    fn compare_and_set(&self, update: RuntimeCasUpdate) -> Result<RuntimeRecord> {
        self.lock_registry()?.runtime_compare_and_set(update)
    }

    fn remove(
        &self,
        object_id: RuntimeObjectId,
        expected_boot_id: SessionId,
        expected_revision: u64,
    ) -> Result<()> {
        self.lock_registry()?
            .runtime_remove(object_id, expected_boot_id, expected_revision)
    }

    fn get(&self, object_id: RuntimeObjectId) -> Result<Option<RuntimeRecord>> {
        Ok(self
            .lock_registry()?
            .runtime_state()
            .get(object_id)
            .cloned())
    }

    fn snapshot(&self) -> Result<RuntimeSnapshot> {
        self.lock_registry()?.runtime_state().snapshot()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeStateStore {
    boot_id: SessionId,
    revision: u64,
    records: BTreeMap<RuntimeObjectId, RuntimeRecord>,
    control_processes: BTreeMap<String, RuntimeObjectId>,
}

impl Default for RuntimeStateStore {
    fn default() -> Self {
        Self::new(generate_boot_id())
    }
}

impl RuntimeStateStore {
    fn new(boot_id: SessionId) -> Self {
        Self {
            boot_id,
            revision: 0,
            records: BTreeMap::new(),
            control_processes: BTreeMap::new(),
        }
    }

    pub(crate) const fn boot_id(&self) -> SessionId {
        self.boot_id
    }

    pub(crate) fn get(&self, object_id: RuntimeObjectId) -> Option<&RuntimeRecord> {
        self.records.get(&object_id)
    }

    pub(crate) fn snapshot(&self) -> Result<RuntimeSnapshot> {
        RuntimeSnapshot::new(
            self.boot_id,
            self.revision,
            self.records.values().cloned().collect(),
        )
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("control runtime snapshot invariant failed: {error}"),
            )
        })
    }

    pub(crate) fn register(&mut self, registration: RuntimeRegistration) -> Result<RuntimeRecord> {
        let object_id = registration.object_id();
        if has_control_id_prefix(object_id) {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "runtime object id uses the reserved process registry namespace",
            ));
        }
        self.insert_registration(registration)
    }

    fn insert_registration(&mut self, registration: RuntimeRegistration) -> Result<RuntimeRecord> {
        let object_id = registration.object_id();
        if self.records.contains_key(&object_id) {
            return Err(ServerError::new(
                ErrorCode::AlreadyExists,
                format!("runtime object already exists object_id=[{object_id:?}]"),
            ));
        }
        let revision = self.next_revision()?;
        let record = RuntimeRecord {
            object_id,
            boot_id: self.boot_id,
            state: registration.state(),
            revision,
            updated_at_ns: now_ns(),
            attributes: registration.attributes().clone(),
            metrics: registration.metrics().clone(),
        };
        self.records.insert(object_id, record.clone());
        Ok(record)
    }

    pub(crate) fn compare_and_set(&mut self, update: RuntimeCasUpdate) -> Result<RuntimeRecord> {
        if has_control_id_prefix(update.object_id()) {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "control-owned runtime records must be mutated through the authenticated control path",
            ));
        }
        if update.expected_boot_id() != self.boot_id {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "runtime compare-and-set boot id does not match current server boot",
            ));
        }
        let current = self
            .records
            .get(&update.object_id())
            .ok_or_else(|| ServerError::new(ErrorCode::NotFound, "runtime object was not found"))?;
        if current.revision != update.expected_revision()
            || current.state != update.expected_state()
        {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                format!(
                    "runtime compare-and-set conflict expected_revision=[{}] actual_revision=[{}] expected_state=[{:?}] actual_state=[{:?}]",
                    update.expected_revision(),
                    current.revision,
                    update.expected_state(),
                    current.state
                ),
            ));
        }
        let revision = self.next_revision()?;
        let current = self
            .records
            .get_mut(&update.object_id())
            .expect("runtime record checked above");
        current.state = update.next_state();
        current.revision = revision;
        current.updated_at_ns = current.updated_at_ns.saturating_add(1).max(now_ns());
        current.attributes = update.attributes().clone();
        current.metrics = update.metrics().clone();
        Ok(current.clone())
    }

    pub(crate) fn remove(
        &mut self,
        object_id: RuntimeObjectId,
        expected_boot_id: SessionId,
        expected_revision: u64,
    ) -> Result<()> {
        if expected_boot_id != self.boot_id {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "runtime remove boot id does not match current server boot",
            ));
        }
        if has_control_id_prefix(object_id) {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "control-owned runtime records must be removed through the authenticated control path",
            ));
        }
        let current = self
            .records
            .get(&object_id)
            .ok_or_else(|| ServerError::new(ErrorCode::NotFound, "runtime object was not found"))?;
        if current.revision != expected_revision {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                format!(
                    "runtime remove revision conflict expected=[{}] actual=[{}]",
                    expected_revision, current.revision
                ),
            ));
        }
        self.next_revision()?;
        self.remove_unchecked(object_id);
        Ok(())
    }

    pub(crate) fn observe_process_registered(&mut self, process: &ProcessRecord) {
        self.remove_control_process(&process.process_id);
        let key = process.process_id.as_str();
        let object_id = self.next_control_object_id(key);
        let attributes = process_attributes(process);
        self.insert_control_record(object_id, attributes, BTreeMap::new())
            .expect("control process runtime registration must have revision capacity");
        self.transition_record(object_id, RuntimeState::Running, None, None)
            .expect("registered process can transition to running");
        self.control_processes
            .insert(process.process_id.clone(), object_id);
    }

    pub(crate) fn observe_process_heartbeat(&mut self, process: &ProcessRecord) {
        let Some(object_id) = self.control_processes.get(&process.process_id).copied() else {
            self.observe_process_registered(process);
            return;
        };
        self.transition_record(
            object_id,
            RuntimeState::Running,
            Some(process_attributes(process)),
            None,
        )
        .expect("control heartbeat transition must be valid");
    }

    pub(crate) fn observe_process_failed(&mut self, process: &ProcessRecord) {
        if let Some(object_id) = self.control_processes.get(&process.process_id).copied() {
            let already_failed = self
                .records
                .get(&object_id)
                .is_some_and(|record| record.state == RuntimeState::Failed);
            if !already_failed {
                self.transition_record(
                    object_id,
                    RuntimeState::Failed,
                    Some(process_attributes(process)),
                    None,
                )
                .expect("live process can transition to failed");
            }
        }
    }

    pub(crate) fn remove_control_process(&mut self, process_id: &str) {
        if let Some(object_id) = self.control_processes.remove(process_id) {
            self.discard_control_record(object_id);
        }
    }

    fn transition_record(
        &mut self,
        object_id: RuntimeObjectId,
        target: RuntimeState,
        mut attributes: Option<BTreeMap<String, String>>,
        mut metrics: Option<BTreeMap<String, RuntimeMetricValue>>,
    ) -> Result<RuntimeRecord> {
        let current_state = self
            .records
            .get(&object_id)
            .map(|record| record.state)
            .ok_or_else(|| {
                ServerError::new(ErrorCode::NotFound, "control runtime object was not found")
            })?;
        let path = transition_path(current_state, target).ok_or_else(|| {
            ServerError::new(
                ErrorCode::Conflict,
                format!(
                    "control runtime transition requires a new instance current=[{:?}] target=[{:?}]",
                    current_state, target
                ),
            )
        })?;
        let last_step = path.len() - 1;
        for (index, next_state) in path.into_iter().enumerate() {
            let revision = self.next_revision()?;
            let record = self
                .records
                .get_mut(&object_id)
                .expect("control runtime record exists for transition");
            record.state = next_state;
            record.revision = revision;
            record.updated_at_ns = record.updated_at_ns.saturating_add(1).max(now_ns());
            if index == last_step {
                if let Some(attributes) = attributes.take() {
                    record.attributes = attributes;
                }
                if let Some(metrics) = metrics.take() {
                    record.metrics = metrics;
                }
            }
        }
        Ok(self.records[&object_id].clone())
    }

    fn insert_control_record(
        &mut self,
        object_id: RuntimeObjectId,
        attributes: BTreeMap<String, String>,
        metrics: BTreeMap<String, RuntimeMetricValue>,
    ) -> Result<RuntimeRecord> {
        let registration =
            RuntimeRegistration::new(object_id, RuntimeState::Registered, attributes, metrics)
                .expect("registered is the sole legal initial runtime state");
        self.insert_registration(registration)
    }

    fn next_control_object_id(&mut self, key: &str) -> RuntimeObjectId {
        loop {
            let nonce = self.revision.saturating_add(1);
            let object_id = control_object_id(self.boot_id, key, nonce);
            if !self.records.contains_key(&object_id) {
                return object_id;
            }
            self.revision = nonce;
        }
    }

    fn next_revision(&mut self) -> Result<u64> {
        let revision = self.revision.checked_add(1).ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "runtime registry revision space is exhausted",
            )
        })?;
        self.revision = revision;
        Ok(revision)
    }

    fn remove_unchecked(&mut self, object_id: RuntimeObjectId) {
        self.records.remove(&object_id);
        self.control_processes.retain(|_, id| *id != object_id);
    }

    fn discard_control_record(&mut self, object_id: RuntimeObjectId) {
        if self.records.contains_key(&object_id) {
            self.next_revision()
                .expect("control runtime revision space must remain available");
            self.remove_unchecked(object_id);
        }
    }
}

fn control_object_id(boot_id: SessionId, key: &str, nonce: u64) -> RuntimeObjectId {
    let mut digest = Sha256::new();
    digest.update(PROCESS_RUNTIME_ID_SCHEME);
    digest.update(b"process");
    digest.update(boot_id.into_bytes());
    digest.update(key.as_bytes());
    digest.update(nonce.to_be_bytes());
    let hash = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes[..4].copy_from_slice(&PROCESS_RUNTIME_ID_PREFIX);
    bytes[4..].copy_from_slice(&hash[..12]);
    RuntimeObjectId::Process(ProcessId::from_bytes(bytes).expect("control id prefix is nonzero"))
}

fn has_control_id_prefix(object_id: RuntimeObjectId) -> bool {
    let bytes = match object_id {
        RuntimeObjectId::Session(id) => id.into_bytes(),
        RuntimeObjectId::Connection(id) => id.into_bytes(),
        RuntimeObjectId::Process(id) => id.into_bytes(),
        RuntimeObjectId::Source(id) => id.into_bytes(),
        RuntimeObjectId::Engine(id) => id.into_bytes(),
    };
    bytes[..4] == PROCESS_RUNTIME_ID_PREFIX
}

fn transition_path(current: RuntimeState, target: RuntimeState) -> Option<Vec<RuntimeState>> {
    if current == target {
        return match current {
            RuntimeState::Registered
            | RuntimeState::Running
            | RuntimeState::Degraded
            | RuntimeState::Stopping => Some(vec![target]),
            RuntimeState::Stopped | RuntimeState::Failed => None,
        };
    }
    if current.can_transition_to(target) {
        return Some(vec![target]);
    }
    match (current, target) {
        (RuntimeState::Registered, RuntimeState::Degraded) => {
            Some(vec![RuntimeState::Running, RuntimeState::Degraded])
        }
        (
            RuntimeState::Registered | RuntimeState::Running | RuntimeState::Degraded,
            RuntimeState::Stopped,
        ) => Some(vec![RuntimeState::Stopping, RuntimeState::Stopped]),
        _ => None,
    }
}

fn process_attributes(process: &ProcessRecord) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app".to_owned(), process.app.clone()),
        ("lease_status".to_owned(), process.lease_status.clone()),
        ("control_process_id".to_owned(), process.process_id.clone()),
    ])
}

fn generate_boot_id() -> SessionId {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).expect("os randomness is required for runtime boot id");
    if bytes == [0; 16] {
        bytes[15] = 1;
    }
    SessionId::from_bytes(bytes).expect("boot id generation rejects nil")
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().try_into().unwrap_or(u64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use zippy_types::runtime::{
        RuntimeCasUpdate, RuntimeObjectId, RuntimeRegistration, RuntimeRegistry, RuntimeState,
    };
    use zippy_types::{ErrorCode, SessionId, SourceId};

    use super::{ProcessRuntimeRegistry, RuntimeStateStore};
    use crate::registry::Registry;

    fn test_registry() -> Registry {
        Registry::with_runtime_state(RuntimeStateStore::new(SessionId::from_u128(1).unwrap()))
    }

    #[test]
    fn stable_contract_enforces_boot_revision_state_and_aba() {
        let mut registry = test_registry();
        let object_id = RuntimeObjectId::Source(SourceId::from_u128(7).unwrap());
        let registered = registry
            .runtime_register(
                RuntimeRegistration::new(
                    object_id,
                    RuntimeState::Registered,
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .unwrap(),
            )
            .unwrap();
        let running = registry
            .runtime_compare_and_set(
                RuntimeCasUpdate::new(
                    object_id,
                    registered.boot_id,
                    registered.revision,
                    RuntimeState::Registered,
                    RuntimeState::Running,
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            registry
                .runtime_remove(
                    object_id,
                    SessionId::from_u128(2).unwrap(),
                    running.revision,
                )
                .unwrap_err()
                .code(),
            ErrorCode::Conflict
        );
        let stale = registry.runtime_compare_and_set(
            RuntimeCasUpdate::new(
                object_id,
                registered.boot_id,
                registered.revision,
                RuntimeState::Registered,
                RuntimeState::Running,
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .unwrap(),
        );
        assert_eq!(stale.unwrap_err().code(), ErrorCode::Conflict);

        registry
            .runtime_remove(object_id, running.boot_id, running.revision)
            .unwrap();
        let replacement = registry
            .runtime_register(
                RuntimeRegistration::new(
                    object_id,
                    RuntimeState::Registered,
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(replacement.revision > running.revision);
        assert_eq!(
            registry
                .runtime_remove(object_id, running.boot_id, running.revision)
                .unwrap_err()
                .code(),
            ErrorCode::Conflict
        );
    }

    #[test]
    fn adapter_boot_id_survives_clone_without_snapshot_persistence() {
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};

        let registry = Arc::new(Mutex::new(test_registry()));
        let adapter =
            ProcessRuntimeRegistry::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));
        let clone = adapter.clone();
        assert_eq!(adapter.boot_id(), clone.boot_id());
        assert_eq!(adapter.snapshot().unwrap().revision(), 0);
    }

    #[test]
    fn heartbeat_advances_one_revision_without_rebuilding_the_registry() {
        let mut registry = test_registry();
        let process_id = registry.register_process("worker").unwrap();
        let before = registry.runtime_state().snapshot().unwrap();
        registry.record_heartbeat(&process_id).unwrap();
        let after = registry.runtime_state().snapshot().unwrap();
        assert_eq!(after.revision(), before.revision() + 1);
        assert_eq!(after.records().len(), before.records().len());
        assert_eq!(after.records()[0].state, RuntimeState::Running);
    }

    #[test]
    fn process_expiry_does_not_mutate_generic_records_with_spoofed_attributes() {
        let mut registry = test_registry();
        let process_id = registry.register_process("worker").unwrap();
        let object_id = RuntimeObjectId::Source(SourceId::from_u128(77).unwrap());
        let generic = registry
            .runtime_register(
                RuntimeRegistration::new(
                    object_id,
                    RuntimeState::Registered,
                    BTreeMap::from([("control_process_id".to_owned(), process_id.clone())]),
                    BTreeMap::new(),
                )
                .unwrap(),
            )
            .unwrap();
        registry.force_expire_process(&process_id).unwrap();
        assert_eq!(registry.runtime_state().get(object_id).unwrap(), &generic);
    }
}
