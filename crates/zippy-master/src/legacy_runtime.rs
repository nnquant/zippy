//! Boot-scoped runtime state hosted inside the legacy master registry lock.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use zippy_types::runtime::{
    FiniteMetricValue, RuntimeCasUpdate, RuntimeMetricValue, RuntimeObjectId, RuntimeRecord,
    RuntimeRegistration, RuntimeRegistry, RuntimeSnapshot, RuntimeState,
};
use zippy_types::{
    ConnectionId, EngineId, ErrorCode, ProcessId, Result, ServerError, SessionId, SourceId,
};

use crate::registry::{EngineRecord, ProcessRecord, Registry, SourceRecord};

const LEGACY_RUNTIME_ID_SCHEME: &[u8] = b"zippy.legacy_runtime.id.v1";
const LEGACY_RUNTIME_ID_PREFIX: [u8; 4] = *b"ZLR1";
static FALLBACK_BOOT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const ADMISSION_INACTIVE: u8 = 0;
const ADMISSION_ACTIVE: u8 = 1;
const ADMISSION_SHUTDOWN: u8 = 2;

/// Compatibility implementation of the stable runtime registry contract.
///
/// The state itself is embedded in [`Registry`]. Adapter clones therefore use
/// the same mutex and cannot diverge from legacy process/source/engine updates.
#[derive(Clone)]
pub struct LegacyRuntimeRegistryAdapter {
    registry: Arc<Mutex<Registry>>,
    running: Arc<AtomicBool>,
    admission: Arc<AtomicU8>,
}

impl LegacyRuntimeRegistryAdapter {
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
                "legacy runtime registry lock is poisoned",
            )
        })?;
        self.ensure_admission_open()?;
        if !self.running.load(Ordering::Acquire) {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy runtime registry master lifecycle is stopped",
            ));
        }
        Ok(registry)
    }

    fn ensure_admission_open(&self) -> Result<()> {
        match self.admission.load(Ordering::Acquire) {
            ADMISSION_ACTIVE => Ok(()),
            ADMISSION_INACTIVE => Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy runtime registry admission is not active",
            )),
            ADMISSION_SHUTDOWN => Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy runtime registry admission is closed",
            )),
            _ => Err(ServerError::new(
                ErrorCode::Internal,
                "legacy runtime registry admission state is invalid",
            )),
        }
    }

    /// Opens an inactive composition adapter exactly once.
    pub fn activate(&self) -> Result<()> {
        let _registry = self.registry.lock().map_err(|_| {
            ServerError::new(
                ErrorCode::Internal,
                "legacy runtime registry lock is poisoned",
            )
        })?;
        if !self.running.load(Ordering::Acquire) {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy runtime registry master lifecycle is stopped",
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
                "legacy runtime registry admission is permanently closed",
            )),
            Err(_) => Err(ServerError::new(
                ErrorCode::Internal,
                "legacy runtime registry admission state is invalid",
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

impl RuntimeRegistry for LegacyRuntimeRegistryAdapter {
    fn boot_id(&self) -> SessionId {
        self.registry
            .lock()
            .map(|registry| registry.runtime_state().boot_id())
            .unwrap_or_else(|poisoned| poisoned.into_inner().runtime_state().boot_id())
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LegacyConnectionKey {
    stream_name: String,
    reader_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LegacyRuntimeState {
    boot_id: SessionId,
    revision: u64,
    records: BTreeMap<RuntimeObjectId, RuntimeRecord>,
    legacy_processes: BTreeMap<String, RuntimeObjectId>,
    legacy_sources: BTreeMap<String, RuntimeObjectId>,
    legacy_engines: BTreeMap<String, RuntimeObjectId>,
    legacy_connections: BTreeMap<LegacyConnectionKey, RuntimeObjectId>,
}

impl Default for LegacyRuntimeState {
    fn default() -> Self {
        Self::new(generate_boot_id())
    }
}

impl LegacyRuntimeState {
    #[cfg(test)]
    pub(crate) fn new_for_test(boot_id: SessionId) -> Self {
        Self::new(boot_id)
    }

    fn new(boot_id: SessionId) -> Self {
        Self {
            boot_id,
            revision: 0,
            records: BTreeMap::new(),
            legacy_processes: BTreeMap::new(),
            legacy_sources: BTreeMap::new(),
            legacy_engines: BTreeMap::new(),
            legacy_connections: BTreeMap::new(),
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
                format!("legacy runtime snapshot invariant failed: {error}"),
            )
        })
    }

    pub(crate) fn clear_records_for_restart(&mut self) {
        self.revision = 0;
        self.records.clear();
        self.legacy_processes.clear();
        self.legacy_sources.clear();
        self.legacy_engines.clear();
        self.legacy_connections.clear();
    }

    pub(crate) fn register(&mut self, registration: RuntimeRegistration) -> Result<RuntimeRecord> {
        let object_id = registration.object_id();
        if has_legacy_id_prefix(object_id) {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "runtime object id uses the reserved legacy adapter namespace",
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
        if has_legacy_id_prefix(update.object_id()) {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "legacy-owned runtime records must be mutated through the authenticated control path",
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
        if has_legacy_id_prefix(object_id) {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "legacy-owned runtime records must be removed through the authenticated control path",
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
        self.remove_legacy_process(&process.process_id);
        let key = process.process_id.as_str();
        let object_id = self.next_legacy_object_id(LegacyDomain::Process, key);
        let attributes = process_attributes(process);
        let record = self
            .insert_legacy_record(object_id, attributes, BTreeMap::new())
            .expect("legacy process runtime registration must have revision capacity");
        let running = self
            .transition_record(record.object_id, RuntimeState::Running, None, None)
            .expect("registered process can transition to running");
        self.legacy_processes
            .insert(process.process_id.clone(), running.object_id);
    }

    pub(crate) fn observe_process_heartbeat(&mut self, process: &ProcessRecord) {
        let Some(object_id) = self.legacy_processes.get(&process.process_id).copied() else {
            self.observe_process_registered(process);
            return;
        };
        self.transition_record(
            object_id,
            RuntimeState::Running,
            Some(process_attributes(process)),
            None,
        )
        .expect("legacy heartbeat transition must be valid");
    }

    pub(crate) fn observe_process_failed(&mut self, process: &ProcessRecord) {
        if let Some(object_id) = self.legacy_processes.get(&process.process_id).copied() {
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
        self.fail_owned_records(&process.process_id);
        self.remove_connections_for_process(&process.process_id);
    }

    pub(crate) fn observe_process_owner_failed(&mut self, process_id: &str) {
        self.fail_owned_records(process_id);
        self.remove_connections_for_process(process_id);
    }

    pub(crate) fn remove_legacy_process(&mut self, process_id: &str) {
        if let Some(object_id) = self.legacy_processes.remove(process_id) {
            self.discard_legacy_record(object_id);
        }
        self.remove_connections_for_process(process_id);
        self.remove_owned_records(process_id);
    }

    pub(crate) fn observe_source_registered(&mut self, source: &SourceRecord) {
        self.remove_legacy_source(&source.source_name);
        let object_id = self.next_legacy_object_id(LegacyDomain::Source, &source.source_name);
        let record = self
            .insert_legacy_record(
                object_id,
                source_attributes(source),
                normalize_metrics(&source.metrics),
            )
            .expect("legacy source runtime registration must have revision capacity");
        self.legacy_sources
            .insert(source.source_name.clone(), record.object_id);
    }

    pub(crate) fn observe_source_status(&mut self, source: &SourceRecord) {
        let target = legacy_status_to_runtime_state(&source.status);
        let Some(object_id) = self.legacy_sources.get(&source.source_name).copied() else {
            self.observe_source_registered(source);
            let object_id = self.legacy_sources[&source.source_name];
            self.transition_record(
                object_id,
                target,
                Some(source_attributes(source)),
                Some(normalize_metrics(&source.metrics)),
            )
            .expect("new source transition must be valid");
            return;
        };
        if self
            .transition_record(
                object_id,
                target,
                Some(source_attributes(source)),
                Some(normalize_metrics(&source.metrics)),
            )
            .is_err()
        {
            self.observe_source_registered(source);
            let object_id = self.legacy_sources[&source.source_name];
            self.transition_record(
                object_id,
                target,
                Some(source_attributes(source)),
                Some(normalize_metrics(&source.metrics)),
            )
            .expect("replacement source transition must be valid");
        }
    }

    pub(crate) fn remove_legacy_source(&mut self, source_name: &str) {
        if let Some(object_id) = self.legacy_sources.remove(source_name) {
            self.discard_legacy_record(object_id);
        }
    }

    pub(crate) fn observe_engine_registered(&mut self, engine: &EngineRecord) {
        self.remove_legacy_engine(&engine.engine_name);
        let object_id = self.next_legacy_object_id(LegacyDomain::Engine, &engine.engine_name);
        let record = self
            .insert_legacy_record(
                object_id,
                engine_attributes(engine),
                normalize_metrics(&engine.metrics),
            )
            .expect("legacy engine runtime registration must have revision capacity");
        self.legacy_engines
            .insert(engine.engine_name.clone(), record.object_id);
    }

    pub(crate) fn observe_engine_status(&mut self, engine: &EngineRecord) {
        let target = legacy_status_to_runtime_state(&engine.status);
        let Some(object_id) = self.legacy_engines.get(&engine.engine_name).copied() else {
            self.observe_engine_registered(engine);
            let object_id = self.legacy_engines[&engine.engine_name];
            self.transition_record(
                object_id,
                target,
                Some(engine_attributes(engine)),
                Some(normalize_metrics(&engine.metrics)),
            )
            .expect("new engine transition must be valid");
            return;
        };
        if self
            .transition_record(
                object_id,
                target,
                Some(engine_attributes(engine)),
                Some(normalize_metrics(&engine.metrics)),
            )
            .is_err()
        {
            self.observe_engine_registered(engine);
            let object_id = self.legacy_engines[&engine.engine_name];
            self.transition_record(
                object_id,
                target,
                Some(engine_attributes(engine)),
                Some(normalize_metrics(&engine.metrics)),
            )
            .expect("replacement engine transition must be valid");
        }
    }

    pub(crate) fn remove_legacy_engine(&mut self, engine_name: &str) {
        if let Some(object_id) = self.legacy_engines.remove(engine_name) {
            self.discard_legacy_record(object_id);
        }
    }

    pub(crate) fn observe_connection_attached(
        &mut self,
        stream_name: &str,
        process_id: &str,
        reader_id: &str,
    ) {
        let key = LegacyConnectionKey {
            stream_name: stream_name.to_owned(),
            reader_id: reader_id.to_owned(),
        };
        if let Some(previous) = self.legacy_connections.remove(&key) {
            self.discard_legacy_record(previous);
        }
        let identity = format!("{stream_name}\0{process_id}\0{reader_id}");
        let object_id = self.next_legacy_object_id(LegacyDomain::Connection, &identity);
        let attributes = BTreeMap::from([
            ("connection_kind".to_owned(), "subscription".to_owned()),
            ("legacy_reader_id".to_owned(), reader_id.to_owned()),
            ("legacy_process_id".to_owned(), process_id.to_owned()),
            ("stream_name".to_owned(), stream_name.to_owned()),
        ]);
        let record = self
            .insert_legacy_record(object_id, attributes, BTreeMap::new())
            .expect("legacy connection registration must have revision capacity");
        let running = self
            .transition_record(record.object_id, RuntimeState::Running, None, None)
            .expect("registered connection can transition to running");
        self.legacy_connections.insert(key, running.object_id);
    }

    pub(crate) fn remove_legacy_connection(&mut self, stream_name: &str, reader_id: &str) {
        let keys = self
            .legacy_connections
            .keys()
            .filter(|key| key.stream_name == stream_name && key.reader_id == reader_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(object_id) = self.legacy_connections.remove(&key) {
                self.discard_legacy_record(object_id);
            }
        }
    }

    pub(crate) fn remove_connections_for_stream(&mut self, stream_name: &str) {
        let keys = self
            .legacy_connections
            .keys()
            .filter(|key| key.stream_name == stream_name)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(object_id) = self.legacy_connections.remove(&key) {
                self.discard_legacy_record(object_id);
            }
        }
    }

    fn fail_owned_records(&mut self, process_id: &str) {
        let object_ids = self
            .records
            .values()
            .filter(|record| {
                record
                    .attributes
                    .get("legacy_process_id")
                    .is_some_and(|owner| owner == process_id)
                    && has_legacy_id_prefix(record.object_id)
                    && !matches!(record.object_id, RuntimeObjectId::Process(_))
            })
            .map(|record| record.object_id)
            .collect::<Vec<_>>();
        for object_id in object_ids {
            let _ = self.transition_record(object_id, RuntimeState::Failed, None, None);
        }
    }

    fn remove_owned_records(&mut self, process_id: &str) {
        let source_names = self
            .legacy_sources
            .iter()
            .filter(|(_, object_id)| self.record_owned_by(**object_id, process_id))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in source_names {
            self.remove_legacy_source(&name);
        }
        let engine_names = self
            .legacy_engines
            .iter()
            .filter(|(_, object_id)| self.record_owned_by(**object_id, process_id))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in engine_names {
            self.remove_legacy_engine(&name);
        }
    }

    fn remove_connections_for_process(&mut self, process_id: &str) {
        let keys = self
            .legacy_connections
            .iter()
            .filter(|(_, object_id)| self.record_owned_by(**object_id, process_id))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(object_id) = self.legacy_connections.remove(&key) {
                self.discard_legacy_record(object_id);
            }
        }
    }

    fn record_owned_by(&self, object_id: RuntimeObjectId, process_id: &str) -> bool {
        self.records
            .get(&object_id)
            .and_then(|record| record.attributes.get("legacy_process_id"))
            .is_some_and(|owner| owner == process_id)
    }

    fn transition_record(
        &mut self,
        object_id: RuntimeObjectId,
        target: RuntimeState,
        attributes: Option<BTreeMap<String, String>>,
        metrics: Option<BTreeMap<String, RuntimeMetricValue>>,
    ) -> Result<RuntimeRecord> {
        let current = self.records.get(&object_id).cloned().ok_or_else(|| {
            ServerError::new(ErrorCode::NotFound, "legacy runtime object was not found")
        })?;
        let path = transition_path(current.state, target).ok_or_else(|| {
            ServerError::new(
                ErrorCode::Conflict,
                format!(
                    "legacy runtime transition requires a new instance current=[{:?}] target=[{:?}]",
                    current.state, target
                ),
            )
        })?;
        if path.is_empty() {
            return Ok(current);
        }
        let mut result = current;
        let step_count = path.len();
        for (index, next_state) in path.into_iter().enumerate() {
            let revision = self.next_revision()?;
            let record = self
                .records
                .get_mut(&object_id)
                .expect("legacy runtime record exists for transition");
            record.state = next_state;
            record.revision = revision;
            record.updated_at_ns = record.updated_at_ns.saturating_add(1).max(now_ns());
            if index + 1 == step_count {
                if let Some(attributes) = attributes.clone() {
                    record.attributes = attributes;
                }
                if let Some(metrics) = metrics.clone() {
                    record.metrics = metrics;
                }
            }
            result = record.clone();
        }
        Ok(result)
    }

    fn insert_legacy_record(
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

    fn next_legacy_object_id(&mut self, domain: LegacyDomain, key: &str) -> RuntimeObjectId {
        loop {
            let nonce = self.revision.saturating_add(1);
            let object_id = legacy_object_id(domain, self.boot_id, key, nonce);
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
        self.legacy_processes.retain(|_, id| *id != object_id);
        self.legacy_sources.retain(|_, id| *id != object_id);
        self.legacy_engines.retain(|_, id| *id != object_id);
        self.legacy_connections.retain(|_, id| *id != object_id);
    }

    fn discard_legacy_record(&mut self, object_id: RuntimeObjectId) {
        if self.records.contains_key(&object_id) {
            self.next_revision()
                .expect("legacy runtime revision space must remain available");
            self.remove_unchecked(object_id);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LegacyDomain {
    Connection,
    Process,
    Source,
    Engine,
}

fn legacy_object_id(
    domain: LegacyDomain,
    boot_id: SessionId,
    key: &str,
    nonce: u64,
) -> RuntimeObjectId {
    let domain_bytes: &[u8] = match domain {
        LegacyDomain::Connection => b"connection",
        LegacyDomain::Process => b"process",
        LegacyDomain::Source => b"source",
        LegacyDomain::Engine => b"engine",
    };
    let mut digest = Sha256::new();
    digest.update(LEGACY_RUNTIME_ID_SCHEME);
    digest.update(domain_bytes);
    digest.update(boot_id.into_bytes());
    digest.update(key.as_bytes());
    digest.update(nonce.to_be_bytes());
    let hash = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes[..4].copy_from_slice(&LEGACY_RUNTIME_ID_PREFIX);
    bytes[4..].copy_from_slice(&hash[..12]);
    match domain {
        LegacyDomain::Connection => RuntimeObjectId::Connection(
            ConnectionId::from_bytes(bytes).expect("legacy id prefix is nonzero"),
        ),
        LegacyDomain::Process => RuntimeObjectId::Process(
            ProcessId::from_bytes(bytes).expect("legacy id prefix is nonzero"),
        ),
        LegacyDomain::Source => RuntimeObjectId::Source(
            SourceId::from_bytes(bytes).expect("legacy id prefix is nonzero"),
        ),
        LegacyDomain::Engine => RuntimeObjectId::Engine(
            EngineId::from_bytes(bytes).expect("legacy id prefix is nonzero"),
        ),
    }
}

fn has_legacy_id_prefix(object_id: RuntimeObjectId) -> bool {
    let bytes = match object_id {
        RuntimeObjectId::Session(id) => id.into_bytes(),
        RuntimeObjectId::Connection(id) => id.into_bytes(),
        RuntimeObjectId::Process(id) => id.into_bytes(),
        RuntimeObjectId::Source(id) => id.into_bytes(),
        RuntimeObjectId::Engine(id) => id.into_bytes(),
    };
    bytes[..4] == LEGACY_RUNTIME_ID_PREFIX
}

fn transition_path(current: RuntimeState, target: RuntimeState) -> Option<Vec<RuntimeState>> {
    if current == target {
        return match current {
            RuntimeState::Registered | RuntimeState::Running | RuntimeState::Degraded => {
                Some(vec![target])
            }
            RuntimeState::Stopping => Some(vec![target]),
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

fn legacy_status_to_runtime_state(status: &str) -> RuntimeState {
    match status.trim().to_ascii_lowercase().as_str() {
        "registered" | "starting" | "defined" => RuntimeState::Registered,
        "alive" | "running" | "ready" | "active" | "writer_attached" | "reader_attached" => {
            RuntimeState::Running
        }
        "degraded" | "stale" => RuntimeState::Degraded,
        "stopping" | "draining" => RuntimeState::Stopping,
        "stopped" | "closed" => RuntimeState::Stopped,
        "failed" | "error" | "lost" | "expired" | "restored" => RuntimeState::Failed,
        _ => RuntimeState::Failed,
    }
}

fn process_attributes(process: &ProcessRecord) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app".to_owned(), process.app.clone()),
        ("lease_status".to_owned(), process.lease_status.clone()),
        ("legacy_process_id".to_owned(), process.process_id.clone()),
    ])
}

fn source_attributes(source: &SourceRecord) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("legacy_process_id".to_owned(), source.process_id.clone()),
        ("legacy_source_name".to_owned(), source.source_name.clone()),
        ("output_stream".to_owned(), source.output_stream.clone()),
        ("source_type".to_owned(), source.source_type.clone()),
        ("writer_epoch".to_owned(), source.writer_epoch.to_string()),
    ])
}

fn engine_attributes(engine: &EngineRecord) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("engine_type".to_owned(), engine.engine_type.clone()),
        ("input_stream".to_owned(), engine.input_stream.clone()),
        ("legacy_engine_name".to_owned(), engine.engine_name.clone()),
        ("legacy_process_id".to_owned(), engine.process_id.clone()),
        ("output_stream".to_owned(), engine.output_stream.clone()),
    ])
}

fn normalize_metrics(metrics: &serde_json::Value) -> BTreeMap<String, RuntimeMetricValue> {
    let Some(metrics) = metrics.as_object() else {
        return BTreeMap::new();
    };
    metrics
        .iter()
        .filter_map(|(name, value)| {
            if let Some(value) = value.as_u64() {
                return Some((name.clone(), RuntimeMetricValue::Counter(value)));
            }
            value.as_f64().and_then(|value| {
                FiniteMetricValue::new(value)
                    .ok()
                    .map(RuntimeMetricValue::Gauge)
                    .map(|value| (name.clone(), value))
            })
        })
        .collect()
}

fn generate_boot_id() -> SessionId {
    let mut bytes = [0_u8; 16];
    if File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .is_err()
    {
        let sequence = FALLBACK_BOOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let now = now_ns();
        bytes[..8].copy_from_slice(&now.to_be_bytes());
        bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    }
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
        RuntimeCasUpdate, RuntimeMetricValue, RuntimeObjectId, RuntimeRegistration,
        RuntimeRegistry, RuntimeState,
    };
    use zippy_types::{ErrorCode, SessionId, SourceId};

    use super::{LegacyRuntimeRegistryAdapter, LegacyRuntimeState};
    use crate::registry::Registry;

    fn test_registry() -> Registry {
        Registry::with_runtime_state(LegacyRuntimeState::new_for_test(
            SessionId::from_u128(1).unwrap(),
        ))
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
        let adapter = LegacyRuntimeRegistryAdapter::new(
            Arc::clone(&registry),
            Arc::new(AtomicBool::new(true)),
        );
        let clone = adapter.clone();
        assert_eq!(adapter.boot_id(), clone.boot_id());
        assert_eq!(adapter.snapshot().unwrap().revision(), 0);
    }

    #[test]
    fn legacy_lifecycle_is_projected_atomically_with_typed_attributes() {
        let mut registry = test_registry();
        let process_id = registry.register_process("market_data");
        registry
            .register_stream(
                "ticks",
                serde_json::json!({"fields": []}),
                "schema-v1",
                1024,
                256,
            )
            .unwrap();
        registry
            .register_source(
                "ctp",
                "openctp",
                &process_id,
                "ticks",
                serde_json::json!({}),
            )
            .unwrap();
        registry
            .register_engine(
                "factors",
                "reactive",
                &process_id,
                "ticks",
                "factor_values",
                serde_json::json!({}),
            )
            .unwrap();
        registry
            .attach_reader("ticks", &process_id, "reader-1")
            .unwrap();
        registry
            .set_source_status(
                "ctp",
                "running",
                Some(serde_json::json!({
                    "rows_total": 3,
                    "queue_delay_ms": -1.5,
                    "label": "ignored",
                    "nested": {"ignored": 1}
                })),
            )
            .unwrap();
        registry
            .set_engine_status(
                "factors",
                "unexpected_legacy_status",
                Some(serde_json::json!({"callbacks_total": 5})),
            )
            .unwrap();

        let snapshot = registry.runtime_state().snapshot().unwrap();
        assert_eq!(snapshot.boot_id(), SessionId::from_u128(1).unwrap());
        assert_eq!(snapshot.records().len(), 4);
        assert!(snapshot
            .records()
            .windows(2)
            .all(|pair| pair[0].object_id < pair[1].object_id));
        let source = snapshot
            .records()
            .iter()
            .find(|record| {
                record
                    .attributes
                    .get("legacy_source_name")
                    .map(String::as_str)
                    == Some("ctp")
            })
            .unwrap();
        assert_eq!(source.state, RuntimeState::Running);
        assert_eq!(source.attributes["source_type"], "openctp");
        assert_eq!(source.attributes["legacy_process_id"], process_id);
        assert_eq!(source.attributes["output_stream"], "ticks");
        assert_eq!(source.metrics.len(), 2);
        assert_eq!(source.metrics["rows_total"], RuntimeMetricValue::Counter(3));
        assert!(!source.metrics.contains_key("label"));
        assert!(!source.metrics.contains_key("nested"));
        let engine = snapshot
            .records()
            .iter()
            .find(|record| {
                record
                    .attributes
                    .get("legacy_engine_name")
                    .map(String::as_str)
                    == Some("factors")
            })
            .unwrap();
        assert_eq!(engine.state, RuntimeState::Failed);

        let unauthorized = RuntimeCasUpdate::new(
            source.object_id,
            source.boot_id,
            source.revision,
            source.state,
            RuntimeState::Running,
            source.attributes.clone(),
            source.metrics.clone(),
        )
        .unwrap();
        assert_eq!(
            registry
                .runtime_compare_and_set(unauthorized)
                .unwrap_err()
                .code(),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            registry
                .runtime_remove(source.object_id, source.boot_id, source.revision)
                .unwrap_err()
                .code(),
            ErrorCode::PermissionDenied
        );

        registry.detach_reader("ticks", "reader-1").unwrap();
        registry.unregister_source("ctp").unwrap();
        let next = registry.runtime_state().snapshot().unwrap();
        assert_eq!(next.records().len(), 2);
        assert!(next.revision() > snapshot.revision());
    }

    #[test]
    fn heartbeat_advances_one_revision_without_rebuilding_the_registry() {
        let mut registry = test_registry();
        let process_id = registry.register_process("worker");
        let before = registry.runtime_state().snapshot().unwrap();
        registry.record_heartbeat(&process_id).unwrap();
        let after = registry.runtime_state().snapshot().unwrap();
        assert_eq!(after.revision(), before.revision() + 1);
        assert_eq!(after.records().len(), before.records().len());
        assert_eq!(after.records()[0].state, RuntimeState::Running);
    }

    #[test]
    fn restored_legacy_definitions_do_not_survive_as_live_runtime_records() {
        let mut registry = test_registry();
        registry.register_process("worker");
        assert!(!registry
            .runtime_state()
            .snapshot()
            .unwrap()
            .records()
            .is_empty());
        registry.clear_runtime_records_for_restart();
        let snapshot = registry.runtime_state().snapshot().unwrap();
        assert_eq!(snapshot.revision(), 0);
        assert!(snapshot.records().is_empty());
    }

    #[test]
    fn reader_identity_is_unique_and_drop_table_removes_connection() {
        let mut registry = test_registry();
        let first_process = registry.register_process("first");
        let second_process = registry.register_process("second");
        registry
            .register_stream(
                "ticks",
                serde_json::json!({"fields": []}),
                "schema-v1",
                1024,
                256,
            )
            .unwrap();
        registry
            .attach_reader("ticks", &first_process, "reader-1")
            .unwrap();
        registry
            .attach_reader("ticks", &first_process, "reader-1")
            .unwrap();
        assert!(matches!(
            registry.attach_reader("ticks", &second_process, "reader-1"),
            Err(crate::registry::RegistryError::ReaderNotOwnedByProcess { .. })
        ));
        assert_eq!(registry.get_stream("ticks").unwrap().reader_count, 1);
        assert_eq!(
            registry
                .runtime_state()
                .snapshot()
                .unwrap()
                .records()
                .iter()
                .filter(|record| matches!(record.object_id, RuntimeObjectId::Connection(_)))
                .count(),
            1
        );

        registry.drop_table("ticks");
        assert!(!registry
            .runtime_state()
            .snapshot()
            .unwrap()
            .records()
            .iter()
            .any(|record| matches!(record.object_id, RuntimeObjectId::Connection(_))));
    }

    #[test]
    fn legacy_expiry_does_not_mutate_generic_records_with_spoofed_attributes() {
        let mut registry = test_registry();
        let process_id = registry.register_process("worker");
        let object_id = RuntimeObjectId::Source(SourceId::from_u128(77).unwrap());
        let generic = registry
            .runtime_register(
                RuntimeRegistration::new(
                    object_id,
                    RuntimeState::Registered,
                    BTreeMap::from([("legacy_process_id".to_owned(), process_id.clone())]),
                    BTreeMap::new(),
                )
                .unwrap(),
            )
            .unwrap();
        registry.force_expire_process(&process_id).unwrap();
        assert_eq!(registry.runtime_state().get(object_id).unwrap(), &generic);
    }

    #[test]
    fn repeated_stopping_and_failed_status_updates_remain_observable() {
        let mut registry = test_registry();
        let process_id = registry.register_process("worker");
        registry
            .register_stream("ticks", serde_json::json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        registry
            .register_source(
                "source",
                "test",
                &process_id,
                "ticks",
                serde_json::json!({}),
            )
            .unwrap();
        registry
            .set_source_status("source", "stopping", Some(serde_json::json!({"step": 1})))
            .unwrap();
        let first = registry.runtime_state().snapshot().unwrap();
        let first = first
            .records()
            .iter()
            .find(|record| matches!(record.object_id, RuntimeObjectId::Source(_)))
            .unwrap();
        registry
            .set_source_status("source", "stopping", Some(serde_json::json!({"step": 2})))
            .unwrap();
        let second = registry.runtime_state().snapshot().unwrap();
        let second = second
            .records()
            .iter()
            .find(|record| matches!(record.object_id, RuntimeObjectId::Source(_)))
            .unwrap();
        assert_eq!(second.object_id, first.object_id);
        assert_eq!(second.revision, first.revision + 1);
        assert_eq!(second.metrics["step"], RuntimeMetricValue::Counter(2));

        registry
            .set_source_status("source", "failed", Some(serde_json::json!({"step": 3})))
            .unwrap();
        registry
            .set_source_status("source", "failed", Some(serde_json::json!({"step": 4})))
            .unwrap();
        let failed = registry.runtime_state().snapshot().unwrap();
        let failed = failed
            .records()
            .iter()
            .find(|record| matches!(record.object_id, RuntimeObjectId::Source(_)))
            .unwrap();
        assert_ne!(failed.object_id, second.object_id);
        assert_eq!(failed.state, RuntimeState::Failed);
        assert_eq!(failed.metrics["step"], RuntimeMetricValue::Counter(4));
    }
}
