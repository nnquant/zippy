//! Ephemeral runtime registry contracts with explicit compare-and-set updates.
//!
//! Runtime records are valid only for the current server boot. Durable table,
//! producer-checkpoint, consumer-offset, and maintenance state belongs in the
//! persistent catalog. Query lifecycle belongs exclusively to `QueryEngine`.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::{ConnectionId, EngineId, ProcessId, Result, SessionId, SourceId};

/// A typed identity for one ephemeral runtime object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum RuntimeObjectId {
    /// A server or client session.
    Session(SessionId),
    /// A boot-scoped network or subscription connection.
    Connection(ConnectionId),
    /// A registered operating-system process.
    Process(ProcessId),
    /// A live source instance.
    Source(SourceId),
    /// A live compute-engine instance.
    Engine(EngineId),
}

/// The lifecycle state of an ephemeral runtime object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeState {
    /// The object is registered but has not started work.
    Registered,
    /// The object is healthy and serving work.
    Running,
    /// The object is serving with a known impairment.
    Degraded,
    /// The object is draining or stopping.
    Stopping,
    /// The object stopped cleanly.
    Stopped,
    /// The object failed and may require intervention.
    Failed,
}

impl RuntimeState {
    /// Returns whether the runtime lifecycle permits this forward transition.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Registered, Self::Registered)
                | (Self::Running, Self::Running)
                | (Self::Degraded, Self::Degraded)
                | (Self::Stopping, Self::Stopping)
                | (
                    Self::Registered,
                    Self::Running | Self::Stopping | Self::Failed
                )
                | (
                    Self::Running,
                    Self::Degraded | Self::Stopping | Self::Failed
                )
                | (
                    Self::Degraded,
                    Self::Running | Self::Stopping | Self::Failed
                )
                | (Self::Stopping, Self::Stopped | Self::Failed)
        )
    }
}

/// A finite floating-point metric value.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct FiniteMetricValue(f64);

impl FiniteMetricValue {
    /// Creates a metric value while rejecting NaN and both infinities.
    pub fn new(value: f64) -> std::result::Result<Self, NonFiniteMetricValue> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(NonFiniteMetricValue)
        }
    }

    /// Returns the validated finite value.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl Serialize for FiniteMetricValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl<'de> Deserialize<'de> for FiniteMetricValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// An error returned for NaN or infinite runtime metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("runtime metric value must be finite")]
pub struct NonFiniteMetricValue;

/// A typed runtime metric that cannot contain non-finite floating-point data.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RuntimeMetricValue {
    /// A monotonic integer counter.
    Counter(u64),
    /// A finite point-in-time gauge.
    Gauge(FiniteMetricValue),
}

/// Caller-supplied fields for first registration of a runtime object.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RuntimeRegistration {
    /// The typed runtime identity.
    object_id: RuntimeObjectId,
    /// The initial lifecycle state.
    state: RuntimeState,
    /// Low-cardinality status attributes for inspection.
    attributes: BTreeMap<String, String>,
    /// Typed runtime metrics that are never persisted as catalog facts.
    metrics: BTreeMap<String, RuntimeMetricValue>,
}

impl RuntimeRegistration {
    /// Creates first-registration fields in the sole legal initial state.
    pub fn new(
        object_id: RuntimeObjectId,
        state: RuntimeState,
        attributes: BTreeMap<String, String>,
        metrics: BTreeMap<String, RuntimeMetricValue>,
    ) -> std::result::Result<Self, InvalidRuntimeRegistration> {
        if state != RuntimeState::Registered {
            return Err(InvalidRuntimeRegistration { state });
        }
        Ok(Self {
            object_id,
            state,
            attributes,
            metrics,
        })
    }

    /// Returns the runtime object being registered.
    #[must_use]
    pub const fn object_id(&self) -> RuntimeObjectId {
        self.object_id
    }

    /// Returns the validated initial state.
    #[must_use]
    pub const fn state(&self) -> RuntimeState {
        self.state
    }

    /// Borrows the initial status attributes.
    #[must_use]
    pub fn attributes(&self) -> &BTreeMap<String, String> {
        &self.attributes
    }

    /// Borrows the initial metrics.
    #[must_use]
    pub fn metrics(&self) -> &BTreeMap<String, RuntimeMetricValue> {
        &self.metrics
    }
}

impl<'de> Deserialize<'de> for RuntimeRegistration {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawRegistration {
            object_id: RuntimeObjectId,
            state: RuntimeState,
            attributes: BTreeMap<String, String>,
            metrics: BTreeMap<String, RuntimeMetricValue>,
        }
        let raw = RawRegistration::deserialize(deserializer)?;
        Self::new(raw.object_id, raw.state, raw.attributes, raw.metrics)
            .map_err(serde::de::Error::custom)
    }
}

/// A runtime object can only be introduced in `registered` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid initial runtime state state=[{state:?}]")]
pub struct InvalidRuntimeRegistration {
    /// The rejected initial state.
    pub state: RuntimeState,
}

/// A registry-assigned boot-scoped runtime status record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeRecord {
    /// The typed runtime identity.
    pub object_id: RuntimeObjectId,
    /// The server boot session that owns this record.
    pub boot_id: SessionId,
    /// The current lifecycle state.
    pub state: RuntimeState,
    /// The registry-assigned revision within this server boot.
    pub revision: u64,
    /// The last update time as Unix nanoseconds in UTC.
    pub updated_at_ns: u64,
    /// Low-cardinality status attributes for inspection.
    pub attributes: BTreeMap<String, String>,
    /// Typed runtime metrics that are never persisted as catalog facts.
    pub metrics: BTreeMap<String, RuntimeMetricValue>,
}

/// A compare-and-set replacement for an existing runtime record.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RuntimeCasUpdate {
    /// The runtime object to update.
    object_id: RuntimeObjectId,
    /// The boot session that must still own the record.
    expected_boot_id: SessionId,
    /// The exact registry revision that must still be current.
    expected_revision: u64,
    /// The lifecycle state guarded by the compare-and-set.
    expected_state: RuntimeState,
    /// The validated replacement lifecycle state.
    next_state: RuntimeState,
    /// Replacement low-cardinality status attributes.
    attributes: BTreeMap<String, String>,
    /// Replacement typed runtime metrics.
    metrics: BTreeMap<String, RuntimeMetricValue>,
}

impl RuntimeCasUpdate {
    /// Creates a CAS update only for a legal transition or active-state heartbeat.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        object_id: RuntimeObjectId,
        expected_boot_id: SessionId,
        expected_revision: u64,
        expected_state: RuntimeState,
        next_state: RuntimeState,
        attributes: BTreeMap<String, String>,
        metrics: BTreeMap<String, RuntimeMetricValue>,
    ) -> std::result::Result<Self, InvalidRuntimeTransition> {
        if !expected_state.can_transition_to(next_state) {
            return Err(InvalidRuntimeTransition {
                expected_state,
                next_state,
            });
        }
        Ok(Self {
            object_id,
            expected_boot_id,
            expected_revision,
            expected_state,
            next_state,
            attributes,
            metrics,
        })
    }

    /// Returns the guarded runtime object identity.
    #[must_use]
    pub const fn object_id(&self) -> RuntimeObjectId {
        self.object_id
    }

    /// Returns the guarded server boot identity.
    #[must_use]
    pub const fn expected_boot_id(&self) -> SessionId {
        self.expected_boot_id
    }

    /// Returns the guarded registry revision.
    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    /// Returns the guarded current lifecycle state.
    #[must_use]
    pub const fn expected_state(&self) -> RuntimeState {
        self.expected_state
    }

    /// Returns the validated replacement lifecycle state.
    #[must_use]
    pub const fn next_state(&self) -> RuntimeState {
        self.next_state
    }

    /// Borrows replacement attributes.
    #[must_use]
    pub fn attributes(&self) -> &BTreeMap<String, String> {
        &self.attributes
    }

    /// Borrows replacement metrics.
    #[must_use]
    pub fn metrics(&self) -> &BTreeMap<String, RuntimeMetricValue> {
        &self.metrics
    }
}

impl<'de> Deserialize<'de> for RuntimeCasUpdate {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawUpdate {
            object_id: RuntimeObjectId,
            expected_boot_id: SessionId,
            expected_revision: u64,
            expected_state: RuntimeState,
            next_state: RuntimeState,
            attributes: BTreeMap<String, String>,
            metrics: BTreeMap<String, RuntimeMetricValue>,
        }
        let raw = RawUpdate::deserialize(deserializer)?;
        Self::new(
            raw.object_id,
            raw.expected_boot_id,
            raw.expected_revision,
            raw.expected_state,
            raw.next_state,
            raw.attributes,
            raw.metrics,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// A runtime CAS request attempted an illegal lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid runtime transition expected=[{expected_state:?}] next=[{next_state:?}]")]
pub struct InvalidRuntimeTransition {
    /// The guarded current state.
    pub expected_state: RuntimeState,
    /// The rejected replacement state.
    pub next_state: RuntimeState,
}

/// An immutable, boot-scoped view of the runtime registry.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RuntimeSnapshot {
    /// The server boot session that owns all records.
    boot_id: SessionId,
    /// The registry revision at which records were collected.
    revision: u64,
    /// Runtime records sorted by their typed identity.
    records: Vec<RuntimeRecord>,
}

impl RuntimeSnapshot {
    /// Creates a boot-consistent snapshot in strict object-identity order.
    pub fn new(
        boot_id: SessionId,
        revision: u64,
        records: Vec<RuntimeRecord>,
    ) -> std::result::Result<Self, InvalidRuntimeSnapshot> {
        for record in &records {
            if record.boot_id != boot_id {
                return Err(InvalidRuntimeSnapshot::BootIdMismatch {
                    object_id: record.object_id,
                });
            }
            if record.revision > revision {
                return Err(InvalidRuntimeSnapshot::RecordRevisionAhead {
                    object_id: record.object_id,
                    record_revision: record.revision,
                    snapshot_revision: revision,
                });
            }
        }
        for pair in records.windows(2) {
            match pair[0].object_id.cmp(&pair[1].object_id) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(InvalidRuntimeSnapshot::DuplicateObjectId {
                        object_id: pair[0].object_id,
                    });
                }
                std::cmp::Ordering::Greater => {
                    return Err(InvalidRuntimeSnapshot::UnsortedObjectIds);
                }
            }
        }
        Ok(Self {
            boot_id,
            revision,
            records,
        })
    }

    /// Returns the server boot identity shared by every record.
    #[must_use]
    pub const fn boot_id(&self) -> SessionId {
        self.boot_id
    }

    /// Returns the registry revision represented by this snapshot.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Borrows records in strict runtime-object identity order.
    #[must_use]
    pub fn records(&self) -> &[RuntimeRecord] {
        &self.records
    }
}

impl<'de> Deserialize<'de> for RuntimeSnapshot {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawSnapshot {
            boot_id: SessionId,
            revision: u64,
            records: Vec<RuntimeRecord>,
        }
        let raw = RawSnapshot::deserialize(deserializer)?;
        Self::new(raw.boot_id, raw.revision, raw.records).map_err(serde::de::Error::custom)
    }
}

/// Invalid boot, revision, or deterministic ordering in a runtime snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidRuntimeSnapshot {
    /// Every record must belong to the snapshot's boot identity.
    #[error("runtime record boot id mismatch object_id=[{object_id:?}]")]
    BootIdMismatch {
        /// The mismatched runtime object.
        object_id: RuntimeObjectId,
    },
    /// A record revision cannot be newer than its enclosing snapshot.
    #[error("runtime revision ahead record=[{record_revision}] max=[{snapshot_revision}]")]
    RecordRevisionAhead {
        /// The invalid runtime object.
        object_id: RuntimeObjectId,
        /// The record revision.
        record_revision: u64,
        /// The enclosing snapshot revision.
        snapshot_revision: u64,
    },
    /// Records must be strictly sorted by typed runtime object identity.
    #[error("runtime snapshot records must be strictly sorted by object id")]
    UnsortedObjectIds,
    /// A runtime object may appear at most once.
    #[error("duplicate runtime snapshot object id object_id=[{object_id:?}]")]
    DuplicateObjectId {
        /// The repeated object identity.
        object_id: RuntimeObjectId,
    },
}

/// In-memory status and lifecycle coordination for live server objects.
pub trait RuntimeRegistry: Send + Sync {
    /// Returns the authoritative server boot identity assigned by the registry.
    fn boot_id(&self) -> SessionId;

    /// Registers a new object and returns the registry-assigned record.
    fn register(&self, registration: RuntimeRegistration) -> Result<RuntimeRecord>;

    /// Applies a prevalidated state transition or same-state heartbeat only if
    /// boot id, revision, and expected state still match.
    fn compare_and_set(&self, update: RuntimeCasUpdate) -> Result<RuntimeRecord>;

    /// Removes one record only if its exact revision still matches.
    fn remove(&self, object_id: RuntimeObjectId, expected_revision: u64) -> Result<()>;

    /// Returns one current runtime record.
    fn get(&self, object_id: RuntimeObjectId) -> Result<Option<RuntimeRecord>>;

    /// Returns one immutable snapshot of all runtime domains.
    fn snapshot(&self) -> Result<RuntimeSnapshot>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        FiniteMetricValue, RuntimeCasUpdate, RuntimeMetricValue, RuntimeObjectId, RuntimeRecord,
        RuntimeRegistration, RuntimeRegistry, RuntimeSnapshot, RuntimeState,
    };
    use crate::{SessionId, SourceId};

    fn runtime_record(source_id: u128, boot_id: SessionId, revision: u64) -> RuntimeRecord {
        RuntimeRecord {
            object_id: RuntimeObjectId::Source(SourceId::from_u128(source_id).unwrap()),
            boot_id,
            state: RuntimeState::Running,
            revision,
            updated_at_ns: revision,
            attributes: BTreeMap::new(),
            metrics: BTreeMap::new(),
        }
    }

    fn assert_object_safe(_: Option<&dyn RuntimeRegistry>) {}

    #[test]
    fn runtime_registry_is_object_safe() {
        assert_object_safe(None);
    }

    #[test]
    fn runtime_state_transitions_are_explicit() {
        assert!(RuntimeState::Registered.can_transition_to(RuntimeState::Running));
        assert!(RuntimeState::Running.can_transition_to(RuntimeState::Degraded));
        assert!(RuntimeState::Degraded.can_transition_to(RuntimeState::Running));
        assert!(RuntimeState::Stopping.can_transition_to(RuntimeState::Stopped));
        assert!(!RuntimeState::Stopped.can_transition_to(RuntimeState::Running));
        assert!(!RuntimeState::Failed.can_transition_to(RuntimeState::Registered));
        assert!(RuntimeState::Running.can_transition_to(RuntimeState::Running));
        assert!(!RuntimeState::Stopped.can_transition_to(RuntimeState::Stopped));
    }

    #[test]
    fn metric_gauge_rejects_every_non_finite_value() {
        assert!(FiniteMetricValue::new(f64::NAN).is_err());
        assert!(FiniteMetricValue::new(f64::INFINITY).is_err());
        assert!(FiniteMetricValue::new(f64::NEG_INFINITY).is_err());
        let metric = RuntimeMetricValue::Gauge(FiniteMetricValue::new(1.25).unwrap());
        assert_eq!(
            serde_json::to_string(&metric).unwrap(),
            r#"{"type":"gauge","value":1.25}"#
        );
    }

    #[test]
    fn validated_runtime_inputs_block_illegal_registration_and_transitions() {
        let object_id = RuntimeObjectId::Source(SourceId::from_u128(1).unwrap());
        let boot_id = SessionId::from_u128(2).unwrap();
        assert!(RuntimeRegistration::new(
            object_id,
            RuntimeState::Registered,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .is_ok());
        assert!(RuntimeRegistration::new(
            object_id,
            RuntimeState::Running,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .is_err());
        assert!(RuntimeCasUpdate::new(
            object_id,
            boot_id,
            3,
            RuntimeState::Running,
            RuntimeState::Running,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .is_ok());
        assert!(RuntimeCasUpdate::new(
            object_id,
            boot_id,
            3,
            RuntimeState::Stopped,
            RuntimeState::Running,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .is_err());
        assert!(serde_json::from_str::<RuntimeRegistration>(concat!(
            r#"{"object_id":{"type":"source","#,
            r#""id":"00000000-0000-0000-0000-000000000001"},"#,
            r#""state":"failed","attributes":{},"metrics":{}}"#,
        ))
        .is_err());
        assert!(serde_json::from_str::<RuntimeCasUpdate>(concat!(
            r#"{"object_id":{"type":"source","#,
            r#""id":"00000000-0000-0000-0000-000000000001"},"#,
            r#""expected_boot_id":"00000000-0000-0000-0000-000000000002","#,
            r#""expected_revision":3,"expected_state":"stopped","next_state":"running","#,
            r#""attributes":{},"metrics":{}}"#,
        ))
        .is_err());
    }

    #[test]
    fn runtime_snapshot_rejects_boot_revision_order_and_serde_bypass() {
        let boot_id = SessionId::from_u128(10).unwrap();
        let first = runtime_record(1, boot_id, 1);
        let second = runtime_record(2, boot_id, 2);
        let valid = RuntimeSnapshot::new(boot_id, 2, vec![first.clone(), second.clone()]).unwrap();
        assert_eq!(valid.boot_id(), boot_id);
        assert_eq!(valid.revision(), 2);
        assert_eq!(valid.records(), [first.clone(), second.clone()]);
        assert_eq!(
            serde_json::from_value::<RuntimeSnapshot>(serde_json::to_value(&valid).unwrap())
                .unwrap(),
            valid
        );

        let foreign_boot = SessionId::from_u128(11).unwrap();
        assert!(
            RuntimeSnapshot::new(boot_id, 2, vec![runtime_record(1, foreign_boot, 1)]).is_err()
        );
        assert!(RuntimeSnapshot::new(boot_id, 1, vec![second.clone()]).is_err());
        assert!(RuntimeSnapshot::new(boot_id, 2, vec![second.clone(), first.clone()]).is_err());
        assert!(RuntimeSnapshot::new(boot_id, 2, vec![first.clone(), first]).is_err());

        let mut invalid_json = serde_json::to_value(&valid).unwrap();
        invalid_json["records"][0]["revision"] = serde_json::json!(3);
        assert!(serde_json::from_value::<RuntimeSnapshot>(invalid_json).is_err());

        let mut invalid_json = serde_json::to_value(&valid).unwrap();
        invalid_json["records"].as_array_mut().unwrap().reverse();
        assert!(serde_json::from_value::<RuntimeSnapshot>(invalid_json).is_err());
    }
}
