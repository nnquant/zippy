//! Startup-only reconciliation of managed Parquet candidates and Catalog identity.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use serde::Serialize;
use zippy_core::{DataDirectoryError, OwnedDataDirectory};
use zippy_types::catalog::{
    CatalogDataFileProbeMatch, CatalogDataFileProbeResult, CatalogFileRead,
    CatalogPublicationControl, ManagedDataFileProbe, VersionedCatalogCommand,
};
use zippy_types::table::{DataFileLocation, DataFileState, ManagedDataPath, PendingDataFile};
use zippy_types::{ErrorCode, FileId, RequestId, Result, ServerError, TableId};

use crate::candidate::{
    map_data_directory_error, open_existing_candidate, recover_parquet_candidate,
    verify_pending_candidate,
};
use crate::commit_intent::{command_files, CommitIntentJournal};

const DEFAULT_MAX_SCAN_ENTRIES: usize = 65_536;
const DEFAULT_PROBE_BATCH_SIZE: usize = 512;
const DEFAULT_MAX_RELATIVE_PATH_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_VERIFICATION_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_MAX_INTENT_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_MAX_PENDING_INTENT_FILES: usize = 65_536;
const DEFAULT_MAX_RETAINED_METADATA_BYTES: u64 = 128 * 1024 * 1024;
const MAX_SCAN_ENTRIES: usize = 1_000_000;
const MAX_PROBE_BATCH_SIZE: usize = 4_096;
const MAX_RELATIVE_PATH_BYTES: usize = 256 * 1024 * 1024;
const MAX_VERIFICATION_BYTES: u64 = 64 * 1024 * 1024 * 1024 * 1024;
const MAX_INTENT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_PENDING_INTENT_FILES: usize = 1_000_000;
const MAX_RETAINED_METADATA_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const PARQUET_DIRECTORY: &str = "parquet";

/// Resource bounds for one startup reconciliation pass.
#[derive(Debug, Clone)]
pub struct StartupOrphanReconcilerConfig {
    max_scan_entries: usize,
    probe_batch_size: usize,
    max_relative_path_bytes: usize,
    max_verification_bytes: u64,
    max_intent_bytes: u64,
    max_pending_intent_files: usize,
    max_retained_metadata_bytes: u64,
}

impl Default for StartupOrphanReconcilerConfig {
    fn default() -> Self {
        Self {
            max_scan_entries: DEFAULT_MAX_SCAN_ENTRIES,
            probe_batch_size: DEFAULT_PROBE_BATCH_SIZE,
            max_relative_path_bytes: DEFAULT_MAX_RELATIVE_PATH_BYTES,
            max_verification_bytes: DEFAULT_MAX_VERIFICATION_BYTES,
            max_intent_bytes: DEFAULT_MAX_INTENT_BYTES,
            max_pending_intent_files: DEFAULT_MAX_PENDING_INTENT_FILES,
            max_retained_metadata_bytes: DEFAULT_MAX_RETAINED_METADATA_BYTES,
        }
    }
}

impl StartupOrphanReconcilerConfig {
    /// Creates the bounded default startup recovery policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum files, directories, and unexpected entries inspected.
    #[must_use]
    pub const fn with_max_scan_entries(mut self, maximum: usize) -> Self {
        self.max_scan_entries = maximum;
        self
    }

    /// Sets the number of physical candidates checked per Catalog transaction.
    #[must_use]
    pub const fn with_probe_batch_size(mut self, size: usize) -> Self {
        self.probe_batch_size = size;
        self
    }

    /// Sets the aggregate UTF-8 bytes retained for relative scan paths.
    #[must_use]
    pub const fn with_max_relative_path_bytes(mut self, maximum: usize) -> Self {
        self.max_relative_path_bytes = maximum;
        self
    }

    /// Sets the total candidate bytes eligible for checksum and footer verification.
    #[must_use]
    pub const fn with_max_verification_bytes(mut self, maximum: u64) -> Self {
        self.max_verification_bytes = maximum;
        self
    }

    /// Sets the aggregate serialized bytes admitted from durable intents.
    #[must_use]
    pub const fn with_max_intent_bytes(mut self, maximum: u64) -> Self {
        self.max_intent_bytes = maximum;
        self
    }

    /// Sets the total candidate descriptors decoded from durable intents.
    #[must_use]
    pub const fn with_max_pending_intent_files(mut self, maximum: usize) -> Self {
        self.max_pending_intent_files = maximum;
        self
    }

    /// Sets the encoded metadata footprint retained as recovery evidence.
    #[must_use]
    pub const fn with_max_retained_metadata_bytes(mut self, maximum: u64) -> Self {
        self.max_retained_metadata_bytes = maximum;
        self
    }
}

/// Why a managed file is retained after startup reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciledFileDisposition {
    /// A Catalog row protects the file, including historical lifecycle states.
    CatalogReferenced {
        /// The physical-deletion lifecycle stored by the Catalog.
        state: DataFileState,
        /// The snapshot that removed the file from visibility, when present.
        removed_snapshot_version: Option<u64>,
        /// The durable intent still naming this exact Catalog row, when present.
        durable_intent_request_id: Option<RequestId>,
    },
    /// A durable intent protects a verified file not yet visible in the Catalog.
    PendingIntent {
        /// The atomic Catalog command that owns this candidate.
        request_id: RequestId,
    },
    /// A valid native candidate has neither a Catalog row nor a durable intent.
    Orphan,
}

/// One canonical managed candidate and its startup reconciliation outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciledFile {
    /// The immutable candidate identity.
    pub file_id: FileId,
    /// The table encoded by the canonical directory and candidate footer.
    pub table_id: TableId,
    /// The canonical path relative to the managed data root.
    pub path: ManagedDataPath,
    /// The physical length observed through a securely opened descriptor.
    pub file_size_bytes: u64,
    /// The retention decision derived from Catalog and intent evidence.
    pub disposition: ReconciledFileDisposition,
    /// A checksum- and footer-validated descriptor for pending or orphan candidates.
    pub verified_candidate: Option<PendingDataFile>,
}

/// Classification for a non-authoritative scan anomaly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationIssueKind {
    /// An entry does not follow the canonical managed Parquet layout.
    UnexpectedEntry,
    /// An unreferenced canonical file cannot be reconstructed from its footer.
    InvalidUnreferencedCandidate,
}

/// One bounded anomaly retained for operator inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationIssue {
    /// The managed path relative to the data root.
    pub path: ManagedDataPath,
    /// Stable high-level anomaly category.
    pub kind: ReconciliationIssueKind,
    /// Concise diagnostic detail; no automatic action is taken from this text.
    pub reason: String,
}

/// Complete evidence from one revision-stable startup reconciliation pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupOrphanReconciliationReport {
    /// Catalog revision shared by the pre-scan, all probes, and post-scan read.
    pub catalog_revision: u64,
    /// Canonical candidate outcomes in strict file-id order.
    pub files: Vec<ReconciledFile>,
    /// Unpublished candidate or intent temporary files, never deleted here.
    pub temporary_files: Vec<ManagedDataPath>,
    /// Invalid or unexpected unreferenced entries, also never deleted here.
    pub issues: Vec<ReconciliationIssue>,
}

impl StartupOrphanReconciliationReport {
    /// Returns the number of valid candidates with no Catalog or intent owner.
    #[must_use]
    pub fn orphan_count(&self) -> usize {
        self.files
            .iter()
            .filter(|file| file.disposition == ReconciledFileDisposition::Orphan)
            .count()
    }

    /// Returns whether this physical-to-Catalog pass observed no local backlog.
    ///
    /// This does not prove that every Catalog-referenced path exists; reverse
    /// verification belongs to the maintenance verifier.
    #[must_use]
    pub fn has_no_observed_backlog(&self) -> bool {
        self.temporary_files.is_empty()
            && self.issues.is_empty()
            && self.files.iter().all(|file| {
                matches!(
                    file.disposition,
                    ReconciledFileDisposition::CatalogReferenced {
                        state: DataFileState::Committed,
                        removed_snapshot_version: None,
                        durable_intent_request_id: None,
                    }
                )
            })
    }
}

/// Performs read-only reconciliation before any table writer receives admission.
///
/// Data-root ownership excludes other processes, but the composition root must
/// also keep candidate publication and commit admission private until this pass
/// returns. The resulting `Unreferenced` observation is deliberately not a
/// deletion capability. Rejected-candidate cleanup additionally requires its
/// affine token and exclusive publication authority; ordinary historical-file
/// deletion requires snapshot leases, a Catalog lifecycle transition, and the
/// durable GC protocol.
pub struct StartupOrphanReconciler {
    config: StartupOrphanReconcilerConfig,
    data_directory: Arc<OwnedDataDirectory>,
    catalog: Arc<dyn CatalogFileRead>,
    publication_control: Option<Arc<dyn CatalogPublicationControl>>,
    #[cfg(test)]
    before_cleanup_unlink: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl StartupOrphanReconciler {
    /// Creates a startup reconciler using conservative default scan budgets.
    #[must_use]
    pub fn new(data_directory: Arc<OwnedDataDirectory>, catalog: Arc<dyn CatalogFileRead>) -> Self {
        Self {
            config: StartupOrphanReconcilerConfig::default(),
            data_directory,
            catalog,
            publication_control: None,
            #[cfg(test)]
            before_cleanup_unlink: None,
        }
    }

    /// Creates a startup reconciler authorized to remove verified orphans.
    ///
    /// The same concrete Catalog object supplies file-reference reads and
    /// publication exclusion, preventing accidental cleanup against a gate
    /// owned by another Catalog instance.
    pub fn new_with_cleanup_control<C>(
        data_directory: Arc<OwnedDataDirectory>,
        catalog: Arc<C>,
    ) -> Self
    where
        C: CatalogFileRead + CatalogPublicationControl + 'static,
    {
        let file_catalog: Arc<dyn CatalogFileRead> = catalog.clone();
        let publication_control: Arc<dyn CatalogPublicationControl> = catalog;
        Self {
            config: StartupOrphanReconcilerConfig::default(),
            data_directory,
            catalog: file_catalog,
            publication_control: Some(publication_control),
            #[cfg(test)]
            before_cleanup_unlink: None,
        }
    }

    /// Creates a startup reconciler with explicit resource budgets.
    pub fn with_config(
        config: StartupOrphanReconcilerConfig,
        data_directory: Arc<OwnedDataDirectory>,
        catalog: Arc<dyn CatalogFileRead>,
    ) -> Result<Self> {
        validate_config(&config)?;
        Ok(Self {
            config,
            data_directory,
            catalog,
            publication_control: None,
            #[cfg(test)]
            before_cleanup_unlink: None,
        })
    }

    /// Creates an orphan-cleanup reconciler with explicit resource budgets.
    pub fn with_config_and_cleanup_control<C>(
        config: StartupOrphanReconcilerConfig,
        data_directory: Arc<OwnedDataDirectory>,
        catalog: Arc<C>,
    ) -> Result<Self>
    where
        C: CatalogFileRead + CatalogPublicationControl + 'static,
    {
        validate_config(&config)?;
        let file_catalog: Arc<dyn CatalogFileRead> = catalog.clone();
        let publication_control: Arc<dyn CatalogPublicationControl> = catalog;
        Ok(Self {
            config,
            data_directory,
            catalog: file_catalog,
            publication_control: Some(publication_control),
            #[cfg(test)]
            before_cleanup_unlink: None,
        })
    }

    #[cfg(test)]
    fn with_before_cleanup_unlink(mut self, hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        self.before_cleanup_unlink = Some(hook);
        self
    }

    /// Scans physical candidates and returns a revision-stable read-only report.
    pub async fn reconcile(&self) -> Result<StartupOrphanReconciliationReport> {
        validate_config(&self.config)?;
        let catalog_revision = self.catalog.revision().await?;
        let data_directory = Arc::clone(&self.data_directory);
        let config = self.config.clone();
        let (scan, intents) = tokio::task::spawn_blocking(move || {
            let mut scan = scan_parquet_directory(&data_directory, &config)?;
            let intents = load_pending_intents(&data_directory, &config, &mut scan)?;
            Ok::<_, ServerError>((scan, intents))
        })
        .await
        .map_err(|error| join_error("scan startup storage state", error))??;

        let mut catalog_results = Vec::with_capacity(scan.candidates.len());
        for chunk in scan.candidates.chunks(self.config.probe_batch_size) {
            let probes = chunk
                .iter()
                .map(PhysicalCandidate::probe)
                .collect::<Vec<_>>();
            let response = self.catalog.inspect_data_file_references(&probes).await?;
            if response.catalog_revision != catalog_revision {
                return Err(revision_changed(
                    catalog_revision,
                    response.catalog_revision,
                ));
            }
            if response.results.len() != probes.len() {
                return Err(corruption(format!(
                    concat!(
                        "catalog file probe response count is not dense ",
                        "expected=[{}] actual=[{}]"
                    ),
                    probes.len(),
                    response.results.len()
                )));
            }
            if !response
                .results
                .iter()
                .zip(&probes)
                .all(|(result, probe)| result.probe == *probe)
            {
                return Err(corruption(
                    "catalog file probe response does not match the requested dense order",
                ));
            }
            catalog_results.extend(response.results);
        }
        let ending_revision = self.catalog.revision().await?;
        if ending_revision != catalog_revision {
            return Err(revision_changed(catalog_revision, ending_revision));
        }

        let data_directory = Arc::clone(&self.data_directory);
        let max_verification_bytes = self.config.max_verification_bytes;
        let max_retained_metadata_bytes = self.config.max_retained_metadata_bytes;
        tokio::task::spawn_blocking(move || {
            classify_scan(
                &data_directory,
                catalog_revision,
                scan,
                intents,
                catalog_results,
                max_verification_bytes,
                max_retained_metadata_bytes,
            )
        })
        .await
        .map_err(|error| join_error("classify startup storage state", error))?
    }

    /// Removes temporary files and verified orphans while admission is private.
    ///
    /// The reconciler must have been created with cleanup control. This method
    /// takes the Catalog's exclusive publication exclusion before its first
    /// scan and retains it through unlink and the second reconciliation pass.
    /// It rechecks the Catalog revision before unlinking, durably removes
    /// scanned candidate and intent `.tmp-*` files plus checksum- and
    /// footer-verified `Orphan` files, then performs a second full pass.
    /// Catalog-referenced and pending-intent files are never removed. The
    /// composition root must still call it before public table admission.
    pub async fn reconcile_and_remove_orphans_before_admission(
        &self,
    ) -> Result<StartupOrphanReconciliationReport> {
        let publication_control = self.publication_control.as_ref().ok_or_else(|| {
            ServerError::new(
                ErrorCode::UnsupportedPlan,
                "startup orphan cleanup requires Catalog publication control",
            )
            .with_retryable(false)
        })?;
        let publication_exclusion = publication_control.acquire_publication_exclusion().await?;
        let report = self.reconcile().await?;
        reject_invalid_unreferenced_candidates(&report)?;
        let orphans = report
            .files
            .iter()
            .filter(|file| file.disposition == ReconciledFileDisposition::Orphan)
            .map(|file| {
                let pending = file.verified_candidate.clone().ok_or_else(|| {
                    corruption(format!(
                        "startup orphan lacks verified candidate file_id=[{}]",
                        file.file_id
                    ))
                })?;
                Ok((pending, file.path.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        let temporary_files = report.temporary_files.clone();
        if orphans.is_empty() && temporary_files.is_empty() {
            return Ok(report);
        }

        let current_revision = self.catalog.revision().await?;
        if current_revision != report.catalog_revision {
            return Err(revision_changed(report.catalog_revision, current_revision));
        }
        let removed_orphans = orphans.len();
        let removed_temporary_files = temporary_files.len();
        let data_directory = Arc::clone(&self.data_directory);
        #[cfg(test)]
        let before_cleanup_unlink = self.before_cleanup_unlink.clone();
        let publication_exclusion = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(hook) = before_cleanup_unlink {
                hook();
            }
            for path in temporary_files {
                data_directory
                    .remove_durable_file(&path)
                    .map_err(|error| startup_temporary_cleanup_error(&path, error))?;
            }
            for (pending, path) in orphans {
                verify_pending_candidate(&data_directory, &pending)?;
                data_directory
                    .remove_durable_file(&path)
                    .map_err(|error| map_data_directory_error(pending.table_id, error))?;
            }
            Ok::<_, ServerError>(publication_exclusion)
        })
        .await
        .map_err(|error| join_error("remove verified startup orphans", error))??;
        tracing::info!(
            catalog_revision = report.catalog_revision,
            removed_orphans,
            removed_temporary_files,
            "removed temporary files and verified orphans before publication admission"
        );
        let reconciled = self.reconcile().await?;
        reject_invalid_unreferenced_candidates(&reconciled)?;
        drop(publication_exclusion);
        Ok(reconciled)
    }
}

#[derive(Debug)]
struct PhysicalCandidate {
    file_id: FileId,
    table_id: TableId,
    path: ManagedDataPath,
    file_size_bytes: u64,
}

impl PhysicalCandidate {
    fn probe(&self) -> ManagedDataFileProbe {
        ManagedDataFileProbe {
            file_id: self.file_id,
            table_id: self.table_id,
            path: self.path.clone(),
        }
    }
}

struct PhysicalScan {
    candidates: Vec<PhysicalCandidate>,
    temporary_files: Vec<ManagedDataPath>,
    issues: Vec<ReconciliationIssue>,
    entry_count: usize,
    path_bytes: usize,
}

#[derive(Debug, Clone)]
struct PendingIntentEvidence {
    request_id: RequestId,
    pending: PendingDataFile,
}

#[derive(Debug)]
struct PendingIntentGroup {
    request_id: RequestId,
    command_fingerprint: String,
    file_ids: Vec<FileId>,
}

#[derive(Debug)]
struct PendingIntents {
    by_file: HashMap<FileId, PendingIntentEvidence>,
    groups: Vec<PendingIntentGroup>,
    retained_metadata_bytes: u64,
}

fn scan_parquet_directory(
    data_directory: &OwnedDataDirectory,
    config: &StartupOrphanReconcilerConfig,
) -> Result<PhysicalScan> {
    let root = data_directory.directory().root();
    let parquet = root.join(PARQUET_DIRECTORY);
    let metadata = match fs::symlink_metadata(&parquet) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PhysicalScan {
                candidates: Vec::new(),
                temporary_files: Vec::new(),
                issues: Vec::new(),
                entry_count: 0,
                path_bytes: 0,
            });
        }
        Err(error) => return Err(io_error("inspect Parquet directory", &parquet, error)),
    };
    validate_directory(&parquet, &metadata)?;

    let mut candidates = Vec::new();
    let mut temporary_files = Vec::new();
    let mut issues = Vec::new();
    let mut entry_count = 0_usize;
    let mut path_bytes = 0_usize;
    for table_entry in read_directory(&parquet)? {
        let table_entry = table_entry
            .map_err(|error| io_error("read Parquet directory entry", &parquet, error))?;
        charge_entry(config, &mut entry_count)?;
        let table_path = table_entry.path();
        let table_relative = relative_managed_path(root, &table_path, config, &mut path_bytes)?;
        let table_metadata = fs::symlink_metadata(&table_path)
            .map_err(|error| io_error("inspect Parquet table entry", &table_path, error))?;
        if table_metadata.file_type().is_symlink() {
            return Err(corruption(format!(
                "parquet table entry is a symbolic link path=[{}]",
                table_path.display()
            )));
        }
        if !table_metadata.is_dir() {
            validate_owner_security(&table_path, &table_metadata, false)?;
            issues.push(ReconciliationIssue {
                path: table_relative,
                kind: ReconciliationIssueKind::UnexpectedEntry,
                reason: "parquet root entry is not a canonical table directory".to_owned(),
            });
            continue;
        }
        validate_directory(&table_path, &table_metadata)?;
        let Some(table_name) = table_entry.file_name().to_str().map(str::to_owned) else {
            return Err(corruption(format!(
                "parquet table directory name is not UTF-8 path=[{}]",
                table_path.display()
            )));
        };
        let table_id = match canonical_id::<TableId>(&table_name) {
            Ok(table_id) => table_id,
            Err(reason) => {
                issues.push(ReconciliationIssue {
                    path: table_relative,
                    kind: ReconciliationIssueKind::UnexpectedEntry,
                    reason,
                });
                continue;
            }
        };

        for file_entry in read_directory(&table_path)? {
            let file_entry = file_entry.map_err(|error| {
                io_error("read Parquet table directory entry", &table_path, error)
            })?;
            charge_entry(config, &mut entry_count)?;
            let file_path = file_entry.path();
            let relative = relative_managed_path(root, &file_path, config, &mut path_bytes)?;
            let file_metadata = fs::symlink_metadata(&file_path)
                .map_err(|error| io_error("inspect Parquet candidate entry", &file_path, error))?;
            if file_metadata.file_type().is_symlink() {
                return Err(corruption(format!(
                    "parquet candidate entry is a symbolic link path=[{}]",
                    file_path.display()
                )));
            }
            let Some(file_name) = file_entry.file_name().to_str().map(str::to_owned) else {
                return Err(corruption(format!(
                    "parquet candidate file name is not UTF-8 path=[{}]",
                    file_path.display()
                )));
            };
            if is_candidate_temporary(&file_name) {
                open_existing_candidate(table_id, &file_path)?;
                temporary_files.push(relative);
                continue;
            }
            let Some(identity) = file_name.strip_suffix(".parquet") else {
                validate_owner_security(&file_path, &file_metadata, file_metadata.is_file())?;
                issues.push(ReconciliationIssue {
                    path: relative,
                    kind: ReconciliationIssueKind::UnexpectedEntry,
                    reason: "entry does not use the canonical Parquet file suffix".to_owned(),
                });
                continue;
            };
            let file_id = match canonical_id::<FileId>(identity) {
                Ok(file_id) => file_id,
                Err(reason) => {
                    validate_owner_security(&file_path, &file_metadata, file_metadata.is_file())?;
                    issues.push(ReconciliationIssue {
                        path: relative,
                        kind: ReconciliationIssueKind::UnexpectedEntry,
                        reason,
                    });
                    continue;
                }
            };
            let (_, file_size_bytes) = open_existing_candidate(table_id, &file_path)?;
            candidates.push(PhysicalCandidate {
                file_id,
                table_id,
                path: relative,
                file_size_bytes,
            });
        }
    }
    candidates.sort_unstable_by_key(|candidate| candidate.file_id);
    for pair in candidates.windows(2) {
        if pair[0].file_id == pair[1].file_id {
            return Err(corruption(format!(
                "managed Parquet tree contains duplicate file identity file_id=[{}]",
                pair[0].file_id
            )));
        }
    }
    temporary_files.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    issues.sort_unstable_by(|left, right| left.path.as_str().cmp(right.path.as_str()));
    Ok(PhysicalScan {
        candidates,
        temporary_files,
        issues,
        entry_count,
        path_bytes,
    })
}

fn load_pending_intents(
    data_directory: &Arc<OwnedDataDirectory>,
    config: &StartupOrphanReconcilerConfig,
    scan: &mut PhysicalScan,
) -> Result<PendingIntents> {
    let journal = CommitIntentJournal::new(Arc::clone(data_directory));
    let remaining_entries = config
        .max_scan_entries
        .checked_sub(scan.entry_count)
        .ok_or_else(|| terminal_resource("reconciliation scan entry budget exhausted"))?;
    let remaining_path_bytes = config
        .max_relative_path_bytes
        .checked_sub(scan.path_bytes)
        .ok_or_else(|| terminal_resource("reconciliation path byte budget exhausted"))?;
    let (paths, temporary_paths) =
        journal.scan_read_only(remaining_entries, remaining_path_bytes)?;
    let mut by_file = HashMap::new();
    let mut groups = Vec::with_capacity(paths.len());
    let mut intent_bytes = 0_u64;
    let mut pending_intent_files = 0_usize;
    let mut retained_metadata_bytes = 0_u64;
    for temporary_path in temporary_paths {
        charge_entry(config, &mut scan.entry_count)?;
        charge_managed_path(config, &temporary_path, &mut scan.path_bytes)?;
        scan.temporary_files.push(temporary_path);
    }
    for path in paths {
        charge_entry(config, &mut scan.entry_count)?;
        charge_managed_path(config, &path.managed_path, &mut scan.path_bytes)?;
        let remaining_intent_bytes = config
            .max_intent_bytes
            .checked_sub(intent_bytes)
            .ok_or_else(|| terminal_resource("reconciliation intent byte budget exhausted"))?;
        let remaining_pending_files = config
            .max_pending_intent_files
            .checked_sub(pending_intent_files)
            .ok_or_else(|| terminal_resource("reconciliation pending file budget exhausted"))?;
        let remaining_metadata_bytes = config
            .max_retained_metadata_bytes
            .checked_sub(retained_metadata_bytes)
            .ok_or_else(|| terminal_resource("reconciliation metadata budget exhausted"))?;
        let intent = journal.load_with_limits(
            &path,
            remaining_intent_bytes,
            remaining_pending_files,
            remaining_metadata_bytes,
        )?;
        intent_bytes = intent_bytes
            .checked_add(intent.record_size_bytes)
            .ok_or_else(|| terminal_resource("reconciliation intent byte count overflow"))?;
        pending_intent_files = pending_intent_files
            .checked_add(intent.pending_file_count)
            .ok_or_else(|| terminal_resource("reconciliation pending file count overflow"))?;
        retained_metadata_bytes = retained_metadata_bytes
            .checked_add(intent.pending_file_serialized_bytes)
            .ok_or_else(|| terminal_resource("reconciliation metadata byte count overflow"))?;
        let VersionedCatalogCommand::V1(envelope) = &intent.command;
        let command_fingerprint = envelope.canonical_fingerprint.to_string();
        let mut file_ids = Vec::new();
        for file in command_files(&intent.command) {
            let evidence = PendingIntentEvidence {
                request_id: intent.path.request_id,
                pending: file.clone(),
            };
            if by_file.insert(file.file_id, evidence).is_some() {
                return Err(corruption(format!(
                    "durable intents contain duplicate candidate identity file_id=[{}]",
                    file.file_id
                )));
            }
            file_ids.push(file.file_id);
        }
        groups.push(PendingIntentGroup {
            request_id: intent.path.request_id,
            command_fingerprint,
            file_ids,
        });
    }
    scan.temporary_files
        .sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    Ok(PendingIntents {
        by_file,
        groups,
        retained_metadata_bytes,
    })
}

fn classify_scan(
    data_directory: &OwnedDataDirectory,
    catalog_revision: u64,
    mut scan: PhysicalScan,
    mut intents: PendingIntents,
    catalog_results: Vec<CatalogDataFileProbeResult>,
    max_verification_bytes: u64,
    max_retained_metadata_bytes: u64,
) -> Result<StartupOrphanReconciliationReport> {
    if scan.candidates.len() != catalog_results.len() {
        return Err(corruption(
            "physical candidate and Catalog probe counts do not match",
        ));
    }
    validate_intent_groups(&intents, &scan.candidates, &catalog_results)?;
    let mut files = Vec::with_capacity(scan.candidates.len());
    let mut verification_bytes = 0_u64;
    let mut retained_metadata_bytes = intents.retained_metadata_bytes;
    for (physical, result) in scan.candidates.into_iter().zip(catalog_results) {
        if result.probe != physical.probe() {
            return Err(corruption(
                "catalog probe result does not match its physical candidate",
            ));
        }
        let evidence = intents.by_file.remove(&physical.file_id);
        match result.matched {
            CatalogDataFileProbeMatch::Exact { reference } => {
                if reference.file_size_bytes != physical.file_size_bytes {
                    return Err(corruption(format!(
                        concat!(
                            "catalog file size does not match physical candidate ",
                            "file_id=[{}] expected=[{}] actual=[{}]"
                        ),
                        physical.file_id, reference.file_size_bytes, physical.file_size_bytes
                    )));
                }
                let durable_intent_request_id = if let Some(evidence) = evidence {
                    charge_verification_bytes(
                        &mut verification_bytes,
                        physical.file_size_bytes,
                        max_verification_bytes,
                    )?;
                    verify_pending_candidate(data_directory, &evidence.pending)?;
                    Some(evidence.request_id)
                } else {
                    None
                };
                files.push(ReconciledFile {
                    file_id: physical.file_id,
                    table_id: physical.table_id,
                    path: physical.path,
                    file_size_bytes: physical.file_size_bytes,
                    disposition: ReconciledFileDisposition::CatalogReferenced {
                        state: reference.state,
                        removed_snapshot_version: reference.removed_snapshot_version,
                        durable_intent_request_id,
                    },
                    verified_candidate: None,
                });
            }
            CatalogDataFileProbeMatch::IdentityConflict {
                by_file_id,
                by_path,
            } => {
                return Err(corruption(format!(
                    concat!(
                        "catalog managed file identity conflict file_id=[{}] path=[{}] ",
                        "by_file_id=[{:?}] by_path=[{:?}]"
                    ),
                    physical.file_id,
                    physical.path.as_str(),
                    by_file_id,
                    by_path
                )));
            }
            CatalogDataFileProbeMatch::Unreferenced => {
                if let Some(evidence) = evidence {
                    charge_verification_bytes(
                        &mut verification_bytes,
                        physical.file_size_bytes,
                        max_verification_bytes,
                    )?;
                    verify_pending_candidate(data_directory, &evidence.pending)?;
                    files.push(ReconciledFile {
                        file_id: physical.file_id,
                        table_id: physical.table_id,
                        path: physical.path,
                        file_size_bytes: physical.file_size_bytes,
                        disposition: ReconciledFileDisposition::PendingIntent {
                            request_id: evidence.request_id,
                        },
                        verified_candidate: Some(evidence.pending),
                    });
                    continue;
                }
                charge_verification_bytes(
                    &mut verification_bytes,
                    physical.file_size_bytes,
                    max_verification_bytes,
                )?;
                match recover_parquet_candidate(
                    data_directory,
                    physical.table_id,
                    physical.file_id,
                    physical.path.clone(),
                    physical.file_size_bytes,
                ) {
                    Ok(candidate) => {
                        let pending = candidate.into_pending();
                        charge_retained_metadata_bytes(
                            &mut retained_metadata_bytes,
                            serialized_size(&pending)?,
                            max_retained_metadata_bytes,
                        )?;
                        files.push(ReconciledFile {
                            file_id: physical.file_id,
                            table_id: physical.table_id,
                            path: physical.path,
                            file_size_bytes: physical.file_size_bytes,
                            disposition: ReconciledFileDisposition::Orphan,
                            verified_candidate: Some(pending),
                        });
                    }
                    Err(error) if error.code() == ErrorCode::DataCorruption => {
                        scan.issues.push(ReconciliationIssue {
                            path: physical.path,
                            kind: ReconciliationIssueKind::InvalidUnreferencedCandidate,
                            reason: error.to_string(),
                        });
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    if let Some(missing) = intents.by_file.values().next() {
        return Err(corruption(format!(
            "durable intent references a missing physical candidate file_id=[{}]",
            missing.pending.file_id
        )));
    }
    scan.issues
        .sort_unstable_by(|left, right| left.path.as_str().cmp(right.path.as_str()));
    Ok(StartupOrphanReconciliationReport {
        catalog_revision,
        files,
        temporary_files: scan.temporary_files,
        issues: scan.issues,
    })
}

fn validate_pending_identity(
    physical: &PhysicalCandidate,
    pending: &PendingDataFile,
) -> Result<()> {
    if pending.file_id != physical.file_id
        || pending.table_id != physical.table_id
        || pending.file_size_bytes != physical.file_size_bytes
        || pending.location != DataFileLocation::Managed(physical.path.clone())
    {
        return Err(corruption(format!(
            "durable intent does not match physical candidate file_id=[{}]",
            physical.file_id
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntentCatalogState {
    Exact,
    Unreferenced,
}

fn validate_intent_groups(
    intents: &PendingIntents,
    candidates: &[PhysicalCandidate],
    catalog_results: &[CatalogDataFileProbeResult],
) -> Result<()> {
    let candidate_indexes = candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| (candidate.file_id, index))
        .collect::<HashMap<_, _>>();
    for group in &intents.groups {
        let mut group_state = None;
        for file_id in &group.file_ids {
            let evidence = intents.by_file.get(file_id).ok_or_else(|| {
                corruption(format!(
                    concat!(
                        "durable intent group is missing its file evidence ",
                        "request_id=[{}] file_id=[{}]"
                    ),
                    group.request_id, file_id
                ))
            })?;
            if evidence.request_id != group.request_id {
                return Err(corruption(format!(
                    "durable intent group request identity changed file_id=[{}]",
                    file_id
                )));
            }
            let index = candidate_indexes.get(file_id).copied().ok_or_else(|| {
                corruption(format!(
                    concat!(
                        "durable intent references a missing physical candidate ",
                        "request_id=[{}] file_id=[{}]"
                    ),
                    group.request_id, file_id
                ))
            })?;
            let physical = &candidates[index];
            let result = &catalog_results[index];
            validate_pending_identity(physical, &evidence.pending)?;
            if result.probe != physical.probe() {
                return Err(corruption(
                    "catalog probe result does not match durable intent candidate order",
                ));
            }
            let current_state = match &result.matched {
                CatalogDataFileProbeMatch::Exact { reference } => {
                    validate_catalog_reference(physical, &evidence.pending, reference)?;
                    IntentCatalogState::Exact
                }
                CatalogDataFileProbeMatch::Unreferenced => IntentCatalogState::Unreferenced,
                CatalogDataFileProbeMatch::IdentityConflict { .. } => {
                    return Err(corruption(format!(
                        concat!(
                            "durable intent has a Catalog identity conflict ",
                            "request_id=[{}] file_id=[{}]"
                        ),
                        group.request_id, file_id
                    )));
                }
            };
            if group_state.is_some_and(|state| state != current_state) {
                return Err(corruption(format!(
                    concat!(
                        "atomic durable intent is only partially represented in Catalog ",
                        "request_id=[{}] fingerprint=[{}]"
                    ),
                    group.request_id, group.command_fingerprint
                )));
            }
            group_state = Some(current_state);
        }
        if group_state.is_none() {
            return Err(corruption(format!(
                "durable intent contains no candidate files request_id=[{}]",
                group.request_id
            )));
        }
    }
    Ok(())
}

fn validate_catalog_reference(
    physical: &PhysicalCandidate,
    pending: &PendingDataFile,
    reference: &zippy_types::catalog::CatalogDataFileReference,
) -> Result<()> {
    if reference.file_id != pending.file_id
        || reference.table_id != pending.table_id
        || reference.location != pending.location
        || reference.file_size_bytes != physical.file_size_bytes
        || reference.checksum != pending.checksum
    {
        return Err(corruption(format!(
            "durable intent conflicts with exact Catalog reference file_id=[{}]",
            physical.file_id
        )));
    }
    Ok(())
}

fn read_directory(path: &Path) -> Result<fs::ReadDir> {
    fs::read_dir(path).map_err(|error| io_error("read reconciliation directory", path, error))
}

fn relative_managed_path(
    root: &Path,
    path: &Path,
    config: &StartupOrphanReconcilerConfig,
    path_bytes: &mut usize,
) -> Result<ManagedDataPath> {
    let relative = path.strip_prefix(root).map_err(|_| {
        corruption(format!(
            "reconciliation path escaped the managed root path=[{}]",
            path.display()
        ))
    })?;
    let value = relative.to_str().ok_or_else(|| {
        corruption(format!(
            "reconciliation path is not UTF-8 path=[{}]",
            path.display()
        ))
    })?;
    *path_bytes = path_bytes
        .checked_add(value.len())
        .ok_or_else(|| terminal_resource("reconciliation relative path byte count overflow"))?;
    if *path_bytes > config.max_relative_path_bytes {
        return Err(terminal_resource(format!(
            "reconciliation relative paths exceed byte budget actual=[{}] maximum=[{}]",
            *path_bytes, config.max_relative_path_bytes
        )));
    }
    ManagedDataPath::new(value.to_owned()).map_err(|error| {
        corruption(format!(
            "reconciliation managed path is invalid path=[{value}] error=[{error}]"
        ))
    })
}

fn charge_managed_path(
    config: &StartupOrphanReconcilerConfig,
    path: &ManagedDataPath,
    path_bytes: &mut usize,
) -> Result<()> {
    *path_bytes = path_bytes
        .checked_add(path.as_str().len())
        .ok_or_else(|| terminal_resource("reconciliation relative path byte count overflow"))?;
    if *path_bytes > config.max_relative_path_bytes {
        return Err(terminal_resource(format!(
            "reconciliation relative paths exceed byte budget actual=[{}] maximum=[{}]",
            *path_bytes, config.max_relative_path_bytes
        )));
    }
    Ok(())
}

fn canonical_id<T>(value: &str) -> std::result::Result<T, String>
where
    T: FromStr + ToString,
    T::Err: std::fmt::Display,
{
    let parsed = value
        .parse::<T>()
        .map_err(|error| format!("entry identity is invalid value=[{value}] error=[{error}]"))?;
    if parsed.to_string() != value {
        return Err(format!(
            "entry identity is not in canonical lowercase form value=[{value}]"
        ));
    }
    Ok(parsed)
}

fn is_candidate_temporary(file_name: &str) -> bool {
    file_name.starts_with('.') && file_name.contains(".parquet.tmp-")
}

#[cfg(test)]
mod tests {
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    use tempfile::TempDir;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_types::catalog::CatalogPublicationControl;

    use super::*;

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_cleanup_retains_exclusion_until_blocking_unlink_finishes() {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new().expect("create test root");
        let data_directory = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(root.path().join("data"))
                .expect("own test data root"),
        );
        let catalog = Arc::new(
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
                .expect("open test Catalog"),
        );
        let target =
            ManagedDataPath::new("wal/catalog-commit-intents/00000000000000000001-test.json")
                .expect("valid test intent path");
        let transaction = data_directory
            .begin_durable_file(&target)
            .expect("create temporary test intent");
        let temporary_path = transaction.temporary_path().to_path_buf();
        drop(transaction);
        fs::write(&temporary_path, b"crash residue").expect("restore closed temporary intent");
        fs::set_permissions(&temporary_path, fs::Permissions::from_mode(0o600))
            .expect("secure temporary intent permissions");

        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let started_sender = Arc::new(Mutex::new(Some(started_sender)));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_sender = Arc::clone(&started_sender);
        let hook_release = Arc::clone(&release);
        let hook = Arc::new(move || {
            if let Some(sender) = hook_sender.lock().expect("lock start signal").take() {
                let _ = sender.send(());
            }
            let (released, condition) = &*hook_release;
            let mut released = released.lock().expect("lock cleanup release");
            while !*released {
                released = condition.wait(released).expect("wait for cleanup release");
            }
        });
        let reconciler = StartupOrphanReconciler::new_with_cleanup_control(
            Arc::clone(&data_directory),
            Arc::clone(&catalog),
        )
        .with_before_cleanup_unlink(hook);
        let cleanup = tokio::spawn(async move {
            reconciler
                .reconcile_and_remove_orphans_before_admission()
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started_receiver)
            .await
            .expect("cleanup must reach the blocking unlink task")
            .expect("cleanup start signal must be delivered");

        cleanup.abort();
        assert!(cleanup
            .await
            .expect_err("outer cleanup future must be cancelled")
            .is_cancelled());
        let permit_catalog = Arc::clone(&catalog);
        let mut permit =
            tokio::spawn(async move { permit_catalog.acquire_publication_permit().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut permit)
                .await
                .is_err(),
            "publication must remain excluded while detached unlink is blocked"
        );

        let (released, condition) = &*release;
        *released.lock().expect("lock cleanup release") = true;
        condition.notify_all();
        let acquired = tokio::time::timeout(Duration::from_secs(2), &mut permit)
            .await
            .expect("publication resumes after unlink finishes")
            .expect("join publication permit task")
            .expect("acquire publication permit");
        drop(acquired);
        assert!(!temporary_path.exists());
    }
}

fn charge_entry(config: &StartupOrphanReconcilerConfig, count: &mut usize) -> Result<()> {
    *count = count
        .checked_add(1)
        .ok_or_else(|| terminal_resource("reconciliation scan entry count overflow"))?;
    if *count > config.max_scan_entries {
        return Err(terminal_resource(format!(
            "reconciliation scan entry count exceeds maximum actual=[{}] maximum=[{}]",
            *count, config.max_scan_entries
        )));
    }
    Ok(())
}

fn charge_verification_bytes(current: &mut u64, additional: u64, maximum: u64) -> Result<()> {
    *current = current
        .checked_add(additional)
        .ok_or_else(|| terminal_resource("reconciliation verification byte count overflow"))?;
    if *current > maximum {
        return Err(terminal_resource(format!(
            "reconciliation verification bytes exceed maximum actual=[{}] maximum=[{}]",
            *current, maximum
        )));
    }
    Ok(())
}

fn charge_retained_metadata_bytes(current: &mut u64, additional: u64, maximum: u64) -> Result<()> {
    *current = current
        .checked_add(additional)
        .ok_or_else(|| terminal_resource("reconciliation retained metadata byte count overflow"))?;
    if *current > maximum {
        return Err(terminal_resource(format!(
            concat!(
                "reconciliation retained metadata exceeds byte budget ",
                "actual=[{}] maximum=[{}]"
            ),
            *current, maximum
        )));
    }
    Ok(())
}

#[derive(Default)]
struct SerializedByteCounter {
    bytes: u64,
}

impl Write for SerializedByteCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let additional = u64::try_from(buffer.len()).map_err(|_| {
            std::io::Error::other("serialized metadata chunk exceeds addressable bytes")
        })?;
        self.bytes = self
            .bytes
            .checked_add(additional)
            .ok_or_else(|| std::io::Error::other("serialized metadata byte count overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialized_size(value: &impl Serialize) -> Result<u64> {
    let mut counter = SerializedByteCounter::default();
    serde_json::to_writer(&mut counter, value).map_err(|error| {
        terminal_resource(format!(
            "measure retained reconciliation metadata failed error=[{error}]"
        ))
    })?;
    Ok(counter.bytes)
}

fn validate_config(config: &StartupOrphanReconcilerConfig) -> Result<()> {
    if config.max_scan_entries == 0 || config.max_scan_entries > MAX_SCAN_ENTRIES {
        return Err(terminal_invalid(format!(
            "reconciliation scan entry budget is outside supported range actual=[{}] maximum=[{}]",
            config.max_scan_entries, MAX_SCAN_ENTRIES
        )));
    }
    if config.probe_batch_size == 0 || config.probe_batch_size > MAX_PROBE_BATCH_SIZE {
        return Err(terminal_invalid(format!(
            "reconciliation probe batch size is outside supported range actual=[{}] maximum=[{}]",
            config.probe_batch_size, MAX_PROBE_BATCH_SIZE
        )));
    }
    if config.max_relative_path_bytes == 0
        || config.max_relative_path_bytes > MAX_RELATIVE_PATH_BYTES
    {
        return Err(terminal_invalid(format!(
            "reconciliation path byte budget is outside supported range actual=[{}] maximum=[{}]",
            config.max_relative_path_bytes, MAX_RELATIVE_PATH_BYTES
        )));
    }
    if config.max_verification_bytes == 0 || config.max_verification_bytes > MAX_VERIFICATION_BYTES
    {
        return Err(terminal_invalid(format!(
            concat!(
                "reconciliation verification byte budget is outside supported range ",
                "actual=[{}] maximum=[{}]"
            ),
            config.max_verification_bytes, MAX_VERIFICATION_BYTES
        )));
    }
    if config.max_intent_bytes == 0 || config.max_intent_bytes > MAX_INTENT_BYTES {
        return Err(terminal_invalid(format!(
            "reconciliation intent byte budget is outside supported range actual=[{}] maximum=[{}]",
            config.max_intent_bytes, MAX_INTENT_BYTES
        )));
    }
    if config.max_pending_intent_files == 0
        || config.max_pending_intent_files > MAX_PENDING_INTENT_FILES
    {
        return Err(terminal_invalid(format!(
            concat!(
                "reconciliation pending intent file budget is outside supported range ",
                "actual=[{}] maximum=[{}]"
            ),
            config.max_pending_intent_files, MAX_PENDING_INTENT_FILES
        )));
    }
    if config.max_retained_metadata_bytes == 0
        || config.max_retained_metadata_bytes > MAX_RETAINED_METADATA_BYTES
    {
        return Err(terminal_invalid(format!(
            concat!(
                "reconciliation retained metadata budget is outside supported range ",
                "actual=[{}] maximum=[{}]"
            ),
            config.max_retained_metadata_bytes, MAX_RETAINED_METADATA_BYTES
        )));
    }
    Ok(())
}

pub(crate) fn validate_directory(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(corruption(format!(
            "reconciliation directory is not a non-symlink directory path=[{}]",
            path.display()
        )));
    }
    validate_owner_security(path, metadata, false)
}

#[cfg(unix)]
fn validate_owner_security(
    path: &Path,
    metadata: &fs::Metadata,
    require_one_link: bool,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let current_uid = rustix::process::geteuid().as_raw();
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != current_uid
        || (require_one_link && metadata.nlink() != 1)
        || mode & 0o077 != 0
    {
        return Err(corruption(format!(
            "reconciliation entry ownership, links, or permissions are invalid path=[{}]",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_owner_security(
    _path: &Path,
    _metadata: &fs::Metadata,
    _require_one_link: bool,
) -> Result<()> {
    Ok(())
}

fn join_error(operation: &str, error: tokio::task::JoinError) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("{operation} blocking task failed error=[{error}]"),
    )
    .with_retryable(false)
}

fn revision_changed(expected: u64, actual: u64) -> ServerError {
    ServerError::new(
        ErrorCode::Conflict,
        format!(
            concat!(
                "catalog revision changed during startup reconciliation ",
                "expected=[{}] actual=[{}]"
            ),
            expected, actual
        ),
    )
    .with_retryable(true)
}

fn startup_temporary_cleanup_error(
    path: &ManagedDataPath,
    error: DataDirectoryError,
) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!(
            "remove startup temporary file failed path=[{}] error=[{}]",
            path.as_str(),
            error
        ),
    )
    .with_retryable(false)
}

fn reject_invalid_unreferenced_candidates(
    report: &StartupOrphanReconciliationReport,
) -> Result<()> {
    if let Some(issue) = report
        .issues
        .iter()
        .find(|issue| issue.kind == ReconciliationIssueKind::InvalidUnreferencedCandidate)
    {
        return Err(corruption(format!(
            "invalid unreferenced candidate blocks startup cleanup path=[{}] reason=[{}]",
            issue.path.as_str(),
            issue.reason
        )));
    }
    Ok(())
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "{operation} failed path=[{}] error=[{}]",
            path.display(),
            error
        ),
    )
}

fn terminal_invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn terminal_resource(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}
