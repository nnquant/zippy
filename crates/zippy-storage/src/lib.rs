//! Durable Parquet, manifest, WAL, and live-snapshot storage for one Zippy server.
//!
//! Native persistence first writes immutable Parquet candidates to affine
//! temporary-file tokens. The complete Catalog envelope is probed for durable
//! idempotency before publication; only a missing current-snapshot advance may
//! publish, journal, and commit. Commit-unknown responses replay the unchanged
//! envelope, and rejected publications are removed only with exclusive cleanup
//! authority plus fresh Catalog and physical-identity verification.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod candidate;
mod catalog_verify;
mod checksum;
mod commit;
mod commit_intent;
mod persist;
mod reconcile;

pub use candidate::{
    DurableParquetCandidate, ParquetCandidateSpec, ParquetCandidateWriter,
    CANDIDATE_FOOTER_METADATA_KEY, PARQUET_CANDIDATE_FORMAT_VERSION,
};
pub use catalog_verify::{
    StartupActiveTableBootstrapDescriptor, StartupCatalogBootstrap,
    StartupCatalogFileVerificationReport, StartupCatalogFileVerifier,
    StartupCatalogFileVerifierConfig,
};
pub use commit::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, CommitDataFilesReceipt,
};
pub use persist::{
    PersistCandidateInput, PersistCompletionFailure, PersistCompletionResult, PersistController,
    PersistControllerConfig, PersistControllerFailureMonitor, PersistControllerHandle, PersistMode,
    PersistRequest, PersistTicket, MAX_PERSIST_PARALLEL_WRITERS,
};
pub use reconcile::{
    ReconciledFile, ReconciledFileDisposition, ReconciliationIssue, ReconciliationIssueKind,
    StartupOrphanReconciler, StartupOrphanReconcilerConfig, StartupOrphanReconciliationReport,
};
