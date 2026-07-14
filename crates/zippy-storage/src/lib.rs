//! Durable Parquet, manifest, WAL, and live-snapshot storage for one Zippy server.
//!
//! Immutable Parquet candidates are fully written, checksummed, fsynced, and
//! published before Catalog admission. A bounded single-worker coordinator
//! then journals the frozen Catalog envelope, retries commit-unknown responses
//! with the same request ID, and removes the intent only after the atomic
//! snapshot response is validated.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod candidate;
mod checksum;
mod commit;
mod commit_intent;

pub use candidate::{
    DurableParquetCandidate, ParquetCandidateSpec, ParquetCandidateWriter,
    CANDIDATE_FOOTER_METADATA_KEY, PARQUET_CANDIDATE_FORMAT_VERSION,
};
pub use commit::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, CommitDataFilesIntent,
    CommitDataFilesReceipt,
};
