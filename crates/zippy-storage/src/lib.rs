//! Durable Parquet, manifest, WAL, and live-snapshot storage for one Zippy server.
//!
//! The first implementation slice exposes only immutable Parquet candidates.
//! A candidate is fully written, checksummed, fsynced, and published before it
//! can be handed to the Catalog. Catalog visibility remains a separate atomic
//! transaction.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod candidate;
mod checksum;

pub use candidate::{
    DurableParquetCandidate, ParquetCandidateSpec, ParquetCandidateWriter,
    CANDIDATE_FOOTER_METADATA_KEY, PARQUET_CANDIDATE_FORMAT_VERSION,
};
