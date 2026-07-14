//! Bounded startup verification of Catalog-visible managed Parquet files.

use std::num::NonZeroU32;
use std::sync::Arc;

use zippy_core::OwnedDataDirectory;
use zippy_types::catalog::{
    CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector, ListTablesRequest,
};
use zippy_types::table::DataFileLocation;
use zippy_types::{ErrorCode, Result, ServerError, TableId};

use crate::candidate::verify_catalog_visible_candidate;

const DEFAULT_MAX_TABLES: usize = 65_536;
const DEFAULT_TABLE_PAGE_SIZE: u32 = 512;
const DEFAULT_SNAPSHOT_BATCH_SIZE: usize = 512;
const DEFAULT_MAX_MANAGED_FILES: usize = 65_536;
const DEFAULT_MAX_VERIFICATION_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_TABLES: usize = 1_000_000;
const MAX_TABLE_PAGE_SIZE: u32 = 4_096;
const MAX_SNAPSHOT_BATCH_SIZE: usize = 4_096;
const MAX_MANAGED_FILES: usize = 1_000_000;
const MAX_VERIFICATION_BYTES: u64 = 64 * 1024 * 1024 * 1024 * 1024;

/// Resource bounds for one Catalog-to-physical startup verification pass.
#[derive(Debug, Clone)]
pub struct StartupCatalogFileVerifierConfig {
    max_tables: usize,
    table_page_size: u32,
    snapshot_batch_size: usize,
    max_managed_files: usize,
    max_verification_bytes: u64,
}

impl Default for StartupCatalogFileVerifierConfig {
    fn default() -> Self {
        Self {
            max_tables: DEFAULT_MAX_TABLES,
            table_page_size: DEFAULT_TABLE_PAGE_SIZE,
            snapshot_batch_size: DEFAULT_SNAPSHOT_BATCH_SIZE,
            max_managed_files: DEFAULT_MAX_MANAGED_FILES,
            max_verification_bytes: DEFAULT_MAX_VERIFICATION_BYTES,
        }
    }
}

impl StartupCatalogFileVerifierConfig {
    /// Creates the bounded default startup verification policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum active tables admitted by one verification pass.
    #[must_use]
    pub const fn with_max_tables(mut self, maximum: usize) -> Self {
        self.max_tables = maximum;
        self
    }

    /// Sets the bounded Catalog table listing page size.
    #[must_use]
    pub const fn with_table_page_size(mut self, size: u32) -> Self {
        self.table_page_size = size;
        self
    }

    /// Sets the number of current table snapshots read in one Catalog transaction.
    #[must_use]
    pub const fn with_snapshot_batch_size(mut self, size: usize) -> Self {
        self.snapshot_batch_size = size;
        self
    }

    /// Sets the maximum current visible managed files admitted by one pass.
    #[must_use]
    pub const fn with_max_managed_files(mut self, maximum: usize) -> Self {
        self.max_managed_files = maximum;
        self
    }

    /// Sets the aggregate bytes checksummed and footer-validated by one pass.
    #[must_use]
    pub const fn with_max_verification_bytes(mut self, maximum: u64) -> Self {
        self.max_verification_bytes = maximum;
        self
    }
}

/// Revision-stable evidence from one Catalog-to-physical startup verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupCatalogFileVerificationReport {
    /// Catalog revision shared by table listing, snapshot reads, and the final guard.
    pub catalog_revision: u64,
    /// Number of active current table snapshots inspected.
    pub table_count: usize,
    /// Number of current visible managed files fully verified.
    pub managed_file_count: usize,
    /// Aggregate physical bytes checksummed and footer-validated.
    pub verified_bytes: u64,
}

/// Verifies every current Catalog-visible managed file before read admission opens.
pub struct StartupCatalogFileVerifier {
    config: StartupCatalogFileVerifierConfig,
    data_directory: Arc<OwnedDataDirectory>,
    catalog: Arc<dyn CatalogRead>,
}

impl StartupCatalogFileVerifier {
    /// Creates a verifier using conservative table, file, and byte budgets.
    #[must_use]
    pub fn new(data_directory: Arc<OwnedDataDirectory>, catalog: Arc<dyn CatalogRead>) -> Self {
        Self {
            config: StartupCatalogFileVerifierConfig::default(),
            data_directory,
            catalog,
        }
    }

    /// Creates a verifier with explicit startup resource budgets.
    pub fn with_config(
        config: StartupCatalogFileVerifierConfig,
        data_directory: Arc<OwnedDataDirectory>,
        catalog: Arc<dyn CatalogRead>,
    ) -> Result<Self> {
        validate_config(&config)?;
        Ok(Self {
            config,
            data_directory,
            catalog,
        })
    }

    /// Reads current snapshots and verifies every visible managed Parquet descriptor.
    pub async fn verify(&self) -> Result<StartupCatalogFileVerificationReport> {
        validate_config(&self.config)?;
        let page_limit = NonZeroU32::new(self.config.table_page_size)
            .expect("validated table page size is nonzero");
        let mut catalog_revision = None;
        let mut after = None;
        let mut table_count = 0_usize;
        let mut managed_file_count = 0_usize;
        let mut verified_bytes = 0_u64;

        loop {
            let page = self
                .catalog
                .list_tables(ListTablesRequest {
                    namespace_id: None,
                    after,
                    limit: page_limit,
                })
                .await?;
            bind_revision(&mut catalog_revision, page.catalog_revision)?;
            validate_table_page(after, page.next_after, &page.tables, page_limit)?;

            table_count = table_count
                .checked_add(page.tables.len())
                .ok_or_else(|| resource("startup Catalog table count overflow"))?;
            if table_count > self.config.max_tables {
                return Err(resource(format!(
                    "startup Catalog table count exceeds maximum actual=[{}] maximum=[{}]",
                    table_count, self.config.max_tables
                )));
            }

            let selectors = page
                .tables
                .iter()
                .map(|table| CatalogSnapshotSelector {
                    table_id: table.table_id,
                    snapshot_version: table.current_snapshot_version,
                    schema_version: table.current_schema_version,
                })
                .collect::<Vec<_>>();
            for batch in selectors.chunks(self.config.snapshot_batch_size) {
                let snapshot = self.catalog.read_snapshot_at(batch).await?;
                bind_revision(&mut catalog_revision, snapshot.catalog_revision())?;
                validate_snapshot_response(batch, &snapshot)?;
                let (batch_files, batch_bytes) = measure_managed_files(&snapshot)?;
                let next_files = managed_file_count
                    .checked_add(batch_files)
                    .ok_or_else(|| resource("startup managed file count overflow"))?;
                if next_files > self.config.max_managed_files {
                    return Err(resource(format!(
                        concat!(
                            "startup visible managed file count exceeds maximum ",
                            "actual=[{}] maximum=[{}]"
                        ),
                        next_files, self.config.max_managed_files
                    )));
                }
                let next_bytes = verified_bytes
                    .checked_add(batch_bytes)
                    .ok_or_else(|| resource("startup verification byte count overflow"))?;
                if next_bytes > self.config.max_verification_bytes {
                    return Err(resource(format!(
                        concat!(
                            "startup Catalog file verification bytes exceed maximum ",
                            "actual=[{}] maximum=[{}]"
                        ),
                        next_bytes, self.config.max_verification_bytes
                    )));
                }

                let data_directory = Arc::clone(&self.data_directory);
                tokio::task::spawn_blocking(move || {
                    verify_snapshot_files(data_directory.as_ref(), &snapshot)
                })
                .await
                .map_err(|error| join_error("verify Catalog-visible managed files", error))??;
                managed_file_count = next_files;
                verified_bytes = next_bytes;
            }

            match page.next_after {
                Some(next_after) => after = Some(next_after),
                None => break,
            }
        }

        let catalog_revision =
            catalog_revision.expect("the first table page always has a revision");
        let ending_revision = self.catalog.revision().await?;
        if ending_revision != catalog_revision {
            return Err(revision_changed(catalog_revision, ending_revision));
        }
        Ok(StartupCatalogFileVerificationReport {
            catalog_revision,
            table_count,
            managed_file_count,
            verified_bytes,
        })
    }
}

fn bind_revision(expected: &mut Option<u64>, actual: u64) -> Result<()> {
    match *expected {
        Some(expected) if expected != actual => Err(revision_changed(expected, actual)),
        Some(_) => Ok(()),
        None => {
            *expected = Some(actual);
            Ok(())
        }
    }
}

fn validate_table_page(
    after: Option<TableId>,
    next_after: Option<TableId>,
    tables: &[zippy_types::table::TableDescriptor],
    limit: NonZeroU32,
) -> Result<()> {
    if tables.len() > limit.get() as usize {
        return Err(corruption("catalog table page exceeds its requested limit"));
    }
    if tables
        .windows(2)
        .any(|pair| pair[0].table_id >= pair[1].table_id)
        || tables
            .first()
            .is_some_and(|table| after.is_some_and(|after| table.table_id <= after))
    {
        return Err(corruption(
            "catalog table page is not strictly ordered after its cursor",
        ));
    }
    if let Some(next_after) = next_after {
        if tables.last().map(|table| table.table_id) != Some(next_after) {
            return Err(corruption(
                "catalog table continuation cursor does not match the page tail",
            ));
        }
    }
    Ok(())
}

fn validate_snapshot_response(
    selectors: &[CatalogSnapshotSelector],
    snapshot: &CatalogQuerySnapshot,
) -> Result<()> {
    if selectors.len() != snapshot.tables().len()
        || !selectors
            .iter()
            .zip(snapshot.tables())
            .all(|(selector, table)| selector == table.selector())
    {
        return Err(corruption(
            "catalog current snapshot response is not dense or selector-aligned",
        ));
    }
    Ok(())
}

fn measure_managed_files(snapshot: &CatalogQuerySnapshot) -> Result<(usize, u64)> {
    let mut files = 0_usize;
    let mut bytes = 0_u64;
    for table in snapshot.tables() {
        for descriptor in table.files().as_slice() {
            if matches!(descriptor.file.location, DataFileLocation::Managed(_)) {
                files = files
                    .checked_add(1)
                    .ok_or_else(|| resource("startup managed file count overflow"))?;
                bytes = bytes
                    .checked_add(descriptor.file.file_size_bytes)
                    .ok_or_else(|| resource("startup verification byte count overflow"))?;
            }
        }
    }
    Ok((files, bytes))
}

fn verify_snapshot_files(
    data_directory: &OwnedDataDirectory,
    snapshot: &CatalogQuerySnapshot,
) -> Result<()> {
    for table in snapshot.tables() {
        for descriptor in table.files().as_slice() {
            if matches!(descriptor.file.location, DataFileLocation::Managed(_)) {
                verify_catalog_visible_candidate(data_directory, &descriptor.file)?;
            }
        }
    }
    Ok(())
}

fn validate_config(config: &StartupCatalogFileVerifierConfig) -> Result<()> {
    if config.max_tables == 0 || config.max_tables > MAX_TABLES {
        return Err(invalid(format!(
            "startup table budget is outside supported range actual=[{}] maximum=[{}]",
            config.max_tables, MAX_TABLES
        )));
    }
    if config.table_page_size == 0 || config.table_page_size > MAX_TABLE_PAGE_SIZE {
        return Err(invalid(format!(
            "startup table page size is outside supported range actual=[{}] maximum=[{}]",
            config.table_page_size, MAX_TABLE_PAGE_SIZE
        )));
    }
    if config.snapshot_batch_size == 0 || config.snapshot_batch_size > MAX_SNAPSHOT_BATCH_SIZE {
        return Err(invalid(format!(
            concat!(
                "startup snapshot batch size is outside supported range ",
                "actual=[{}] maximum=[{}]"
            ),
            config.snapshot_batch_size, MAX_SNAPSHOT_BATCH_SIZE
        )));
    }
    if config.max_managed_files == 0 || config.max_managed_files > MAX_MANAGED_FILES {
        return Err(invalid(format!(
            "startup file budget is outside supported range actual=[{}] maximum=[{}]",
            config.max_managed_files, MAX_MANAGED_FILES
        )));
    }
    if config.max_verification_bytes == 0 || config.max_verification_bytes > MAX_VERIFICATION_BYTES
    {
        return Err(invalid(format!(
            concat!(
                "startup verification byte budget is outside supported range ",
                "actual=[{}] maximum=[{}]"
            ),
            config.max_verification_bytes, MAX_VERIFICATION_BYTES
        )));
    }
    Ok(())
}

fn revision_changed(expected: u64, actual: u64) -> ServerError {
    ServerError::new(
        ErrorCode::Conflict,
        format!(
            concat!(
                "catalog revision changed during startup verification ",
                "expected=[{}] actual=[{}]"
            ),
            expected, actual
        ),
    )
    .with_retryable(true)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}

fn resource(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn join_error(operation: &str, error: tokio::task::JoinError) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("{operation} blocking task failed error=[{error}]"),
    )
    .with_retryable(false)
}
