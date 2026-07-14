//! Composition root for the incremental single-node server refactor.
//!
//! Phase 1 keeps the existing master and gateway implementations behind the
//! stable service contracts. Capability metadata makes every compatibility
//! limitation explicit so callers cannot mistake an adapter for the future
//! SQLite catalog or DataFusion query engine.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod catalog_shadow;
mod composition;
mod health;
mod legacy_catalog_migration;
mod legacy_query;
mod native_storage;
mod runtime;

pub use composition::LegacyServerServices;
pub use health::{LegacyServerCapabilities, LegacyServerHealth, ServiceHealth};
pub use legacy_catalog_migration::{
    execute_legacy_catalog_metadata_import, plan_legacy_catalog_metadata_import,
    LegacyCatalogCommandDisposition, LegacyCatalogMetadataImportPlan,
    LegacyCatalogMetadataMigrationReport, LegacyCatalogMigrationDegradation,
    LegacyCatalogMigrationDegradedReason, LegacyCatalogMigrationState,
    LegacyCatalogMigrationStatus, LegacyCatalogTableMigrationOutcome,
    LegacyCatalogTableMigrationReport, LegacyCatalogTablePlan, LegacyCatalogTablePlanStatus,
    LegacyCatalogV2ImportRequirement, LEGACY_CATALOG_METADATA_MIGRATION_VERSION,
    LEGACY_CATALOG_TARGET_REVISION_BASE, LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE,
    LEGACY_MIGRATION_VERSION_CONFIG_KEY, LEGACY_REVISION_BASE_CONFIG_KEY,
    LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY, LEGACY_STREAM_NAME_CONFIG_KEY,
    NATIVE_SNAPSHOT_BASE_CONFIG_KEY,
};
pub use legacy_query::{
    LegacyGatewayPlan, LegacyQueryEngineAdapter, LegacyQueryEngineConfig,
    LEGACY_GATEWAY_PLAN_FORMAT_VERSION,
};
pub use native_storage::{
    NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    NativeStorageStartOutcome, NativeStorageStartup,
};
pub use runtime::{
    run_server_daemon, LegacyServerMode, LegacyServerReadiness, LegacyServerRuntime,
    LegacyServerRuntimeConfig, LegacyServerRuntimeHealth, LegacyServerShutdownOutcome,
};
