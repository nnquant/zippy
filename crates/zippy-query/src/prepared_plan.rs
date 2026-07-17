//! Bounded, capability-free preparation of persisted SQL physical plans.

use std::collections::HashSet;
use std::fmt;
use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog_common::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::datasource::physical_plan::parquet::ParquetExec;
use datafusion::datasource::source_as_provider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::parser::{DFParser, Statement as DataFusionStatement};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Distinct, Expr as SqlExpr, FunctionArgumentClause, FunctionArguments,
    GroupByExpr, Ident, JoinConstraint, JoinOperator, Query as SqlQuery, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, Value as SqlValue, Visit, Visitor,
};
use sha2::{Digest, Sha256};

use crate::active_plan::ActiveSegmentExec;
use crate::persisted_plan::{
    bind_persisted_snapshot_plan, is_exact_planning_active, is_exact_planning_parquet,
};
use crate::{
    ActiveSegmentOpener, BoundPersistedPlan, PersistedFileOpener, PersistedSnapshotDescription,
    PersistedTableDescription, QuerySessionPolicy, ZippyTableProvider,
};

const PROFILE_FORMAT_VERSION: u32 = 9;
const PREFLIGHT_FIXED_TRANSIENT_BYTES: u64 = 2 * 1024 * 1024;
const PREFLIGHT_BYTES_PER_SQL_BYTE: u64 = 1024;
const PREFLIGHT_BYTES_PER_BINDING: u64 = 64 * 1024;
const PREFLIGHT_BYTES_PER_RELATION: u64 = 16 * 1024;
const PREFLIGHT_BYTES_PER_LOGICAL_NODE: u64 = 4 * 1024;
const PREFLIGHT_BYTES_PER_EXPRESSION_NODE: u64 = 4 * 1024;
const RETAINED_FIXED_BYTES: u64 = 1024 * 1024;
const RETAINED_BYTES_PER_TEXT_BYTE: u64 = 32;
const RETAINED_BYTES_PER_BINDING: u64 = 16 * 1024;
const RETAINED_BYTES_PER_EXPANDED_FILE: u64 = 1024;
const RETAINED_BYTES_PER_PHYSICAL_NODE: u64 = 8 * 1024;
const RETAINED_BYTES_PER_EXPRESSION_NODE: u64 = 2 * 1024;
const RETAINED_SCHEMA_EXPANSION_FACTOR: u64 = 2;
const RETAINED_SCHEMA_BYTES_PER_PHYSICAL_NODE: u64 = 1;
const BUILD_FIXED_TRANSIENT_BYTES: u64 = 8 * 1024 * 1024;
const BUILD_BYTES_PER_TEXT_BYTE: u64 = 128;
const BUILD_BYTES_PER_BINDING: u64 = 64 * 1024;
const BUILD_BYTES_PER_EXPANDED_FILE: u64 = 1024;
const BUILD_BYTES_PER_LOGICAL_NODE: u64 = 8 * 1024;
const BUILD_BYTES_PER_PHYSICAL_NODE: u64 = 8 * 1024;
const BUILD_BYTES_PER_EXPRESSION_NODE: u64 = 2 * 1024;
const BUILD_SCHEMA_TRANSIENT_FACTOR: u64 = 2;
const BUILD_SCHEMA_BYTES_PER_LOGICAL_NODE: u64 = 1;
const BUILD_SCHEMA_BYTES_PER_TRANSIENT_PHYSICAL_NODE: u64 = 1;
const GENERATED_SCHEMA_BYTES_PER_EXPRESSION: u64 = 512;
const GENERATED_SCHEMA_TEXT_EXPANSION_FACTOR: u64 = 4;
const BUILD_LOGICAL_NODE_EXPANSION_FACTOR: usize = 8;
const BUILD_LOGICAL_NODES_PER_EXPRESSION_NODE: usize = 8;
const BUILD_PHYSICAL_NODES_PER_VALIDATED_LOGICAL_NODE: usize = 8;
const BUILD_EXPRESSION_NODE_EXPANSION_FACTOR: usize = 8;
const BUILD_EXPRESSION_NODES_PER_TEXT_BYTE: usize = 2;

/// One SQL-visible name bound to an exact table ordinal in a projected snapshot manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTableBinding {
    sql_name: Box<str>,
    table_index: usize,
}

impl PersistedTableBinding {
    /// Creates a table binding without parsing or allocating a DataFusion catalog.
    pub fn try_new(sql_name: impl Into<String>, table_index: usize) -> Result<Self> {
        let sql_name = sql_name.into();
        if sql_name.trim().is_empty() {
            return Err(plan_error("persisted SQL table binding name is empty"));
        }
        Ok(Self {
            sql_name: sql_name.into_boxed_str(),
            table_index,
        })
    }

    /// Returns the SQL identifier, including any caller-supplied qualification or quoting.
    #[must_use]
    pub fn sql_name(&self) -> &str {
        &self.sql_name
    }

    /// Returns the exact ordinal in the projected snapshot manifest.
    #[must_use]
    pub const fn table_index(&self) -> usize {
        self.table_index
    }
}

/// Structural limits applied before and after persisted SQL planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedPlanLimits {
    max_sql_bytes: usize,
    max_bindings: usize,
    max_relation_occurrences: usize,
    max_logical_nodes: usize,
    max_physical_nodes: usize,
    max_expression_nodes: usize,
}

impl PersistedPlanLimits {
    /// Creates nonzero planning limits.
    pub fn try_new(
        max_sql_bytes: usize,
        max_bindings: usize,
        max_relation_occurrences: usize,
        max_logical_nodes: usize,
        max_physical_nodes: usize,
        max_expression_nodes: usize,
    ) -> Result<Self> {
        if [
            max_sql_bytes,
            max_bindings,
            max_relation_occurrences,
            max_logical_nodes,
            max_physical_nodes,
            max_expression_nodes,
        ]
        .contains(&0)
        {
            return Err(plan_error("persisted plan limits must all be nonzero"));
        }
        Ok(Self {
            max_sql_bytes,
            max_bindings,
            max_relation_occurrences,
            max_logical_nodes,
            max_physical_nodes,
            max_expression_nodes,
        })
    }

    /// Returns the maximum accepted SQL byte length.
    #[must_use]
    pub const fn max_sql_bytes(self) -> usize {
        self.max_sql_bytes
    }

    /// Returns the maximum number of registered persisted tables.
    #[must_use]
    pub const fn max_bindings(self) -> usize {
        self.max_bindings
    }

    /// Returns the maximum pre-optimization table-scan occurrence count.
    #[must_use]
    pub const fn max_relation_occurrences(self) -> usize {
        self.max_relation_occurrences
    }

    /// Returns the maximum number of logical plan nodes.
    #[must_use]
    pub const fn max_logical_nodes(self) -> usize {
        self.max_logical_nodes
    }

    /// Returns the maximum number of physical plan nodes.
    #[must_use]
    pub const fn max_physical_nodes(self) -> usize {
        self.max_physical_nodes
    }

    /// Returns the maximum number of logical expression nodes.
    #[must_use]
    pub const fn max_expression_nodes(self) -> usize {
        self.max_expression_nodes
    }

    /// Returns the conservative task-memory peak for the first SQL preflight pass.
    ///
    /// This bound is computable without parsing SQL, so a server can admit it before
    /// [`profile_persisted_plan`] constructs an AST. The exact physical-build reservation is
    /// still derived from the resulting [`PersistedPlanProfile`] and admitted separately.
    pub fn preflight_peak_bytes(self) -> Result<NonZeroU64> {
        let components = [
            (
                self.max_sql_bytes,
                PREFLIGHT_BYTES_PER_SQL_BYTE,
                "SQL bytes",
            ),
            (self.max_bindings, PREFLIGHT_BYTES_PER_BINDING, "bindings"),
            (
                self.max_relation_occurrences,
                PREFLIGHT_BYTES_PER_RELATION,
                "relation occurrences",
            ),
            (
                self.max_logical_nodes,
                PREFLIGHT_BYTES_PER_LOGICAL_NODE,
                "logical nodes",
            ),
            (
                self.max_expression_nodes,
                PREFLIGHT_BYTES_PER_EXPRESSION_NODE,
                "expression nodes",
            ),
        ];
        let total = components.into_iter().try_fold(
            PREFLIGHT_FIXED_TRANSIENT_BYTES,
            |total, (count, bytes_per_item, label)| {
                u64::try_from(count)
                    .ok()
                    .and_then(|count| count.checked_mul(bytes_per_item))
                    .and_then(|component| total.checked_add(component))
                    .ok_or_else(|| exhausted(format!("persisted SQL preflight {label} overflowed")))
            },
        )?;
        NonZeroU64::new(total)
            .ok_or_else(|| exhausted("persisted SQL preflight memory peak must be nonzero"))
    }
}

/// A versioned admission envelope computed before DataFusion planning allocations begin.
///
/// The profile is an intentionally conservative, version-locked accounting contract. It does not
/// claim allocator-level enforcement: DataFusion parser and optimizer allocations do not yet flow
/// through its execution memory pool. A server must retain the returned build reservation across
/// the complete async prepare and bind operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedPlanProfile {
    format_version: u32,
    limits: PersistedPlanLimits,
    input_fingerprint: [u8; 32],
    sql_bytes: usize,
    binding_count: usize,
    expanded_file_count: usize,
    max_plan_schema_bytes: u64,
    preflight: SqlPreflightMetrics,
    construction: PlanConstructionBounds,
    build_peak_bytes: NonZeroU64,
    retained_plan_bytes: NonZeroU64,
}

/// Stable server-facing classification of a persisted-plan failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedPlanErrorClass {
    /// SQL, schema, or expression input is invalid for the persisted query subset.
    InvalidInput,
    /// The requested plan feature is valid but intentionally unsupported.
    Unsupported,
    /// Planning could not obtain or represent its bounded resources.
    ResourceExhausted,
    /// A planner, runtime, I/O, or integration invariant failed unexpectedly.
    Internal,
}

/// Classifies a DataFusion persisted-plan error without exposing its unstable variants to Server.
#[must_use]
pub fn classify_persisted_plan_error(error: &DataFusionError) -> PersistedPlanErrorClass {
    match error {
        DataFusionError::Context(_, source) => classify_persisted_plan_error(source),
        DataFusionError::ResourcesExhausted(_) => PersistedPlanErrorClass::ResourceExhausted,
        DataFusionError::NotImplemented(_) => PersistedPlanErrorClass::Unsupported,
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::Configuration(_)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::Execution(_) => PersistedPlanErrorClass::InvalidInput,
        DataFusionError::ArrowError(_, _)
        | DataFusionError::ParquetError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::IoError(_)
        | DataFusionError::Internal(_)
        | DataFusionError::ExecutionJoin(_)
        | DataFusionError::External(_)
        | DataFusionError::Substrait(_) => PersistedPlanErrorClass::Internal,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlanConstructionBounds {
    logical_nodes: usize,
    physical_nodes: usize,
    expression_nodes: usize,
}

impl PersistedPlanProfile {
    /// Returns the complete temporary reservation required while preparing and binding the plan.
    #[must_use]
    pub const fn build_peak_bytes(self) -> NonZeroU64 {
        self.build_peak_bytes
    }

    /// Returns the upper bound retained by the prepared physical plan after binding.
    #[must_use]
    pub const fn retained_plan_bytes(self) -> NonZeroU64 {
        self.retained_plan_bytes
    }

    /// Returns the number of SQL-visible persisted table bindings.
    #[must_use]
    pub const fn binding_count(self) -> usize {
        self.binding_count
    }

    /// Returns the worst-case file metadata expansion across allowed relation occurrences.
    #[must_use]
    pub const fn expanded_file_count(self) -> usize {
        self.expanded_file_count
    }
}

/// A short-lived planning-only physical plan that must be consumed by snapshot binding.
pub struct PreparedPersistedPlan {
    plan: Arc<dyn ExecutionPlan>,
    expected_snapshot: PersistedSnapshotDescription,
    referenced_table_indices: Box<[usize]>,
    profile: PersistedPlanProfile,
    logical_node_count: usize,
    expression_node_count: usize,
    physical_node_count: usize,
    planning_scan_count: usize,
    planning_active_scan_count: usize,
}

impl fmt::Debug for PreparedPersistedPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedPersistedPlan")
            .field("referenced_table_indices", &self.referenced_table_indices)
            .field("logical_node_count", &self.logical_node_count)
            .field("expression_node_count", &self.expression_node_count)
            .field("physical_node_count", &self.physical_node_count)
            .field("planning_scan_count", &self.planning_scan_count)
            .field(
                "planning_active_scan_count",
                &self.planning_active_scan_count,
            )
            .finish_non_exhaustive()
    }
}

impl PreparedPersistedPlan {
    /// Returns the exact pre-admitted memory profile used to build this plan.
    #[must_use]
    pub const fn profile(&self) -> PersistedPlanProfile {
        self.profile
    }

    /// Returns the number of pre-optimization persisted relation occurrences.
    #[must_use]
    pub fn relation_occurrence_count(&self) -> usize {
        self.referenced_table_indices.len()
    }

    /// Consumes the planning-only plan and binds every remaining persisted scan to one opener.
    pub fn bind(self, opener: &Arc<dyn PersistedFileOpener>) -> Result<BoundPersistedPlan> {
        bind_persisted_snapshot_plan(self.plan, &self.expected_snapshot, opener, None)
    }

    /// Consumes the planning-only plan and binds persisted and active scan capabilities.
    pub fn bind_with_active(
        self,
        opener: &Arc<dyn PersistedFileOpener>,
        active_opener: &Arc<dyn ActiveSegmentOpener>,
    ) -> Result<BoundPersistedPlan> {
        bind_persisted_snapshot_plan(
            self.plan,
            &self.expected_snapshot,
            opener,
            Some(active_opener),
        )
    }
}

/// Computes a conservative planning envelope without constructing a DataFusion context or plan.
pub fn profile_persisted_plan(
    sql: &str,
    bindings: &[PersistedTableBinding],
    snapshot: &PersistedSnapshotDescription,
    limits: PersistedPlanLimits,
) -> Result<PersistedPlanProfile> {
    let preflight = validate_profile_inputs(sql, bindings, snapshot, limits)?;

    let binding_name_bytes = bindings.iter().try_fold(0_usize, |total, binding| {
        total
            .checked_add(binding.sql_name.len())
            .ok_or_else(|| exhausted("persisted binding name bytes overflowed usize"))
    })?;
    if binding_name_bytes > limits.max_sql_bytes {
        return Err(exhausted(format!(
            "persisted binding names exceed limit actual=[{binding_name_bytes}] limit=[{}]",
            limits.max_sql_bytes
        )));
    }
    let text_bytes = sql
        .len()
        .checked_add(binding_name_bytes)
        .ok_or_else(|| exhausted("persisted planning text bytes overflowed usize"))?;

    let extra_occurrences = preflight
        .relation_occurrence_count
        .checked_sub(bindings.len())
        .ok_or_else(|| plan_error("SQL relation count is below the admitted binding count"))?;
    let max_inputs_per_table = snapshot
        .max_files_per_table()
        .checked_add(snapshot.max_active_segments_per_table())
        .ok_or_else(|| exhausted("maximum per-table input count overflowed usize"))?;
    let base_input_count = snapshot
        .total_file_count()
        .checked_add(snapshot.total_active_segment_count())
        .ok_or_else(|| exhausted("snapshot input count overflowed usize"))?;
    let expanded_file_count = extra_occurrences
        .checked_mul(max_inputs_per_table)
        .and_then(|extra| base_input_count.checked_add(extra))
        .ok_or_else(|| exhausted("expanded persisted file count overflowed usize"))?;
    let expanded_input_schema_bytes = u64::try_from(extra_occurrences)
        .ok()
        .and_then(|extra| extra.checked_mul(snapshot.max_retained_schema_bytes()))
        .and_then(|extra| snapshot.total_retained_schema_bytes().checked_add(extra))
        .ok_or_else(|| exhausted("expanded persisted schema bytes overflowed u64"))?;
    let generated_schema_bytes = u64::try_from(preflight.expression_node_count)
        .ok()
        .and_then(|expressions| expressions.checked_mul(GENERATED_SCHEMA_BYTES_PER_EXPRESSION))
        .and_then(|fixed| {
            u64::try_from(text_bytes)
                .ok()
                .and_then(|text| text.checked_mul(GENERATED_SCHEMA_TEXT_EXPANSION_FACTOR))
                .and_then(|text| fixed.checked_add(text))
        })
        .ok_or_else(|| exhausted("generated plan schema bytes overflowed u64"))?;
    let max_plan_schema_bytes = expanded_input_schema_bytes
        .checked_add(generated_schema_bytes)
        .ok_or_else(|| exhausted("maximum plan schema bytes overflowed u64"))?;

    let logical_nodes_from_validated_limit = expanded_node_bound(
        preflight.structural_node_count,
        BUILD_LOGICAL_NODE_EXPANSION_FACTOR,
        "logical plan construction nodes",
    )?;
    let logical_nodes_from_expression_limit = expanded_node_bound(
        preflight.expression_node_count,
        BUILD_LOGICAL_NODES_PER_EXPRESSION_NODE,
        "expression-driven logical plan construction nodes",
    )?;
    let expression_nodes_from_ast = expanded_node_bound(
        preflight.expression_node_count,
        BUILD_EXPRESSION_NODE_EXPANSION_FACTOR.max(preflight.relation_occurrence_count),
        "expression construction nodes",
    )?;
    let expression_nodes_from_text = expanded_node_bound(
        text_bytes,
        BUILD_EXPRESSION_NODES_PER_TEXT_BYTE,
        "text-driven expression construction nodes",
    )?;
    let construction = PlanConstructionBounds {
        logical_nodes: logical_nodes_from_validated_limit
            .checked_add(logical_nodes_from_expression_limit)
            .ok_or_else(|| {
                exhausted("combined logical plan construction nodes overflowed usize")
            })?,
        physical_nodes: limits.max_physical_nodes.max(expanded_node_bound(
            limits.max_logical_nodes,
            BUILD_PHYSICAL_NODES_PER_VALIDATED_LOGICAL_NODE,
            "physical plan construction nodes",
        )?),
        expression_nodes: expression_nodes_from_ast.max(expression_nodes_from_text),
    };
    let retained_plan_bytes = retained_plan_upper_bound(
        text_bytes,
        bindings.len(),
        expanded_file_count,
        construction.expression_nodes,
        max_plan_schema_bytes,
        limits,
    )?;
    let build_peak_bytes = build_peak_upper_bound(
        retained_plan_bytes,
        text_bytes,
        bindings.len(),
        expanded_file_count,
        max_plan_schema_bytes,
        construction,
    )?;

    Ok(PersistedPlanProfile {
        format_version: PROFILE_FORMAT_VERSION,
        limits,
        input_fingerprint: profile_fingerprint(sql, bindings, snapshot),
        sql_bytes: sql.len(),
        binding_count: bindings.len(),
        expanded_file_count,
        max_plan_schema_bytes,
        preflight,
        construction,
        build_peak_bytes,
        retained_plan_bytes,
    })
}

/// Builds and validates one planning-only physical plan under a precomputed profile.
pub async fn prepare_persisted_plan(
    policy: QuerySessionPolicy,
    sql: &str,
    bindings: &[PersistedTableBinding],
    snapshot: &PersistedSnapshotDescription,
    admitted: PersistedPlanProfile,
) -> Result<PreparedPersistedPlan> {
    let actual = profile_persisted_plan(sql, bindings, snapshot, admitted.limits)?;
    if actual != admitted {
        return Err(plan_error(
            "persisted plan inputs do not match the admitted profile",
        ));
    }

    let tables = snapshot.tables();
    let context = policy.create_context();
    register_bindings(&context, bindings, tables)?;
    let state = context.state();
    let logical = state.create_logical_plan(sql).await?;
    let logical_stats = validate_logical_plan(&logical, tables.len(), admitted.limits, true)?;
    validate_construction_bounds(&logical_stats, admitted, "initial logical plan")?;
    if logical_stats.referenced_table_indices.len() != admitted.preflight.relation_occurrence_count
    {
        return Err(plan_error(
            "DataFusion logical relation count differs from SQL preflight",
        ));
    }
    let optimized = state.optimize(&logical)?;
    let optimized_stats = validate_logical_plan(&optimized, tables.len(), admitted.limits, false)?;
    validate_construction_bounds(&optimized_stats, admitted, "optimized logical plan")?;
    let plan = state
        .query_planner()
        .create_physical_plan(&optimized, &state)
        .await?;
    let (physical_node_count, planning_scan_count, planning_active_scan_count) =
        validate_physical_plan(&plan, admitted.limits)?;
    drop(state);
    drop(context);

    Ok(PreparedPersistedPlan {
        plan,
        expected_snapshot: snapshot.clone(),
        referenced_table_indices: logical_stats.referenced_table_indices.into_boxed_slice(),
        profile: admitted,
        logical_node_count: logical_stats.node_count.max(optimized_stats.node_count),
        expression_node_count: logical_stats
            .expression_node_count
            .max(optimized_stats.expression_node_count),
        physical_node_count,
        planning_scan_count,
        planning_active_scan_count,
    })
}

fn validate_construction_bounds(
    stats: &LogicalPlanStats,
    admitted: PersistedPlanProfile,
    phase: &str,
) -> Result<()> {
    if stats.node_count > admitted.construction.logical_nodes
        || stats.expression_node_count > admitted.construction.expression_nodes
    {
        return Err(exhausted(format!(
            "{phase} exceeded the version-locked construction envelope logical=[{}]/[{}] expressions=[{}]/[{}]",
            stats.node_count,
            admitted.construction.logical_nodes,
            stats.expression_node_count,
            admitted.construction.expression_nodes
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct LogicalPlanStats {
    node_count: usize,
    expression_node_count: usize,
    referenced_table_indices: Vec<usize>,
}

fn validate_logical_plan(
    plan: &LogicalPlan,
    table_count: usize,
    limits: PersistedPlanLimits,
    require_exact_relations: bool,
) -> Result<LogicalPlanStats> {
    let mut node_count = 0_usize;
    let mut expression_node_count = 0_usize;
    let mut referenced_table_indices = Vec::new();
    plan.apply_with_subqueries(|node| {
        node_count = checked_increment(node_count, limits.max_logical_nodes, "logical plan nodes")?;
        if !is_allowed_logical_node(node) {
            return Err(plan_error(format!(
                "persisted SQL contains unsupported logical node node=[{}]",
                node.display()
            )));
        }
        if let LogicalPlan::TableScan(scan) = node {
            let provider = source_as_provider(&scan.source)?;
            let provider = provider
                .as_any()
                .downcast_ref::<ZippyTableProvider>()
                .ok_or_else(|| plan_error("persisted SQL resolved a non-Zippy table source"))?;
            let table_index = provider.description().table_index();
            if table_index >= table_count {
                return Err(plan_error(
                    "persisted SQL provider table ordinal is out of bounds",
                ));
            }
            referenced_table_indices.push(table_index);
            if referenced_table_indices.len() > limits.max_relation_occurrences {
                return Err(exhausted(format!(
                    "persisted relation occurrence limit exceeded actual=[{}] limit=[{}]",
                    referenced_table_indices.len(),
                    limits.max_relation_occurrences
                )));
            }
        }
        for expression in node.expressions() {
            expression.apply(|_| {
                expression_node_count = checked_increment(
                    expression_node_count,
                    limits.max_expression_nodes,
                    "logical expression nodes",
                )?;
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    if require_exact_relations {
        let mut seen = vec![false; table_count];
        for &table_index in &referenced_table_indices {
            seen[table_index] = true;
        }
        if referenced_table_indices.is_empty() || seen.iter().any(|seen| !seen) {
            return Err(plan_error(
                "persisted SQL relation set does not match all admitted table bindings",
            ));
        }
    }
    Ok(LogicalPlanStats {
        node_count,
        expression_node_count,
        referenced_table_indices,
    })
}

fn is_allowed_logical_node(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Unnest(_)
    )
}

fn validate_physical_plan(
    plan: &Arc<dyn ExecutionPlan>,
    limits: PersistedPlanLimits,
) -> Result<(usize, usize, usize)> {
    let mut node_count = 0_usize;
    let mut planning_scan_count = 0_usize;
    let mut planning_active_scan_count = 0_usize;
    plan.apply(|node| {
        node_count =
            checked_increment(node_count, limits.max_physical_nodes, "physical plan nodes")?;
        if let Some(parquet) = node.as_any().downcast_ref::<ParquetExec>() {
            if !is_exact_planning_parquet(parquet) {
                return Err(plan_error(
                    "prepared persisted plan contains a foreign or unmarked Parquet scan",
                ));
            }
            planning_scan_count = planning_scan_count
                .checked_add(1)
                .ok_or_else(|| exhausted("planning Parquet scan count overflowed usize"))?;
        } else if let Some(active) = node.as_any().downcast_ref::<ActiveSegmentExec>() {
            if !is_exact_planning_active(active) {
                return Err(plan_error(
                    "prepared persisted plan contains an already-bound active segment scan",
                ));
            }
            planning_active_scan_count = planning_active_scan_count
                .checked_add(1)
                .ok_or_else(|| exhausted("planning active scan count overflowed usize"))?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok((node_count, planning_scan_count, planning_active_scan_count))
}

fn register_bindings(
    context: &SessionContext,
    bindings: &[PersistedTableBinding],
    tables: &[PersistedTableDescription],
) -> Result<()> {
    let state = context.state();
    let default_catalog = state.config_options().catalog.default_catalog.to_string();
    let default_schema = state.config_options().catalog.default_schema.to_string();
    drop(state);

    let mut resolved_names = HashSet::with_capacity(bindings.len());
    for binding in bindings {
        let table = tables
            .get(binding.table_index)
            .ok_or_else(|| plan_error("persisted table binding ordinal is out of bounds"))?;
        let reference = TableReference::from(binding.sql_name.as_ref());
        let resolved = reference.clone().resolve(&default_catalog, &default_schema);
        if !resolved_names.insert(resolved.clone()) {
            return Err(plan_error(format!(
                "persisted SQL table binding resolves more than once reference=[{resolved}]"
            )));
        }
        let catalog = match context.catalog(&resolved.catalog) {
            Some(catalog) => catalog,
            None => {
                let catalog = Arc::new(MemoryCatalogProvider::new());
                context.register_catalog(resolved.catalog.to_string(), catalog.clone());
                catalog
            }
        };
        if catalog.schema(&resolved.schema).is_none() {
            catalog.register_schema(&resolved.schema, Arc::new(MemorySchemaProvider::new()))?;
        }
        context.register_table(reference, Arc::new(ZippyTableProvider::new(table.clone())))?;
    }
    Ok(())
}

fn validate_profile_inputs(
    sql: &str,
    bindings: &[PersistedTableBinding],
    snapshot: &PersistedSnapshotDescription,
    limits: PersistedPlanLimits,
) -> Result<SqlPreflightMetrics> {
    if sql.trim().is_empty() {
        return Err(plan_error("persisted SQL text is empty"));
    }
    if sql.len() > limits.max_sql_bytes {
        return Err(exhausted(format!(
            "persisted SQL exceeds byte limit actual=[{}] limit=[{}]",
            sql.len(),
            limits.max_sql_bytes
        )));
    }
    if bindings.is_empty() || bindings.len() != snapshot.table_count() {
        return Err(plan_error(
            "persisted table bindings must map every manifest table exactly once",
        ));
    }
    if bindings.len() > limits.max_bindings {
        return Err(exhausted(format!(
            "persisted table binding limit exceeded actual=[{}] limit=[{}]",
            bindings.len(),
            limits.max_bindings
        )));
    }
    if bindings.len() > limits.max_relation_occurrences {
        return Err(exhausted(
            "persisted relation occurrence limit is below the binding count",
        ));
    }
    let tables = snapshot.tables();
    let mut table_indices = HashSet::with_capacity(bindings.len());
    let mut sql_names = HashSet::with_capacity(bindings.len());
    for binding in bindings {
        if binding.sql_name.trim().is_empty() || binding.sql_name.len() > limits.max_sql_bytes {
            return Err(plan_error("persisted SQL table binding name is invalid"));
        }
        if binding.table_index >= tables.len() {
            return Err(plan_error(
                "persisted table binding ordinal is out of bounds",
            ));
        }
        if !table_indices.insert(binding.table_index) {
            return Err(plan_error("persisted table binding ordinal is duplicated"));
        }
        if !sql_names.insert(binding.sql_name.as_ref()) {
            return Err(plan_error("persisted SQL table binding name is duplicated"));
        }
    }
    let wildcard_expression_upper_bound = limits
        .max_relation_occurrences
        .checked_mul(snapshot.max_columns_per_table())
        .ok_or_else(|| exhausted("wildcard expression expansion overflowed usize"))?;
    preflight_persisted_sql(sql, limits, wildcard_expression_upper_bound)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlPreflightMetrics {
    structural_node_count: usize,
    expression_node_count: usize,
    relation_occurrence_count: usize,
}

fn preflight_persisted_sql(
    sql: &str,
    limits: PersistedPlanLimits,
    wildcard_expression_upper_bound: usize,
) -> Result<SqlPreflightMetrics> {
    let mut statements = DFParser::parse_sql(sql)
        .map_err(|error| plan_error(format!("persisted SQL parse failed error=[{error}]")))?;
    if statements.len() != 1 {
        return Err(plan_error(
            "persisted SQL must contain exactly one read-only query statement",
        ));
    }
    let statement = statements
        .pop_front()
        .expect("one parsed SQL statement was validated");
    let query = match statement {
        DataFusionStatement::Statement(statement) => match *statement {
            SqlStatement::Query(query) => query,
            _ => {
                return Err(plan_error(
                    "persisted SQL must contain exactly one read-only query statement",
                ));
            }
        },
        _ => {
            return Err(plan_error(
                "persisted SQL must contain exactly one read-only query statement",
            ));
        }
    };

    let mut visitor = SqlPreflightVisitor {
        limits,
        structural_node_count: 0,
        expression_node_count: 0,
        relation_occurrence_count: 0,
        wildcard_expression_upper_bound,
    };
    match query.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(SqlPreflightMetrics {
            structural_node_count: visitor.structural_node_count,
            expression_node_count: visitor.expression_node_count,
            relation_occurrence_count: visitor.relation_occurrence_count,
        }),
        ControlFlow::Break(error) => Err(error),
    }
}

struct SqlPreflightVisitor {
    limits: PersistedPlanLimits,
    structural_node_count: usize,
    expression_node_count: usize,
    relation_occurrence_count: usize,
    wildcard_expression_upper_bound: usize,
}

impl SqlPreflightVisitor {
    fn visit_query(&mut self, query: &SqlQuery) -> Result<()> {
        if !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || query.with.is_some()
            || query.fetch.is_some()
            || !query.limit_by.is_empty()
            || query.order_by.is_some()
        {
            return Err(plan_error(
                "persisted SQL query contains unsupported CTE, ordering, or control clauses",
            ));
        }
        let subquery_expansion =
            subquery_optimizer_expression_expansion(query, self.limits.max_expression_nodes)?;
        self.expression_node_count = checked_add_bounded(
            self.expression_node_count,
            subquery_expansion,
            self.limits.max_expression_nodes,
            "SQL expression nodes after subquery optimizer fanout",
        )?;
        let remaining_structural_nodes = self
            .limits
            .max_logical_nodes
            .checked_sub(self.structural_node_count)
            .ok_or_else(|| exhausted("SQL structural node limit was already exceeded"))?;
        let inspected = inspect_set_expr_structure(&query.body, remaining_structural_nodes)?;
        let structural_nodes = inspected
            .structural_nodes
            .checked_add(1)
            .ok_or_else(|| exhausted("SQL structural node count overflowed usize"))?;
        self.structural_node_count = checked_add_bounded(
            self.structural_node_count,
            structural_nodes,
            self.limits.max_logical_nodes,
            "SQL structural nodes",
        )?;
        let wildcard_expressions = inspected
            .wildcard_count
            .checked_mul(self.wildcard_expression_upper_bound)
            .ok_or_else(|| exhausted("SQL wildcard expression count overflowed usize"))?;
        self.expression_node_count = checked_add_bounded(
            self.expression_node_count,
            wildcard_expressions,
            self.limits.max_expression_nodes,
            "SQL expression nodes after wildcard expansion",
        )?;
        Ok(())
    }

    fn visit_table_factor(&mut self, table_factor: &TableFactor) -> Result<()> {
        self.structural_node_count = checked_increment(
            self.structural_node_count,
            self.limits.max_logical_nodes,
            "SQL structural nodes",
        )?;
        match table_factor {
            TableFactor::Table {
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                ..
            } if args.is_none()
                && alias.as_ref().is_none_or(|alias| alias.columns.is_empty())
                && with_hints.is_empty()
                && version.is_none()
                && !with_ordinality
                && partitions.is_empty()
                && json_path.is_none() =>
            {
                self.relation_occurrence_count = checked_increment(
                    self.relation_occurrence_count,
                    self.limits.max_relation_occurrences,
                    "persisted relation occurrences",
                )?;
                Ok(())
            }
            TableFactor::Table {
                alias: Some(alias),
                ..
            } if !alias.columns.is_empty() => Err(plan_error(
                "persisted SQL table column-list aliases are unsupported because schema expansion is not pre-admissible",
            )),
            TableFactor::NestedJoin { .. } => Err(plan_error(
                "persisted SQL nested joins are unsupported because join expansion is not pre-admissible",
            )),
            TableFactor::Derived { .. } => Err(plan_error(
                "persisted SQL derived relations are unsupported because optimizer rewrites are not pre-admissible",
            )),
            _ => Err(plan_error(format!(
                "persisted SQL contains unsupported table factor factor=[{table_factor}]"
            ))),
        }
    }

    fn visit_expression(&mut self, expression: &SqlExpr) -> Result<()> {
        if matches!(expression, SqlExpr::Between { .. } | SqlExpr::Case { .. }) {
            return Err(plan_error(
                "persisted SQL BETWEEN and CASE expressions are unsupported because optimizer rewrites are not pre-admissible",
            ));
        }
        if matches!(
            expression,
            SqlExpr::BinaryOp {
                op: BinaryOperator::PGRegexMatch
                    | BinaryOperator::PGRegexIMatch
                    | BinaryOperator::PGRegexNotMatch
                    | BinaryOperator::PGRegexNotIMatch,
                ..
            }
        ) {
            return Err(plan_error(
                "persisted SQL regex operators are unsupported because optimizer rewrites are not pre-admissible",
            ));
        }
        if matches!(
            expression,
            SqlExpr::Cube(_) | SqlExpr::Rollup(_) | SqlExpr::GroupingSets(_)
        ) {
            return Err(plan_error(
                "persisted SQL grouping sets are unsupported because they can expand exponentially",
            ));
        }
        if matches!(expression, SqlExpr::InUnnest { .. }) {
            return Err(plan_error(
                "persisted SQL IN UNNEST is unsupported because schema-driven expansion is not pre-admissible",
            ));
        }
        if let SqlExpr::Function(function) = expression {
            if function.over.is_some() {
                return Err(plan_error(
                    "persisted SQL WINDOW expressions are unsupported because physical ordering expansion is not pre-admissible",
                ));
            }
            if function
                .name
                .0
                .last()
                .is_some_and(|identifier| identifier.value.eq_ignore_ascii_case("unnest"))
            {
                return Err(plan_error(
                    "persisted SQL UNNEST is unsupported because schema-driven expansion is not pre-admissible",
                ));
            }
            if !function.within_group.is_empty()
                || [&function.parameters, &function.args]
                    .into_iter()
                    .any(|arguments| {
                        matches!(arguments, FunctionArguments::List(list) if list
                            .clauses
                            .iter()
                            .any(|clause| matches!(clause, FunctionArgumentClause::OrderBy(_))))
                    })
            {
                return Err(plan_error(
                    "persisted SQL ordered aggregate expressions are unsupported because physical ordering expansion is not pre-admissible",
                ));
            }
            let is_bounded_aggregate = function.name.0.len() == 1
                && function.name.0.last().is_some_and(|identifier| {
                    ["count", "min", "max", "sum", "avg"]
                        .iter()
                        .any(|name| identifier.value.eq_ignore_ascii_case(name))
                });
            if !is_bounded_aggregate {
                return Err(plan_error(
                    "persisted SQL function is outside the bounded aggregate allowlist",
                ));
            }
        }
        self.expression_node_count = checked_increment(
            self.expression_node_count,
            self.limits.max_expression_nodes,
            "SQL expression nodes",
        )?;
        if let SqlExpr::CompoundIdentifier(identifiers) = expression {
            let expanded_nodes = identifiers
                .len()
                .saturating_sub(1)
                .checked_mul(2)
                .ok_or_else(|| exhausted("compound identifier expansion overflowed usize"))?;
            self.expression_node_count = checked_add_bounded(
                self.expression_node_count,
                expanded_nodes,
                self.limits.max_expression_nodes,
                "SQL expression nodes after compound identifier expansion",
            )?;
        }
        Ok(())
    }
}

fn subquery_optimizer_expression_expansion(query: &SqlQuery, limit: usize) -> Result<usize> {
    struct SubqueryFanoutVisitor {
        expression_node_count: usize,
        subquery_count: usize,
        limit: usize,
    }

    impl Visitor for SubqueryFanoutVisitor {
        type Break = DataFusionError;

        fn pre_visit_expr(&mut self, expression: &SqlExpr) -> ControlFlow<Self::Break> {
            let result = (|| {
                self.expression_node_count = checked_increment(
                    self.expression_node_count,
                    self.limit,
                    "SQL subquery fanout expression nodes",
                )?;
                if matches!(
                    expression,
                    SqlExpr::Subquery(_) | SqlExpr::Exists { .. } | SqlExpr::InSubquery { .. }
                ) {
                    self.subquery_count = checked_increment(
                        self.subquery_count,
                        self.limit,
                        "SQL subquery occurrences",
                    )?;
                }
                Ok(())
            })();
            result_control_flow(result)
        }
    }

    let mut visitor = SubqueryFanoutVisitor {
        expression_node_count: 0,
        subquery_count: 0,
        limit,
    };
    match query.visit(&mut visitor) {
        ControlFlow::Continue(()) => visitor
            .expression_node_count
            .checked_mul(visitor.subquery_count)
            .ok_or_else(|| exhausted("SQL subquery optimizer fanout overflowed usize")),
        ControlFlow::Break(error) => Err(error),
    }
}

impl Visitor for SqlPreflightVisitor {
    type Break = DataFusionError;

    fn pre_visit_query(&mut self, query: &SqlQuery) -> ControlFlow<Self::Break> {
        result_control_flow(self.visit_query(query))
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        result_control_flow(self.visit_table_factor(table_factor))
    }

    fn pre_visit_expr(&mut self, expression: &SqlExpr) -> ControlFlow<Self::Break> {
        result_control_flow(self.visit_expression(expression))
    }

    fn pre_visit_statement(&mut self, _statement: &SqlStatement) -> ControlFlow<Self::Break> {
        ControlFlow::Break(plan_error(
            "persisted SQL query contains an unsupported nested statement",
        ))
    }
}

#[derive(Debug, Clone, Copy)]
struct SetExprMetrics {
    structural_nodes: usize,
    wildcard_count: usize,
}

fn inspect_set_expr_structure(root: &SetExpr, limit: usize) -> Result<SetExprMetrics> {
    if limit == 0 {
        return Err(exhausted("SQL structural node limit exceeded"));
    }
    let mut pending = vec![root];
    let mut structural_nodes = 1_usize;
    let mut wildcard_count = 0_usize;
    while let Some(node) = pending.pop() {
        match node {
            SetExpr::Select(select) => {
                if select.into.is_some()
                    || matches!(&select.distinct, Some(Distinct::On(_)))
                    || select.connect_by.is_some()
                    || select.prewhere.is_some()
                    || !select.cluster_by.is_empty()
                    || select.having.is_some()
                    || !select.lateral_views.is_empty()
                    || select.qualify.is_some()
                    || select.top.is_some()
                    || !select.sort_by.is_empty()
                    || select.value_table_mode.is_some()
                    || !select.named_window.is_empty()
                {
                    return Err(plan_error(
                        "persisted SQL SELECT contains an unsupported semantic clause",
                    ));
                }
                let group_modifiers = match &select.group_by {
                    GroupByExpr::All(modifiers) | GroupByExpr::Expressions(_, modifiers) => {
                        modifiers
                    }
                };
                if !group_modifiers.is_empty() {
                    return Err(plan_error(
                        "persisted SQL GROUP BY modifiers are unsupported",
                    ));
                }
                let projection_aliases = select
                    .projection
                    .iter()
                    .filter_map(|item| match item {
                        SelectItem::ExprWithAlias { alias, .. } => Some(alias),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let group_expressions = match &select.group_by {
                    GroupByExpr::All(_) => &[][..],
                    GroupByExpr::Expressions(expressions, _) => expressions.as_slice(),
                };
                if group_expressions.iter().any(|expression| {
                    matches!(expression, SqlExpr::Value(SqlValue::Number(_, _)))
                        || expression_contains_projection_alias(expression, &projection_aliases)
                }) {
                    return Err(plan_error(
                        "persisted SQL GROUP BY projection aliases or positions are unsupported",
                    ));
                }
                let select_wildcards = select
                    .projection
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                        )
                    })
                    .count();
                wildcard_count = wildcard_count
                    .checked_add(select_wildcards)
                    .ok_or_else(|| exhausted("SQL wildcard count overflowed usize"))?;
                if select_wildcards > 0
                    && select.from.iter().any(|from| {
                        !matches!(&from.relation, TableFactor::Table { .. })
                            || from
                                .joins
                                .iter()
                                .any(|join| !matches!(&join.relation, TableFactor::Table { .. }))
                    })
                {
                    return Err(plan_error(
                        "persisted SQL wildcard over a derived relation is unsupported",
                    ));
                }
                let join_count = select.from.iter().try_fold(0_usize, |count, from| {
                    count
                        .checked_add(from.joins.len())
                        .ok_or_else(|| exhausted("SQL join count overflowed usize"))
                })?;
                structural_nodes = checked_add_bounded(
                    structural_nodes,
                    join_count,
                    limit,
                    "SQL structural nodes",
                )?;
                if select.from.iter().flat_map(|from| &from.joins).any(|join| {
                    matches!(
                        join_constraint(&join.join_operator),
                        Some(JoinConstraint::Using(_) | JoinConstraint::Natural)
                    )
                }) {
                    return Err(plan_error(
                        "persisted SQL NATURAL and USING joins are unsupported",
                    ));
                }
            }
            SetExpr::Query(_) | SetExpr::Values(_) => {}
            SetExpr::SetOperation { left, right, .. } => {
                structural_nodes =
                    checked_add_bounded(structural_nodes, 2, limit, "SQL structural nodes")?;
                pending.push(right);
                pending.push(left);
            }
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Table(_) => {
                return Err(plan_error(
                    "persisted SQL query body contains a non-read-only operation",
                ));
            }
        }
    }
    Ok(SetExprMetrics {
        structural_nodes,
        wildcard_count,
    })
}

fn join_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Inner(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => Some(constraint),
        JoinOperator::AsOf { constraint, .. } => Some(constraint),
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => None,
    }
}

fn expression_contains_projection_alias(expression: &SqlExpr, aliases: &[&Ident]) -> bool {
    struct AliasVisitor<'a> {
        aliases: &'a [&'a Ident],
    }

    impl Visitor for AliasVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &SqlExpr) -> ControlFlow<Self::Break> {
            if matches!(expression, SqlExpr::Identifier(identifier) if self
                .aliases
                .iter()
                .any(|alias| alias.value.eq_ignore_ascii_case(&identifier.value)))
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    matches!(
        expression.visit(&mut AliasVisitor { aliases }),
        ControlFlow::Break(())
    )
}

fn result_control_flow(result: Result<()>) -> ControlFlow<DataFusionError> {
    match result {
        Ok(()) => ControlFlow::Continue(()),
        Err(error) => ControlFlow::Break(error),
    }
}

fn retained_plan_upper_bound(
    text_bytes: usize,
    binding_count: usize,
    expanded_file_count: usize,
    retained_expression_nodes: usize,
    max_plan_schema_bytes: u64,
    limits: PersistedPlanLimits,
) -> Result<NonZeroU64> {
    let mut total = RETAINED_FIXED_BYTES;
    total = checked_add_scaled(
        total,
        text_bytes,
        RETAINED_BYTES_PER_TEXT_BYTE,
        "retained text",
    )?;
    total = checked_add_scaled(
        total,
        binding_count,
        RETAINED_BYTES_PER_BINDING,
        "retained bindings",
    )?;
    total = checked_add_scaled(
        total,
        expanded_file_count,
        RETAINED_BYTES_PER_EXPANDED_FILE,
        "retained expanded files",
    )?;
    total = checked_add_scaled(
        total,
        limits.max_physical_nodes,
        RETAINED_BYTES_PER_PHYSICAL_NODE,
        "retained physical nodes",
    )?;
    total = checked_add_scaled(
        total,
        retained_expression_nodes,
        RETAINED_BYTES_PER_EXPRESSION_NODE,
        "retained expressions",
    )?;
    total = total
        .checked_add(
            max_plan_schema_bytes
                .checked_mul(RETAINED_SCHEMA_EXPANSION_FACTOR)
                .ok_or_else(|| exhausted("retained schema expansion overflowed u64"))?,
        )
        .ok_or_else(|| exhausted("retained plan bytes overflowed u64"))?;
    total = checked_add_schema_node_product(
        total,
        max_plan_schema_bytes,
        limits.max_physical_nodes,
        RETAINED_SCHEMA_BYTES_PER_PHYSICAL_NODE,
        "retained physical node schemas",
    )?;
    NonZeroU64::new(total).ok_or_else(|| exhausted("retained plan bytes are zero"))
}

fn build_peak_upper_bound(
    retained: NonZeroU64,
    text_bytes: usize,
    binding_count: usize,
    expanded_file_count: usize,
    max_plan_schema_bytes: u64,
    construction: PlanConstructionBounds,
) -> Result<NonZeroU64> {
    let mut total = retained
        .get()
        .checked_add(BUILD_FIXED_TRANSIENT_BYTES)
        .ok_or_else(|| exhausted("build fixed transient overflowed u64"))?;
    total = checked_add_scaled(total, text_bytes, BUILD_BYTES_PER_TEXT_BYTE, "build text")?;
    total = checked_add_scaled(
        total,
        binding_count,
        BUILD_BYTES_PER_BINDING,
        "build bindings",
    )?;
    total = checked_add_scaled(
        total,
        expanded_file_count,
        BUILD_BYTES_PER_EXPANDED_FILE,
        "build expanded files",
    )?;
    total = checked_add_scaled(
        total,
        construction.logical_nodes,
        BUILD_BYTES_PER_LOGICAL_NODE,
        "build logical nodes",
    )?;
    total = checked_add_scaled(
        total,
        construction.physical_nodes,
        BUILD_BYTES_PER_PHYSICAL_NODE,
        "build physical nodes",
    )?;
    total = checked_add_scaled(
        total,
        construction.expression_nodes,
        BUILD_BYTES_PER_EXPRESSION_NODE,
        "build expressions",
    )?;
    total = total
        .checked_add(
            max_plan_schema_bytes
                .checked_mul(BUILD_SCHEMA_TRANSIENT_FACTOR)
                .ok_or_else(|| exhausted("build schema transient overflowed u64"))?,
        )
        .ok_or_else(|| exhausted("build peak bytes overflowed u64"))?;
    total = checked_add_schema_node_product(
        total,
        max_plan_schema_bytes,
        construction.logical_nodes,
        BUILD_SCHEMA_BYTES_PER_LOGICAL_NODE,
        "build logical node schemas",
    )?;
    total = checked_add_schema_node_product(
        total,
        max_plan_schema_bytes,
        construction.physical_nodes,
        BUILD_SCHEMA_BYTES_PER_TRANSIENT_PHYSICAL_NODE,
        "build transient physical node schemas",
    )?;
    NonZeroU64::new(total).ok_or_else(|| exhausted("build peak bytes are zero"))
}

fn expanded_node_bound(count: usize, factor: usize, label: &str) -> Result<usize> {
    count.checked_mul(factor).ok_or_else(|| {
        exhausted(format!(
            "persisted plan construction bound overflow component=[{label}]"
        ))
    })
}

fn checked_add_schema_node_product(
    total: u64,
    schema_bytes: u64,
    node_count: usize,
    factor: u64,
    label: &str,
) -> Result<u64> {
    u64::try_from(node_count)
        .ok()
        .and_then(|nodes| schema_bytes.checked_mul(nodes))
        .and_then(|bytes| bytes.checked_mul(factor))
        .and_then(|component| total.checked_add(component))
        .ok_or_else(|| {
            exhausted(format!(
                "persisted plan schema-node estimate overflow component=[{label}]"
            ))
        })
}

fn checked_add_scaled(total: u64, count: usize, unit: u64, label: &str) -> Result<u64> {
    u64::try_from(count)
        .ok()
        .and_then(|count| count.checked_mul(unit))
        .and_then(|component| total.checked_add(component))
        .ok_or_else(|| {
            exhausted(format!(
                "persisted plan byte estimate overflow component=[{label}]"
            ))
        })
}

fn checked_increment(current: usize, limit: usize, label: &str) -> Result<usize> {
    checked_add_bounded(current, 1, limit, label)
}

fn checked_add_bounded(
    current: usize,
    additional: usize,
    limit: usize,
    label: &str,
) -> Result<usize> {
    let next = current
        .checked_add(additional)
        .ok_or_else(|| exhausted(format!("{label} overflowed usize")))?;
    if next > limit {
        return Err(exhausted(format!(
            "{label} limit exceeded actual=[{next}] limit=[{limit}]"
        )));
    }
    Ok(next)
}

fn profile_fingerprint(
    sql: &str,
    bindings: &[PersistedTableBinding],
    snapshot: &PersistedSnapshotDescription,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(PROFILE_FORMAT_VERSION.to_le_bytes());
    update_length_prefixed(&mut digest, sql.as_bytes());
    digest.update((bindings.len() as u64).to_le_bytes());
    for binding in bindings {
        update_length_prefixed(&mut digest, binding.sql_name.as_bytes());
        digest.update((binding.table_index as u64).to_le_bytes());
    }
    digest.update(snapshot.manifest_digest());
    digest.finalize().into()
}

fn update_length_prefixed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

fn plan_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

fn exhausted(message: impl Into<String>) -> DataFusionError {
    DataFusionError::ResourcesExhausted(message.into())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{provider_as_source, MemTable};
    use datafusion::logical_expr::LogicalPlanBuilder;
    use zippy_types::{ErrorCode, FileId, ServerError, TableId};

    use super::{
        classify_persisted_plan_error, prepare_persisted_plan as prepare_validated_persisted_plan,
        profile_persisted_plan as profile_validated_persisted_plan, register_bindings,
        validate_logical_plan, PersistedPlanErrorClass, PersistedPlanLimits, PersistedTableBinding,
        BUILD_BYTES_PER_LOGICAL_NODE, BUILD_BYTES_PER_PHYSICAL_NODE,
        BUILD_EXPRESSION_NODES_PER_TEXT_BYTE, BUILD_EXPRESSION_NODE_EXPANSION_FACTOR,
        BUILD_LOGICAL_NODES_PER_EXPRESSION_NODE, RETAINED_BYTES_PER_PHYSICAL_NODE,
    };
    use crate::{
        BoxedAsyncFileReader, PersistedFileDescription, PersistedFileKey, PersistedFileOpenRequest,
        PersistedFileOpener, PersistedSnapshotDescription, PersistedTableDescription,
        QuerySessionPolicy,
    };

    struct RejectingOpener {
        snapshot: PersistedSnapshotDescription,
    }

    impl PersistedFileOpener for RejectingOpener {
        fn snapshot_description(&self) -> &PersistedSnapshotDescription {
            &self.snapshot
        }

        fn open_reader(
            &self,
            _request: PersistedFileOpenRequest,
        ) -> zippy_types::Result<BoxedAsyncFileReader> {
            Err(ServerError::new(
                ErrorCode::Internal,
                "prepared-plan test does not execute physical files",
            ))
        }
    }

    fn nonzero(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("test value is nonzero")
    }

    fn table_id(value: u128) -> TableId {
        TableId::from_u128(value).expect("test table ID is nonzero")
    }

    fn file_id(value: u128) -> FileId {
        FileId::from_u128(value).expect("test file ID is nonzero")
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
        ]))
    }

    fn table(table_index: usize, identity: u128, file_count: usize) -> PersistedTableDescription {
        table_with_schema(table_index, identity, file_count, schema())
    }

    fn table_with_schema(
        table_index: usize,
        identity: u128,
        file_count: usize,
        schema: Arc<Schema>,
    ) -> PersistedTableDescription {
        let table_id = table_id(identity);
        let files = (0..file_count)
            .map(|file_index| {
                PersistedFileDescription::try_new(
                    PersistedFileKey::new(
                        table_index,
                        file_index,
                        table_id,
                        file_id(identity * 100 + file_index as u128 + 1),
                    ),
                    nonzero(7),
                    nonzero(2),
                    nonzero(1024),
                    nonzero(10),
                )
                .expect("build test file description")
            })
            .collect();
        PersistedTableDescription::try_new(
            table_index,
            table_id,
            nonzero(7),
            nonzero(2),
            schema,
            files,
        )
        .expect("build test table description")
    }

    fn binding(name: &str, table_index: usize) -> PersistedTableBinding {
        PersistedTableBinding::try_new(name, table_index).expect("build test table binding")
    }

    fn limits() -> PersistedPlanLimits {
        PersistedPlanLimits::try_new(4096, 8, 16, 256, 512, 2048)
            .expect("build test planning limits")
    }

    fn policy() -> QuerySessionPolicy {
        QuerySessionPolicy::new(NonZeroUsize::new(2).expect("nonzero target partitions"))
    }

    fn profile_persisted_plan(
        sql: &str,
        bindings: &[PersistedTableBinding],
        tables: &[PersistedTableDescription],
        limits: PersistedPlanLimits,
    ) -> datafusion::common::Result<super::PersistedPlanProfile> {
        let snapshot = PersistedSnapshotDescription::try_new(tables.to_vec())?;
        profile_validated_persisted_plan(sql, bindings, &snapshot, limits)
    }

    async fn prepare_persisted_plan(
        policy: QuerySessionPolicy,
        sql: &str,
        bindings: &[PersistedTableBinding],
        tables: &[PersistedTableDescription],
        admitted: super::PersistedPlanProfile,
    ) -> datafusion::common::Result<super::PreparedPersistedPlan> {
        let snapshot = PersistedSnapshotDescription::try_new(tables.to_vec())?;
        prepare_validated_persisted_plan(policy, sql, bindings, &snapshot, admitted).await
    }

    #[tokio::test]
    async fn qualified_self_join_retains_preoptimization_relations_and_consumes_into_binding() {
        let tables = vec![table(0, 1, 1)];
        let bindings = vec![binding(r#""Market"."Ticks""#, 0)];
        let sql = concat!(
            "select a.price from \"Market\".\"Ticks\" a ",
            "join \"Market\".\"Ticks\" b on a.symbol = b.symbol where false"
        );
        let profile = profile_persisted_plan(sql, &bindings, &tables, limits())
            .expect("profile qualified self join");
        assert!(profile.build_peak_bytes() > profile.retained_plan_bytes());
        assert_eq!(profile.binding_count(), 1);
        assert!(profile.expanded_file_count() >= 2);
        assert_eq!(profile.preflight.relation_occurrence_count, 2);

        let prepared = prepare_persisted_plan(policy(), sql, &bindings, &tables, profile)
            .await
            .expect("prepare qualified self join");
        assert_eq!(prepared.relation_occurrence_count(), 2);
        let opener: Arc<dyn PersistedFileOpener> = Arc::new(RejectingOpener {
            snapshot: PersistedSnapshotDescription::try_new(tables.clone())
                .expect("build rejecting opener snapshot"),
        });
        let bound = prepared
            .bind(&opener)
            .expect("bind optimizer-eliminated persisted plan");
        assert_eq!(bound.bound_scan_count(), 0);
    }

    #[tokio::test]
    async fn bindings_are_exact_unique_and_independent_from_manifest_order() {
        let tables = vec![table(0, 1, 0), table(1, 2, 0)];
        let reversed = vec![binding("quotes", 1), binding("ticks", 0)];
        let sql = "select ticks.price from ticks join quotes on ticks.symbol = quotes.symbol";
        let profile = profile_persisted_plan(sql, &reversed, &tables, limits())
            .expect("profile reversed binding order");
        let prepared = prepare_persisted_plan(policy(), sql, &reversed, &tables, profile)
            .await
            .expect("prepare reversed binding order");
        assert_eq!(prepared.relation_occurrence_count(), 2);

        assert!(profile_persisted_plan(sql, &reversed[..1], &tables, limits()).is_err());
        assert!(profile_persisted_plan(
            sql,
            &[binding("ticks", 0), binding("quotes", 0)],
            &tables,
            limits(),
        )
        .is_err());
        assert!(profile_persisted_plan(
            sql,
            &[binding("ticks", 0), binding("ticks", 1)],
            &tables,
            limits(),
        )
        .is_err());

        let normalized_duplicate = vec![binding("ticks", 0), binding("TICKS", 1)];
        let duplicate_profile =
            profile_persisted_plan(sql, &normalized_duplicate, &tables, limits())
                .expect("raw names differ before DataFusion normalization");
        assert!(prepare_persisted_plan(
            policy(),
            sql,
            &normalized_duplicate,
            &tables,
            duplicate_profile,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn exists_in_and_scalar_subqueries_are_traversed_before_optimization() {
        let tables = vec![table(0, 1, 0), table(1, 2, 0)];
        let bindings = vec![binding("ticks", 0), binding("quotes", 1)];
        for sql in [
            concat!(
                "select price from ticks where exists ",
                "(select 1 from quotes where quotes.price > 0)"
            ),
            "select price from ticks where price in (select price from quotes)",
            concat!(
                "select price, (select max(price) from quotes) as quote_max ",
                "from ticks"
            ),
        ] {
            let profile = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect("profile embedded-subquery SQL");
            assert_eq!(profile.preflight.relation_occurrence_count, 2, "SQL: {sql}");
            let context = policy().create_context();
            register_bindings(&context, &bindings, &tables)
                .expect("register embedded-subquery tables");
            let logical = context
                .state()
                .create_logical_plan(sql)
                .await
                .expect("build embedded-subquery logical plan");
            let stats = validate_logical_plan(&logical, tables.len(), limits(), true)
                .expect("validate embedded-subquery logical plan");
            assert_eq!(stats.referenced_table_indices.len(), 2, "SQL: {sql}");
        }
    }

    #[tokio::test]
    async fn repeated_scalar_subqueries_charge_optimizer_clone_fanout() {
        let tables = vec![table(0, 1, 0)];
        let bindings = vec![binding("ticks", 0)];
        let scalar_terms = (0..16)
            .map(|value| format!("(select {value})"))
            .collect::<Vec<_>>()
            .join(" + ");
        let sql = format!("select price + {scalar_terms} from ticks");
        let profile = profile_persisted_plan(&sql, &bindings, &tables, limits())
            .expect("profile repeated scalar subqueries");
        assert!(profile.preflight.expression_node_count >= 16 * 16);
        prepare_persisted_plan(policy(), &sql, &bindings, &tables, profile)
            .await
            .expect("prepare repeated scalar subqueries under clone-fanout admission");
    }

    #[tokio::test]
    async fn constant_function_outputs_fail_before_optimizer_materialization() {
        let tables = vec![table(0, 1, 0)];
        let bindings = vec![binding("ticks", 0)];
        for sql in [
            "select repeat('x', 1000000000) from ticks",
            concat!(
                "select repeat('x', 1000000000 + ",
                "cast(false and price > 0 as bigint)) from ticks"
            ),
            "select abs(price) from ticks",
        ] {
            let error = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect_err("non-aggregate function must fail before optimizer evaluation");
            assert!(error.to_string().contains("bounded aggregate allowlist"));
        }

        for sql in [
            "select count(*) from ticks",
            "select min(price) from ticks",
            "select max(price) from ticks",
            "select sum(price) from ticks",
            "select avg(price) from ticks",
        ] {
            let profile = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect("profile bounded aggregate function");
            prepare_persisted_plan(policy(), sql, &bindings, &tables, profile)
                .await
                .expect("prepare bounded aggregate function");
        }
    }

    #[test]
    fn all_window_forms_fail_closed() {
        let tables = vec![table(0, 1, 1)];
        let bindings = vec![binding("ticks", 0)];
        let inline_sql = "select row_number() over (order by price) from ticks";
        let inline_error = profile_persisted_plan(inline_sql, &bindings, &tables, limits())
            .expect_err("inline WINDOW must fail before DataFusion planning");
        assert!(inline_error.to_string().contains("WINDOW expressions"));

        let named_sql = concat!(
            "select row_number() over w as row_number from ticks ",
            "window w as (order by price)"
        );
        let error = profile_persisted_plan(named_sql, &bindings, &tables, limits())
            .expect_err("named WINDOW must fail before DataFusion planning");
        assert!(error.to_string().contains("unsupported semantic clause"));

        for sql in [
            "select array_agg(price order by symbol) from ticks",
            "select percentile_cont(0.5) within group (order by price) from ticks",
        ] {
            let ordered_error = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect_err("ordered aggregate must fail before physical planning");
            assert!(
                ordered_error
                    .to_string()
                    .contains("ordered aggregate expressions"),
                "unexpected ordered aggregate error for SQL [{sql}]: {ordered_error}"
            );
        }
    }

    #[test]
    fn schema_and_projection_driven_join_and_group_expansions_fail_closed() {
        let fields = (0..64)
            .map(|index| Field::new(format!("column_{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let tables = vec![table_with_schema(0, 1, 0, Arc::new(Schema::new(fields)))];
        let bindings = vec![binding("ticks", 0)];

        for (sql, expected) in [
            (
                "select column_0 + 1 as bucket from ticks group by bucket",
                "GROUP BY projection aliases",
            ),
            (
                "select column_0 + column_1 as bucket from ticks group by bucket + 1",
                "GROUP BY projection aliases",
            ),
            (
                concat!(
                    "select sum(column_0 + column_1) as total from ticks ",
                    "having total > 1 or total > 2"
                ),
                "unsupported semantic clause",
            ),
            (
                "select column_0 from ticks group by 1",
                "GROUP BY projection aliases",
            ),
            (
                "select left_ticks.column_0 from ticks left_ticks natural join ticks right_ticks",
                "NATURAL and USING joins",
            ),
            (
                concat!(
                    "select left_ticks.column_0 from ticks left_ticks ",
                    "join ticks right_ticks using (column_0)"
                ),
                "NATURAL and USING joins",
            ),
            (
                concat!(
                    "select left_ticks.column_0 from ",
                    "(ticks left_ticks natural join ticks right_ticks)"
                ),
                "nested joins",
            ),
        ] {
            let error = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect_err("schema-driven expansion must fail before DataFusion planning");
            assert!(
                error.to_string().contains(expected),
                "unexpected preflight error for SQL [{sql}]: {error}"
            );
        }

        let alias_columns = (0..64)
            .map(|index| format!("alias_{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let alias_sql = format!("select aliased.alias_0 from ticks aliased({alias_columns})");
        let alias_error = profile_persisted_plan(&alias_sql, &bindings, &tables, limits())
            .expect_err("table column-list alias must fail before DataFusion planning");
        assert!(alias_error.to_string().contains("column-list aliases"));

        profile_persisted_plan(
            "select column_0 from ticks group by column_0",
            &bindings,
            &tables,
            limits(),
        )
        .expect("ordinary GROUP BY expression remains supported");
    }

    #[test]
    fn optimizer_fanout_from_derived_set_operations_fails_closed() {
        let tables = vec![table(0, 1, 0)];
        let bindings = vec![binding("ticks", 0)];
        let sql = concat!(
            "select price from (",
            "select price from ticks union all select price from ticks",
            ") union_ticks where price > 0"
        );
        let error = profile_persisted_plan(sql, &bindings, &tables, limits())
            .expect_err("derived set operation must fail before optimizer predicate fanout");
        assert!(error.to_string().contains("derived relations"));

        profile_persisted_plan(
            "select price from ticks union all select price from ticks",
            &bindings,
            &tables,
            limits(),
        )
        .expect("top-level set operation remains supported");
    }

    #[tokio::test]
    async fn compound_identifiers_and_join_optimizer_fanout_are_pre_admitted() {
        let nested_type = (0..8).rev().fold(DataType::Int64, |child, index| {
            DataType::Struct(
                vec![Arc::new(Field::new(format!("level_{index}"), child, false))].into(),
            )
        });
        let nested_tables = vec![table_with_schema(
            0,
            1,
            0,
            Arc::new(Schema::new(vec![Field::new("payload", nested_type, false)])),
        )];
        let bindings = vec![binding("ticks", 0)];
        let nested_sql = concat!(
            "select payload.level_0.level_1.level_2.level_3.",
            "level_4.level_5.level_6.level_7 from ticks"
        );
        let nested_profile =
            profile_persisted_plan(nested_sql, &bindings, &nested_tables, limits())
                .expect("profile nested struct path");
        assert!(nested_profile.preflight.expression_node_count >= 17);
        prepare_persisted_plan(
            policy(),
            nested_sql,
            &bindings,
            &nested_tables,
            nested_profile,
        )
        .await
        .expect("prepare nested struct path under its expanded expression admission");

        let join_tables = vec![table(0, 1, 0)];
        let mut join_sql = String::from("select t1.price from ticks t1");
        for index in 2..=16 {
            join_sql.push_str(&format!(
                " join ticks t{index} on t1.symbol = t{index}.symbol"
            ));
        }
        join_sql.push_str(" where t1.price > 0");
        let join_profile = profile_persisted_plan(&join_sql, &bindings, &join_tables, limits())
            .expect("profile maximum-relation join chain");
        assert_eq!(join_profile.preflight.relation_occurrence_count, 16);
        assert!(
            join_profile.construction.expression_nodes
                >= join_profile.preflight.expression_node_count * 16
        );
        assert!(
            join_profile.construction.expression_nodes
                >= join_sql.len() * BUILD_EXPRESSION_NODES_PER_TEXT_BYTE
        );
        prepare_persisted_plan(policy(), &join_sql, &bindings, &join_tables, join_profile)
            .await
            .expect("prepare join chain under relation-fanout expression admission");
    }

    #[tokio::test]
    async fn hidden_subquery_relations_and_foreign_sources_fail_before_physical_planning() {
        let tables = vec![table(0, 1, 0)];
        let bindings = vec![binding("ticks", 0)];
        let scalar = "select price, (select max(s.price) from ticks s) from ticks t";
        let one_relation = PersistedPlanLimits::try_new(4096, 1, 1, 256, 512, 2048)
            .expect("build one-relation limit");
        let error = profile_persisted_plan(scalar, &bindings, &tables, one_relation)
            .expect_err("hidden self-scan must consume relation budget before DataFusion");
        assert!(matches!(
            error,
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ));

        for sql in [
            concat!(
                "select price from ticks where exists ",
                "(select 1 from generate_series(1, 10))"
            ),
            concat!(
                "select price from ticks where price in ",
                "(select value from generate_series(1, 10))"
            ),
            concat!(
                "select price, (select max(value) from generate_series(1, 10)) ",
                "from ticks"
            ),
        ] {
            let preflight_error = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect_err("foreign table function must fail AST preflight");
            assert!(
                preflight_error
                    .to_string()
                    .contains("unsupported table factor"),
                "unexpected preflight error for SQL [{sql}]: {preflight_error}"
            );

            let context = policy().create_context();
            register_bindings(&context, &bindings, &tables)
                .expect("register persisted table beside foreign subquery");
            let logical = context
                .state()
                .create_logical_plan(sql)
                .await
                .expect("build foreign-subquery logical plan");
            let error = validate_logical_plan(&logical, tables.len(), limits(), true)
                .expect_err("foreign subquery source must fail logical validation");
            assert!(
                error.to_string().contains("non-Zippy table source"),
                "unexpected error for SQL [{sql}]: {error}"
            );
        }

        for (sql, expected) in [
            (
                "select price between 1 and 2 from ticks",
                "BETWEEN and CASE",
            ),
            (
                "select case when price > 0 then 1 else 0 end from ticks",
                "BETWEEN and CASE",
            ),
            (
                "select symbol ~ '^(a|b|c|d|e)$' from ticks",
                "regex operators",
            ),
        ] {
            let error = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect_err("optimizer-expanding expression must fail before planning");
            assert!(
                error.to_string().contains(expected),
                "unexpected optimizer-expansion error for SQL [{sql}]: {error}"
            );
        }
    }

    #[tokio::test]
    async fn admitted_profile_binds_canonical_schema_content_not_only_its_charge() {
        let admitted_schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]));
        let changed_schema = Arc::new(Schema::new(vec![Field::new(
            "costs",
            DataType::Int64,
            false,
        )]));
        let admitted_tables = vec![table_with_schema(0, 1, 0, admitted_schema)];
        let changed_tables = vec![table_with_schema(0, 1, 0, changed_schema)];
        assert_eq!(
            admitted_tables[0].retained_schema_bytes(),
            changed_tables[0].retained_schema_bytes(),
            "same-size schemas isolate canonical content from byte charge"
        );
        assert_ne!(
            admitted_tables[0].schema_digest(),
            changed_tables[0].schema_digest()
        );

        let bindings = vec![binding("ticks", 0)];
        let sql = "select price from ticks";
        let admitted = profile_persisted_plan(sql, &bindings, &admitted_tables, limits())
            .expect("profile exact admitted schema");
        let changed = profile_persisted_plan(sql, &bindings, &changed_tables, limits())
            .expect("profile same-charge changed schema");
        assert_ne!(admitted, changed);

        let error = prepare_persisted_plan(policy(), sql, &bindings, &changed_tables, admitted)
            .await
            .expect_err("same-charge changed schema must not reuse admission");
        assert!(error.to_string().contains("admitted profile"));
    }

    #[tokio::test]
    async fn admitted_profile_is_bound_to_exact_sql_and_manifest() {
        let tables = vec![table(0, 1, 1)];
        let bindings = vec![binding("ticks", 0)];
        let admitted_sql = "select price from ticks where price = 1";
        let changed_sql = "select price from ticks where price = 2";
        assert_eq!(admitted_sql.len(), changed_sql.len());
        let profile = profile_persisted_plan(admitted_sql, &bindings, &tables, limits())
            .expect("profile admitted SQL");
        let error = prepare_persisted_plan(policy(), changed_sql, &bindings, &tables, profile)
            .await
            .expect_err("same-size changed SQL must not reuse an admitted profile");
        assert!(error.to_string().contains("admitted profile"));

        let exact_profile = profile_persisted_plan(admitted_sql, &bindings, &tables, limits())
            .expect("profile exact SQL");
        let different = vec![table(0, 9, 1)];
        assert!(prepare_persisted_plan(
            policy(),
            admitted_sql,
            &bindings,
            &different,
            exact_profile,
        )
        .await
        .is_err());
    }

    #[test]
    fn non_read_only_ignored_and_unsupported_sql_are_rejected() {
        let tables = vec![table(0, 1, 0)];
        let bindings = vec![binding("ticks", 0)];
        for sql in [
            "drop table ticks",
            "insert into ticks values (1, 'x')",
            "explain select * from ticks",
            "select price into copied_ticks from ticks",
            "select price from ticks fetch first 1 rows only",
            "select price from ticks limit 1 by symbol",
            "select price from ticks prewhere price > 0",
            "select top 1 price from ticks",
            "select price from ticks qualify row_number() over () = 1",
            "select price from ticks cluster by symbol",
            "select price from ticks sort by symbol",
            "select price from ticks order by price",
            "select distinct on (symbol) symbol, price from ticks",
            "with quoted as (select price from ticks) select price from quoted",
            concat!(
                "with changed as (insert into ticks values (1, 'x') returning *) ",
                "select * from changed"
            ),
            "select * from ticks; select * from ticks",
        ] {
            assert!(
                profile_persisted_plan(sql, &bindings, &tables, limits()).is_err(),
                "SQL must be rejected: {sql}"
            );
        }

        for sql in [
            "select count(*) from ticks group by cube(price, symbol)",
            "select count(*) from ticks group by rollup(price, symbol)",
            "select count(*) from ticks group by grouping sets ((price), (symbol))",
        ] {
            let error = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect_err("grouping sets must fail before DataFusion planning");
            assert!(
                error.to_string().contains("expand exponentially"),
                "unexpected grouping-set error for SQL [{sql}]: {error}"
            );
        }
    }

    #[test]
    fn wildcard_expansion_and_expression_driven_logical_nodes_are_pre_admitted() {
        let fields = (0..64)
            .map(|index| Field::new(format!("column_{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let tables = vec![table_with_schema(0, 1, 0, Arc::new(Schema::new(fields)))];
        let bindings = vec![binding("ticks", 0)];
        let wildcard_limit = PersistedPlanLimits::try_new(4096, 1, 1, 64, 128, 16)
            .expect("build wildcard expression limit");
        let error =
            profile_persisted_plan("select * from ticks", &bindings, &tables, wildcard_limit)
                .expect_err("wide wildcard must fail before DataFusion planning");
        assert!(error.to_string().contains("wildcard expansion"));
        let derived = profile_persisted_plan(
            "select * from (select column_0 from ticks) derived_ticks",
            &bindings,
            &tables,
            limits(),
        )
        .expect_err("derived wildcard must fail before DataFusion planning");
        assert!(derived.to_string().contains("derived relation"));

        profile_persisted_plan(
            "select count(*) from ticks",
            &bindings,
            &tables,
            wildcard_limit,
        )
        .expect("aggregate wildcard does not expand table columns");

        let base_sql = "select column_0 from ticks";
        let wider_sql = "select column_0 + column_1 from ticks";
        let base = profile_persisted_plan(base_sql, &bindings, &tables, limits())
            .expect("profile base expression-driven construction bound");
        let wider = profile_persisted_plan(wider_sql, &bindings, &tables, limits())
            .expect("profile wider expression-driven construction bound");
        let expression_delta =
            wider.preflight.expression_node_count - base.preflight.expression_node_count;
        assert_eq!(
            wider.construction.logical_nodes - base.construction.logical_nodes,
            expression_delta * BUILD_LOGICAL_NODES_PER_EXPRESSION_NODE
        );
        for (profile, sql) in [(base, base_sql), (wider, wider_sql)] {
            assert!(
                profile.construction.expression_nodes
                    >= profile.preflight.expression_node_count
                        * BUILD_EXPRESSION_NODE_EXPANSION_FACTOR
            );
            assert!(
                profile.construction.expression_nodes
                    >= sql.len() * BUILD_EXPRESSION_NODES_PER_TEXT_BYTE
            );
        }
        assert!(wider.build_peak_bytes() > base.build_peak_bytes());
        assert!(base.retained_plan_bytes().get() < 512 * 1024 * 1024);
        assert!(base.build_peak_bytes().get() < 512 * 1024 * 1024);
    }

    #[test]
    fn schema_driven_unnest_is_rejected_before_datafusion_planning() {
        let children = (0..64)
            .map(|index| Arc::new(Field::new(format!("child_{index}"), DataType::Int64, false)))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(children.into()),
            false,
        )]));
        let tables = vec![table_with_schema(0, 1, 0, schema)];
        let bindings = vec![binding("ticks", 0)];
        for sql in [
            "select unnest(payload) from ticks",
            "select count(*) from ticks group by unnest(payload)",
            "select payload in unnest(payload) from ticks",
            "select value from unnest(payload) as expanded(value)",
        ] {
            let error = profile_persisted_plan(sql, &bindings, &tables, limits())
                .expect_err("schema-driven UNNEST must fail AST preflight");
            assert!(
                error.to_string().contains("UNNEST")
                    || error.to_string().contains("unsupported table factor"),
                "unexpected UNNEST error for SQL [{sql}]: {error}"
            );
        }
    }

    #[tokio::test]
    async fn foreign_table_sources_and_structural_limits_fail_closed() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "price",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("build ordinary memory batch");
        let provider = Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch]])
                .expect("build ordinary memory table"),
        );
        let ordinary = LogicalPlanBuilder::scan("ordinary", provider_as_source(provider), None)
            .expect("plan ordinary source")
            .build()
            .expect("build ordinary source plan");
        assert!(validate_logical_plan(&ordinary, 1, limits(), true).is_err());

        let tables = vec![table(0, 1, 1)];
        let bindings = vec![binding("ticks", 0)];
        let sql = "select price + 1 from ticks where price > 0";
        let logical_limit = PersistedPlanLimits::try_new(4096, 1, 1, 1, 512, 2048)
            .expect("build logical-node limit");
        assert!(matches!(
            profile_persisted_plan(sql, &bindings, &tables, logical_limit),
            Err(datafusion::common::DataFusionError::ResourcesExhausted(_))
        ));

        let expression_limit = PersistedPlanLimits::try_new(4096, 1, 1, 256, 512, 1)
            .expect("build expression-node limit");
        assert!(matches!(
            profile_persisted_plan(sql, &bindings, &tables, expression_limit),
            Err(datafusion::common::DataFusionError::ResourcesExhausted(_))
        ));

        let union_sql = concat!(
            "select price from ticks union all select price from ticks ",
            "union all select price from ticks union all select price from ticks"
        );
        let union_limit = PersistedPlanLimits::try_new(4096, 1, 4, 8, 512, 64)
            .expect("build union structural limit");
        let union_error = profile_persisted_plan(union_sql, &bindings, &tables, union_limit)
            .expect_err("set operations must consume preflight structural nodes");
        assert!(union_error.to_string().contains("SQL structural nodes"));

        let physical_limit = PersistedPlanLimits::try_new(4096, 1, 1, 256, 1, 2048)
            .expect("build physical-node limit");
        let profile = profile_persisted_plan(sql, &bindings, &tables, physical_limit)
            .expect("profile physical-node-limited query");
        assert!(
            prepare_persisted_plan(policy(), sql, &bindings, &tables, profile,)
                .await
                .is_err()
        );
    }

    #[test]
    fn zero_limits_and_budget_overflow_are_rejected_before_planning() {
        assert!(PersistedPlanLimits::try_new(0, 1, 1, 1, 1, 1).is_err());
        let base = PersistedPlanLimits::try_new(4096, 1, 1, 8, 8, 8)
            .expect("build bounded preflight limits");
        let wider = PersistedPlanLimits::try_new(4096, 1, 1, 8, 8, 9)
            .expect("build wider preflight limits");
        assert!(wider.preflight_peak_bytes().unwrap() > base.preflight_peak_bytes().unwrap());
        let overflowing = PersistedPlanLimits::try_new(usize::MAX, 1, 1, 1, 1, 1)
            .expect("build overflowing preflight limits");
        assert!(matches!(
            overflowing.preflight_peak_bytes(),
            Err(datafusion::common::DataFusionError::ResourcesExhausted(_))
        ));
        let tables = vec![table(0, 1, 1)];
        let bindings = vec![binding("ticks", 0)];
        let tiny_sql_limit =
            PersistedPlanLimits::try_new(4, 1, 1, 8, 8, 8).expect("build tiny SQL limit");
        let error =
            profile_persisted_plan("select * from ticks", &bindings, &tables, tiny_sql_limit)
                .expect_err("SQL exceeds the scalar admission limit");
        assert!(matches!(
            error,
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ));
    }

    #[test]
    fn datafusion_failures_map_to_stable_persisted_plan_classes() {
        use datafusion::common::DataFusionError;

        assert_eq!(
            classify_persisted_plan_error(&DataFusionError::Context(
                "bounded planning".to_owned(),
                Box::new(DataFusionError::ResourcesExhausted("memory".to_owned(),)),
            )),
            PersistedPlanErrorClass::ResourceExhausted
        );
        assert_eq!(
            classify_persisted_plan_error(&DataFusionError::NotImplemented("operator".to_owned(),)),
            PersistedPlanErrorClass::Unsupported
        );
        assert_eq!(
            classify_persisted_plan_error(&DataFusionError::Plan("input".to_owned())),
            PersistedPlanErrorClass::InvalidInput
        );
        assert_eq!(
            classify_persisted_plan_error(&DataFusionError::Internal("bug".to_owned())),
            PersistedPlanErrorClass::Internal
        );
    }

    #[test]
    fn schema_width_is_charged_for_each_allowed_plan_node() {
        let tables = vec![table(0, 1, 1)];
        let bindings = vec![binding("ticks", 0)];
        let sql = "select price from ticks";
        let base_limits = PersistedPlanLimits::try_new(4096, 1, 1, 8, 64, 32)
            .expect("build base schema-node limits");
        let physical_plus_one = PersistedPlanLimits::try_new(4096, 1, 1, 8, 65, 32)
            .expect("build wider physical-node limits");
        let logical_plus_one = PersistedPlanLimits::try_new(4096, 1, 1, 9, 64, 32)
            .expect("build wider logical-node limits");

        let base = profile_persisted_plan(sql, &bindings, &tables, base_limits)
            .expect("profile base schema-node limits");
        assert_eq!(base.preflight.structural_node_count, 3);
        let physical = profile_persisted_plan(sql, &bindings, &tables, physical_plus_one)
            .expect("profile one extra physical node");
        let logical = profile_persisted_plan(sql, &bindings, &tables, logical_plus_one)
            .expect("profile one extra logical node");

        assert_eq!(
            physical.retained_plan_bytes.get() - base.retained_plan_bytes.get(),
            RETAINED_BYTES_PER_PHYSICAL_NODE + base.max_plan_schema_bytes
        );
        assert_eq!(
            physical.build_peak_bytes.get() - base.build_peak_bytes.get(),
            RETAINED_BYTES_PER_PHYSICAL_NODE
                + BUILD_BYTES_PER_PHYSICAL_NODE
                + 2 * base.max_plan_schema_bytes
        );
        assert_eq!(
            logical.retained_plan_bytes, base.retained_plan_bytes,
            "logical-node width is transient only"
        );
        let logical_node_delta =
            u64::try_from(logical.construction.logical_nodes - base.construction.logical_nodes)
                .unwrap();
        let physical_node_delta =
            u64::try_from(logical.construction.physical_nodes - base.construction.physical_nodes)
                .unwrap();
        assert_eq!(
            logical.build_peak_bytes.get() - base.build_peak_bytes.get(),
            logical_node_delta * (BUILD_BYTES_PER_LOGICAL_NODE + base.max_plan_schema_bytes)
                + physical_node_delta
                    * (BUILD_BYTES_PER_PHYSICAL_NODE + base.max_plan_schema_bytes)
        );
    }
}
