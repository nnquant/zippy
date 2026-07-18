//! Gateway adapter for the server-owned native query authority.

use zippy_gateway::{
    GatewayNativeQueryExplanation, GatewayNativeQueryFrontend, GatewayNativeQueryOutputField,
    GatewayNativeQueryRequest,
};
use zippy_types::query::{QueryEngine, QueryInput, QueryRequest, SendableRecordBatchStream};
use zippy_types::{BoxFuture, Result};

use crate::{NativeQueryBinding, NativeQueryEngine};

#[derive(Clone)]
pub(crate) struct NativeGatewayQueryFrontend {
    engine: NativeQueryEngine,
}

impl NativeGatewayQueryFrontend {
    pub(crate) const fn new(engine: NativeQueryEngine) -> Self {
        Self { engine }
    }
}

impl GatewayNativeQueryFrontend for NativeGatewayQueryFrontend {
    fn execute(
        &self,
        request: GatewayNativeQueryRequest,
    ) -> BoxFuture<'_, Result<SendableRecordBatchStream>> {
        Box::pin(async move {
            let (request, bindings) = into_native_request(request)?;
            let owner = self.engine.owner(request.session_id);
            let token = self
                .engine
                .plan_bound(owner.as_ref(), request, bindings)
                .await?;
            self.engine.execute(token).await
        })
    }

    fn explain(
        &self,
        request: GatewayNativeQueryRequest,
    ) -> BoxFuture<'_, Result<GatewayNativeQueryExplanation>> {
        Box::pin(async move {
            let (request, bindings) = into_native_request(request)?;
            let owner = self.engine.owner(request.session_id);
            let explanation = self
                .engine
                .explain_bound(owner.as_ref(), request, bindings)
                .await?;
            let output_fields = explanation
                .output_schema
                .fields()
                .iter()
                .map(|field| GatewayNativeQueryOutputField {
                    name: field.name().clone(),
                    data_type: field.data_type().to_string(),
                    nullable: field.is_nullable(),
                })
                .collect();
            Ok(GatewayNativeQueryExplanation {
                query_id: explanation.instance_id.query_id(),
                generation: explanation.instance_id.generation(),
                catalog_revision: explanation.plan.catalog_revision,
                snapshots: explanation.plan.snapshots,
                output_fields,
                metrics: explanation.metrics,
            })
        })
    }
}

fn into_native_request(
    request: GatewayNativeQueryRequest,
) -> Result<(QueryRequest, Vec<NativeQueryBinding>)> {
    let bindings = request
        .bindings
        .into_iter()
        .map(|binding| NativeQueryBinding::try_new(binding.sql_name, binding.table))
        .collect::<Result<Vec<_>>>()?;
    Ok((
        QueryRequest {
            query_id: request.query_id,
            session_id: request.session_id,
            input: QueryInput::Sql(request.sql),
            tables: Vec::new(),
            priority: request.priority,
            deadline_ns: request.deadline_ns,
            memory_limit_bytes: request.memory_limit_bytes,
            spill_limit_bytes: request.spill_limit_bytes,
            result_batch_rows: request.result_batch_rows,
        },
        bindings,
    ))
}
