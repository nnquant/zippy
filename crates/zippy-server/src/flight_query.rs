//! Arrow Flight adapter for the server-owned native query authority.

use zippy_protocol::query::FlightQueryRequest;
use zippy_protocol::server::{FlightQueryFrontend, OpaquePreparedFlightQuery, PreparedFlightQuery};
use zippy_types::query::{QueryInstanceId, QueryPlanInfo, SendableRecordBatchStream};
use zippy_types::{BoxFuture, Result};

use crate::{NativePreparedQuery, NativeQueryBinding, NativeQueryEngine};

/// Protocol adapter that sends Flight requests directly through the native
/// snapshot planner and execution authority.
#[derive(Clone)]
pub struct NativeFlightQueryFrontend {
    engine: NativeQueryEngine,
}

impl NativeFlightQueryFrontend {
    /// Creates a Flight frontend over one server-owned native query engine.
    #[must_use]
    pub const fn new(engine: NativeQueryEngine) -> Self {
        Self { engine }
    }
}

impl FlightQueryFrontend for NativeFlightQueryFrontend {
    fn prepare(
        &self,
        request: FlightQueryRequest,
    ) -> BoxFuture<'_, Result<OpaquePreparedFlightQuery>> {
        Box::pin(async move {
            let bindings = request
                .bindings
                .into_iter()
                .map(|binding| NativeQueryBinding::try_new(binding.sql_name, binding.table))
                .collect::<Result<Vec<_>>>()?;
            let owner = self.engine.owner(request.request.session_id);
            let prepared = self
                .engine
                .prepare_bound(owner.as_ref(), request.request, bindings)
                .await?;
            Ok(Box::new(prepared) as OpaquePreparedFlightQuery)
        })
    }
}

impl PreparedFlightQuery for NativePreparedQuery {
    fn info(&self) -> &QueryPlanInfo {
        self.info()
    }

    fn instance_id(&self) -> QueryInstanceId {
        self.instance_id()
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema()
    }

    fn execute(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        (*self).into_stream()
    }
}
