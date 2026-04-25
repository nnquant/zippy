pub mod cross_sectional;
pub mod reactive;
pub mod stream_table;
mod table_view;
pub mod testing;
pub mod timeseries;

pub use cross_sectional::CrossSectionalEngine;
pub use reactive::ReactiveStateEngine;
pub use stream_table::StreamTableEngine;
pub use testing::hash_record_batches;
pub use timeseries::TimeSeriesEngine;

pub fn crate_name() -> &'static str {
    "zippy-engines"
}
