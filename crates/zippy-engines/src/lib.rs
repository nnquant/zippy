pub mod bar_generator;
mod chain;
pub mod cross_sectional;
mod cross_sectional_bucket;
mod key_value_state;
mod latest_state;
pub mod reactive;
pub mod reactive_latest;
pub mod stream_table;
mod table_view;
pub mod testing;
pub mod timeseries;

pub use bar_generator::{
    AuctionPolicy, BarGeneratorEngine, BarGeneratorSpec, BarInputColumns, BarSessionSpec,
    BootstrapPolicy, DtLabelPolicy, SessionWindow, VolumeOutputDtype, VolumeSpec,
};
pub use chain::{EngineChain, IdentityEngine};
pub use cross_sectional::CrossSectionalEngine;
pub use key_value_state::KeyValueState;
pub use reactive::{ReactiveInvalidValuePolicy, ReactiveStateEngine, ReactiveStateFailurePolicy};
pub use reactive_latest::ReactiveLatestEngine;
pub use stream_table::{
    StreamTableLiveSegment, StreamTableMaterializer, DEFAULT_STREAM_TABLE_ROW_CAPACITY,
};
pub use testing::hash_record_batches;
pub use timeseries::TimeSeriesEngine;

pub fn crate_name() -> &'static str {
    "zippy-engines"
}
