pub mod cross_sectional;
pub mod reactive;
pub mod testing;
pub mod timeseries;

pub use cross_sectional::CrossSectionalEngine;
pub use reactive::ReactiveStateEngine;
pub use testing::hash_record_batches;
pub use timeseries::TimeSeriesEngine;

pub fn crate_name() -> &'static str {
    "zippy-engines"
}
