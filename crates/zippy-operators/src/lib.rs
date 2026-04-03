pub mod aggregation;
pub mod reactive;

pub use aggregation::{
    AggCountSpec, AggFirstSpec, AggLastSpec, AggMaxSpec, AggMinSpec, AggSumSpec, AggVwapSpec,
    AggregationKind, AggregationSpec,
};
pub use reactive::{ReactiveFactor, TsEmaSpec, TsReturnSpec};

pub fn crate_name() -> &'static str {
    "zippy-operators"
}
