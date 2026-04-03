pub mod aggregation;
pub mod reactive;

pub use aggregation::{
    AggCountSpec, AggFirstSpec, AggLastSpec, AggMaxSpec, AggMinSpec, AggSumSpec, AggVwapSpec,
    AggregationKind, AggregationSpec,
};
pub use reactive::{
    AbsSpec, CastSpec, ClipSpec, LogSpec, ReactiveFactor, TsDelaySpec, TsDiffSpec, TsEmaSpec,
    TsMeanSpec, TsReturnSpec, TsStdSpec,
};

pub fn crate_name() -> &'static str {
    "zippy-operators"
}
