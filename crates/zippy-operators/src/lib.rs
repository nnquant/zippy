pub mod aggregation;
pub mod cross_sectional;
pub mod expression;
pub mod reactive;

pub use aggregation::{
    AggCountSpec, AggFirstSpec, AggLastSpec, AggMaxSpec, AggMinSpec, AggSumSpec, AggVwapSpec,
    AggregationKind, AggregationSpec,
};
pub use cross_sectional::{CSDemeanSpec, CSRankSpec, CSZscoreSpec, CrossSectionalFactor};
pub use expression::ExpressionSpec;
pub use reactive::{
    AbsSpec, CastSpec, ClipSpec, LogSpec, ReactiveFactor, TsDelaySpec, TsDiffSpec, TsEmaSpec,
    TsMeanSpec, TsReturnSpec, TsStdSpec,
};

pub fn crate_name() -> &'static str {
    "zippy-operators"
}
