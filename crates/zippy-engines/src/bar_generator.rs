use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};

/// Supported one-minute bar frequency.
pub const BAR_FREQUENCY_1M: &str = "1m";

/// Input column names consumed by [`BarGeneratorEngine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarInputColumns {
    pub instrument: String,
    pub dt: String,
    pub price: String,
    pub volume: String,
    pub total_turnover: String,
    pub trading_day: Option<String>,
    pub num_trades: Option<String>,
    pub limit_up: Option<String>,
    pub limit_down: Option<String>,
}

/// Trading session window represented as seconds since local midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionWindow {
    pub start_seconds: u32,
    pub end_seconds: u32,
}

impl SessionWindow {
    /// Parse a local time session window in `HH:MM:SS` format.
    pub fn parse(start: &str, end: &str) -> Result<Self> {
        let start_seconds = parse_seconds_since_midnight(start)?;
        let end_seconds = parse_seconds_since_midnight(end)?;

        if start_seconds >= end_seconds {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "session start must be before end start=[{}] end=[{}]",
                    start, end
                ),
            });
        }

        Ok(Self {
            start_seconds,
            end_seconds,
        })
    }
}

/// Session profile used to assign ticks into bar windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarSessionSpec {
    pub timezone: String,
    pub regular: Vec<SessionWindow>,
    pub auction: Vec<SessionWindow>,
}

/// Bootstrap policy for cumulative volume streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapPolicy {
    SkipFirstDelta,
    FromZero,
}

/// Tick volume interpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeSpec {
    Delta,
    Cumulative {
        trading_day_column: String,
        bootstrap: BootstrapPolicy,
    },
}

/// Auction tick handling policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuctionPolicy {
    Drop,
    MergeToFirstRegularBar,
    EmitSeparateBar,
}

/// Timestamp label policy for generated bars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DtLabelPolicy {
    CloseDt,
    StartDt,
}

/// Configuration for [`BarGeneratorEngine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarGeneratorSpec {
    pub input: BarInputColumns,
    pub session: BarSessionSpec,
    pub frequency: String,
    pub volume: VolumeSpec,
    pub auction: AuctionPolicy,
    pub dt_label: DtLabelPolicy,
}

/// Stateful tick-to-bar engine.
pub struct BarGeneratorEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    pending_filtered_rows: u64,
}

impl BarGeneratorEngine {
    /// Create a bar generator with a stable output schema.
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        spec: BarGeneratorSpec,
    ) -> Result<Self> {
        validate_spec(input_schema.as_ref(), &spec)?;
        let output_schema = build_output_schema(input_schema.as_ref(), &spec)?;

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema,
            pending_filtered_rows: 0,
        })
    }
}

impl Engine for BarGeneratorEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn on_data(&mut self, _table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        Ok(vec![])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            filtered_rows_total: self.pending_filtered_rows,
            ..Default::default()
        };
        self.pending_filtered_rows = 0;
        delta
    }
}

fn validate_spec(schema: &Schema, spec: &BarGeneratorSpec) -> Result<()> {
    if spec.frequency != BAR_FREQUENCY_1M {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "bar frequency must be supported frequency=[{}]",
                spec.frequency
            ),
        });
    }

    if spec.session.regular.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "regular sessions must not be empty".to_string(),
        });
    }

    validate_utf8_column(schema, &spec.input.instrument, "instrument")?;
    validate_dt_column(schema, &spec.input.dt)?;
    validate_float64_column(schema, &spec.input.price, "price")?;
    validate_float64_column(schema, &spec.input.volume, "volume")?;
    validate_float64_column(schema, &spec.input.total_turnover, "total_turnover")?;

    if let Some(column) = &spec.input.trading_day {
        validate_utf8_column(schema, column, "trading_day")?;
    }

    if let VolumeSpec::Cumulative {
        trading_day_column, ..
    } = &spec.volume
    {
        validate_utf8_column(schema, trading_day_column, "trading_day")?;
    }

    if let Some(column) = &spec.input.num_trades {
        validate_int64_column(schema, column, "num_trades")?;
    }

    if let Some(column) = &spec.input.limit_up {
        validate_float64_column(schema, column, "limit_up")?;
    }

    if let Some(column) = &spec.input.limit_down {
        validate_float64_column(schema, column, "limit_down")?;
    }

    Ok(())
}

fn build_output_schema(schema: &Schema, spec: &BarGeneratorSpec) -> Result<SchemaRef> {
    let instrument_field = schema
        .field_with_name(&spec.input.instrument)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing utf8 instrument field field=[{}]",
                spec.input.instrument
            ),
        })?;
    let timestamp_type = DataType::Timestamp(
        TimeUnit::Nanosecond,
        Some(spec.session.timezone.clone().into()),
    );

    Ok(Arc::new(Schema::new(vec![
        Arc::new(instrument_field.clone()),
        Arc::new(Field::new("dt", timestamp_type.clone(), false)),
        Arc::new(Field::new("open", DataType::Float64, false)),
        Arc::new(Field::new("high", DataType::Float64, false)),
        Arc::new(Field::new("low", DataType::Float64, false)),
        Arc::new(Field::new("close", DataType::Float64, false)),
        Arc::new(Field::new("volume", DataType::Float64, false)),
        Arc::new(Field::new("total_turnover", DataType::Float64, false)),
        Arc::new(Field::new("num_trades", DataType::Int64, true)),
        Arc::new(Field::new("limit_up", DataType::Float64, true)),
        Arc::new(Field::new("limit_down", DataType::Float64, true)),
        Arc::new(Field::new("start_dt", timestamp_type.clone(), false)),
        Arc::new(Field::new("close_dt", timestamp_type, false)),
    ])))
}

fn validate_utf8_column(schema: &Schema, column: &str, role: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 {} field field=[{}]", role, column),
        })?;

    if field.data_type() != &DataType::Utf8 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field must be utf8 field=[{}]", role, column),
        });
    }

    Ok(())
}

fn validate_dt_column(schema: &Schema, column: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing timezone-aware nanosecond dt field field=[{}]",
                column
            ),
        })?;

    if !matches!(
        field.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
    ) {
        return Err(ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be timezone-aware nanosecond timestamp field=[{}]",
                column
            ),
        });
    }

    Ok(())
}

fn validate_float64_column(schema: &Schema, column: &str, role: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing float64 {} field field=[{}]", role, column),
        })?;

    if field.data_type() != &DataType::Float64 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field must be float64 field=[{}]", role, column),
        });
    }

    Ok(())
}

fn validate_int64_column(schema: &Schema, column: &str, role: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing int64 {} field field=[{}]", role, column),
        })?;

    if field.data_type() != &DataType::Int64 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field must be int64 field=[{}]", role, column),
        });
    }

    Ok(())
}

fn parse_seconds_since_midnight(value: &str) -> Result<u32> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("time must use hh:mm:ss format value=[{}]", value),
        });
    }

    let hour = parse_time_part(parts[0], "hour", value)?;
    let minute = parse_time_part(parts[1], "minute", value)?;
    let second = parse_time_part(parts[2], "second", value)?;

    if hour > 23 || minute > 59 || second > 59 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("time value out of range value=[{}]", value),
        });
    }

    Ok(hour * 3600 + minute * 60 + second)
}

fn parse_time_part(part: &str, role: &str, value: &str) -> Result<u32> {
    part.parse::<u32>()
        .map_err(|error| ZippyError::InvalidConfig {
            reason: format!(
                "invalid time {} value=[{}] part=[{}] error=[{}]",
                role, value, part, error
            ),
        })
}
