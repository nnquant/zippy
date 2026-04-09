use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Array, Float32Builder, Float64Array, Float64Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, ZippyError};

use crate::reactive::ReactiveFactor;

const EXPRESSION_DIVISION_BY_ZERO: &str = "expression division by zero";
const EXPRESSION_LOG_INPUT_MUST_BE_POSITIVE: &str = "expression log input must be positive";
const EXPRESSION_CLIP_BOUNDS_INVALID: &str = "expression clip bounds invalid";

/// Builder for a reactive expression factor.
pub struct ExpressionSpec {
    expression: String,
    output_field: String,
}

impl ExpressionSpec {
    /// Create a new expression factor spec.
    pub fn new(expression: &str, output_field: &str) -> Self {
        Self {
            expression: expression.to_string(),
            output_field: output_field.to_string(),
        }
    }

    /// Build the expression factor against the current schema.
    pub fn build(&self, input_schema: &Schema) -> Result<Box<dyn ReactiveFactor>> {
        let mut parser = Parser::new(&self.expression, input_schema)?;
        let ast = parser.parse_expression()?;
        parser.expect_end()?;

        Ok(Box::new(ExpressionFactor {
            ast: ast.clone(),
            output_field: Field::new(&self.output_field, ast.data_type.clone(), ast.nullable),
        }))
    }
}

struct ExpressionFactor {
    ast: TypedExpr,
    output_field: Field,
}

impl ReactiveFactor for ExpressionFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let mut values = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            values.push(evaluate_expr(&self.ast, batch, row)?);
        }

        build_output_array(self.output_field.data_type(), values)
    }
}

#[derive(Clone)]
struct TypedExpr {
    kind: TypedExprKind,
    data_type: DataType,
    nullable: bool,
}

#[derive(Clone)]
enum TypedExprKind {
    Number(f64),
    String(String),
    Identifier(String),
    UnaryNeg(Box<TypedExpr>),
    Binary {
        op: BinaryOp,
        left: Box<TypedExpr>,
        right: Box<TypedExpr>,
    },
    Function {
        kind: FunctionKind,
        args: Vec<TypedExpr>,
    },
}

#[derive(Clone, Copy)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy)]
enum FunctionKind {
    Abs,
    Log,
    Clip,
    Cast(CastKind),
}

#[derive(Clone, Copy)]
enum CastKind {
    Float64,
    Float32,
    Int64,
    Int32,
    Utf8,
}

impl CastKind {
    fn parse(dtype: &str) -> Result<Self> {
        match dtype {
            "float64" => Ok(Self::Float64),
            "float32" => Ok(Self::Float32),
            "int64" => Ok(Self::Int64),
            "int32" => Ok(Self::Int32),
            "utf8" | "string" => Ok(Self::Utf8),
            _ => Err(ZippyError::InvalidConfig {
                reason: format!("unsupported cast dtype dtype=[{}]", dtype),
            }),
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Float64 => DataType::Float64,
            Self::Float32 => DataType::Float32,
            Self::Int64 => DataType::Int64,
            Self::Int32 => DataType::Int32,
            Self::Utf8 => DataType::Utf8,
        }
    }
}

#[derive(Clone)]
enum EvalValue {
    Null,
    Float64(f64),
    Float32(f32),
    Int64(i64),
    Int32(i32),
    String(String),
}

impl EvalValue {
    fn as_f64(&self) -> Result<Option<f64>> {
        match self {
            Self::Null => Ok(None),
            Self::Float64(value) => Ok(Some(*value)),
            Self::Float32(value) => Ok(Some(*value as f64)),
            Self::Int64(value) => Ok(Some(*value as f64)),
            Self::Int32(value) => Ok(Some(*value as f64)),
            Self::String(_) => Err(ZippyError::InvalidConfig {
                reason: "expression expected numeric value but got utf8".to_string(),
            }),
        }
    }

    fn as_string(&self) -> Result<Option<String>> {
        match self {
            Self::Null => Ok(None),
            Self::Float64(value) => Ok(Some(value.to_string())),
            Self::Float32(value) => Ok(Some(value.to_string())),
            Self::Int64(value) => Ok(Some(value.to_string())),
            Self::Int32(value) => Ok(Some(value.to_string())),
            Self::String(value) => Ok(Some(value.clone())),
        }
    }
}

#[derive(Clone, Debug)]
enum Token {
    Ident(String),
    Number(f64),
    String(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    Comma,
    End,
}

struct Parser<'a> {
    tokens: Vec<Token>,
    index: usize,
    schema: &'a Schema,
}

impl<'a> Parser<'a> {
    fn new(expression: &str, schema: &'a Schema) -> Result<Self> {
        Ok(Self {
            tokens: tokenize(expression)?,
            index: 0,
            schema,
        })
    }

    fn parse_expression(&mut self) -> Result<TypedExpr> {
        self.parse_additive()
    }

    fn expect_end(&self) -> Result<()> {
        match self.current() {
            Token::End => Ok(()),
            token => Err(ZippyError::InvalidConfig {
                reason: format!(
                    "unexpected trailing token token=[{}]",
                    describe_token(token)
                ),
            }),
        }
    }

    fn parse_additive(&mut self) -> Result<TypedExpr> {
        let mut expr = self.parse_multiplicative()?;

        loop {
            let op = match self.current() {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            expr = build_binary_expr(op, expr, right)?;
        }

        Ok(expr)
    }

    fn parse_multiplicative(&mut self) -> Result<TypedExpr> {
        let mut expr = self.parse_unary()?;

        loop {
            let op = match self.current() {
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            expr = build_binary_expr(op, expr, right)?;
        }

        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<TypedExpr> {
        match self.current() {
            Token::Minus => {
                self.advance();
                let inner = self.parse_unary()?;
                ensure_numeric_type(&inner.data_type, "unary minus")?;
                Ok(TypedExpr {
                    kind: TypedExprKind::UnaryNeg(Box::new(inner.clone())),
                    data_type: DataType::Float64,
                    nullable: inner.nullable,
                })
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<TypedExpr> {
        match self.current().clone() {
            Token::Number(value) => {
                self.advance();
                Ok(TypedExpr {
                    kind: TypedExprKind::Number(value),
                    data_type: DataType::Float64,
                    nullable: false,
                })
            }
            Token::String(value) => {
                self.advance();
                Ok(TypedExpr {
                    kind: TypedExprKind::String(value),
                    data_type: DataType::Utf8,
                    nullable: false,
                })
            }
            Token::Ident(name) => {
                self.advance();
                if matches!(self.current(), Token::LParen) {
                    self.advance();
                    let args = self.parse_arguments()?;
                    self.expect_token(Token::RParen, "expected ')' to close function call")?;
                    build_function_expr(&name, args)
                } else {
                    build_identifier_expr(self.schema, &name)
                }
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen, "expected ')' to close expression")?;
                Ok(expr)
            }
            token => Err(ZippyError::InvalidConfig {
                reason: format!("unexpected token token=[{}]", describe_token(&token)),
            }),
        }
    }

    fn parse_arguments(&mut self) -> Result<Vec<TypedExpr>> {
        let mut args = Vec::new();

        if matches!(self.current(), Token::RParen) {
            return Ok(args);
        }

        loop {
            args.push(self.parse_expression()?);
            if matches!(self.current(), Token::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        Ok(args)
    }

    fn expect_token(&mut self, expected: Token, message: &str) -> Result<()> {
        if std::mem::discriminant(self.current()) != std::mem::discriminant(&expected) {
            return Err(ZippyError::InvalidConfig {
                reason: message.to_string(),
            });
        }
        self.advance();
        Ok(())
    }

    fn current(&self) -> &Token {
        self.tokens
            .get(self.index)
            .unwrap_or_else(|| self.tokens.last().expect("token stream is never empty"))
    }

    fn advance(&mut self) {
        if self.index < self.tokens.len().saturating_sub(1) {
            self.index += 1;
        }
    }
}

fn build_binary_expr(op: BinaryOp, left: TypedExpr, right: TypedExpr) -> Result<TypedExpr> {
    ensure_numeric_type(&left.data_type, "binary expression")?;
    ensure_numeric_type(&right.data_type, "binary expression")?;

    Ok(TypedExpr {
        kind: TypedExprKind::Binary {
            op,
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
        },
        data_type: DataType::Float64,
        nullable: left.nullable || right.nullable,
    })
}

fn build_identifier_expr(schema: &Schema, name: &str) -> Result<TypedExpr> {
    let index = schema
        .index_of(name)
        .map_err(|_| ZippyError::InvalidConfig {
            reason: format!("unknown expression identifier identifier=[{}]", name),
        })?;
    let field = schema.field(index);

    ensure_supported_scalar_type(field.data_type(), name)?;

    Ok(TypedExpr {
        kind: TypedExprKind::Identifier(name.to_string()),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
    })
}

fn build_function_expr(name: &str, args: Vec<TypedExpr>) -> Result<TypedExpr> {
    let builtin = match name {
        "ABS" => Some("ABS"),
        "LOG" => Some("LOG"),
        "CLIP" => Some("CLIP"),
        "CAST" => Some("CAST"),
        _ => ["ABS", "LOG", "CLIP", "CAST"]
            .into_iter()
            .find(|expected| name.eq_ignore_ascii_case(expected)),
    };

    if let Some(expected) = builtin {
        if name != expected {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "function names must be uppercase function=[{}] expected=[{}]",
                    name, expected
                ),
            });
        }
    }

    match name {
        "ABS" => {
            if args.len() != 1 {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "expression function abs expects 1 argument args=[{}]",
                        args.len()
                    ),
                });
            }
            let nullable = args[0].nullable;
            ensure_numeric_type(&args[0].data_type, "abs")?;
            Ok(TypedExpr {
                kind: TypedExprKind::Function {
                    kind: FunctionKind::Abs,
                    args,
                },
                data_type: DataType::Float64,
                nullable,
            })
        }
        "LOG" => {
            if args.len() != 1 {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "expression function log expects 1 argument args=[{}]",
                        args.len()
                    ),
                });
            }
            let nullable = args[0].nullable;
            ensure_numeric_type(&args[0].data_type, "log")?;
            Ok(TypedExpr {
                kind: TypedExprKind::Function {
                    kind: FunctionKind::Log,
                    args,
                },
                data_type: DataType::Float64,
                nullable,
            })
        }
        "CLIP" => {
            if args.len() != 3 {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "expression function clip expects 3 arguments args=[{}]",
                        args.len()
                    ),
                });
            }
            for arg in &args {
                ensure_numeric_type(&arg.data_type, "clip")?;
            }
            Ok(TypedExpr {
                kind: TypedExprKind::Function {
                    kind: FunctionKind::Clip,
                    args: args.clone(),
                },
                data_type: DataType::Float64,
                nullable: args.iter().any(|arg| arg.nullable),
            })
        }
        "CAST" => {
            if args.len() != 2 {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "expression function cast expects 2 arguments args=[{}]",
                        args.len()
                    ),
                });
            }
            let dtype = match &args[1].kind {
                TypedExprKind::String(value) => value.clone(),
                _ => {
                    return Err(ZippyError::InvalidConfig {
                        reason: "expression function cast expects a string dtype literal"
                            .to_string(),
                    })
                }
            };
            let cast_kind = CastKind::parse(&dtype)?;

            Ok(TypedExpr {
                kind: TypedExprKind::Function {
                    kind: FunctionKind::Cast(cast_kind),
                    args: vec![args[0].clone()],
                },
                data_type: cast_kind.data_type(),
                nullable: args[0].nullable,
            })
        }
        _ => Err(ZippyError::InvalidConfig {
            reason: format!("unsupported expression function function=[{}]", name),
        }),
    }
}

fn ensure_supported_scalar_type(data_type: &DataType, name: &str) -> Result<()> {
    match data_type {
        DataType::Float64
        | DataType::Float32
        | DataType::Int64
        | DataType::Int32
        | DataType::Utf8 => Ok(()),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unsupported expression field type field=[{}] dtype=[{:?}]",
                name, data_type
            ),
        }),
    }
}

fn ensure_numeric_type(data_type: &DataType, context: &str) -> Result<()> {
    match data_type {
        DataType::Float64 | DataType::Float32 | DataType::Int64 | DataType::Int32 => Ok(()),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "expression {} requires numeric inputs dtype=[{:?}]",
                context, data_type
            ),
        }),
    }
}

fn evaluate_expr(expr: &TypedExpr, batch: &RecordBatch, row: usize) -> Result<EvalValue> {
    match &expr.kind {
        TypedExprKind::Number(value) => Ok(EvalValue::Float64(*value)),
        TypedExprKind::String(value) => Ok(EvalValue::String(value.clone())),
        TypedExprKind::Identifier(name) => extract_batch_value(batch, name, row),
        TypedExprKind::UnaryNeg(inner) => {
            let value = evaluate_expr(inner, batch, row)?;
            match value.as_f64()? {
                Some(value) => Ok(EvalValue::Float64(-value)),
                None => Ok(EvalValue::Null),
            }
        }
        TypedExprKind::Binary { op, left, right } => {
            let left = evaluate_expr(left, batch, row)?;
            let right = evaluate_expr(right, batch, row)?;
            let Some(left) = left.as_f64()? else {
                return Ok(EvalValue::Null);
            };
            let Some(right) = right.as_f64()? else {
                return Ok(EvalValue::Null);
            };

            let value = match op {
                BinaryOp::Add => left + right,
                BinaryOp::Sub => left - right,
                BinaryOp::Mul => left * right,
                BinaryOp::Div => {
                    if right == 0.0 {
                        return Err(ZippyError::InvalidState {
                            status: EXPRESSION_DIVISION_BY_ZERO,
                        });
                    }
                    left / right
                }
            };

            Ok(EvalValue::Float64(value))
        }
        TypedExprKind::Function { kind, args } => match kind {
            FunctionKind::Abs => {
                let value = evaluate_expr(&args[0], batch, row)?;
                match value.as_f64()? {
                    Some(value) => Ok(EvalValue::Float64(value.abs())),
                    None => Ok(EvalValue::Null),
                }
            }
            FunctionKind::Log => {
                let value = evaluate_expr(&args[0], batch, row)?;
                match value.as_f64()? {
                    Some(value) => {
                        if value <= 0.0 {
                            return Err(ZippyError::InvalidState {
                                status: EXPRESSION_LOG_INPUT_MUST_BE_POSITIVE,
                            });
                        }
                        Ok(EvalValue::Float64(value.ln()))
                    }
                    None => Ok(EvalValue::Null),
                }
            }
            FunctionKind::Clip => {
                let value = evaluate_expr(&args[0], batch, row)?;
                let min = evaluate_expr(&args[1], batch, row)?;
                let max = evaluate_expr(&args[2], batch, row)?;

                let Some(value) = value.as_f64()? else {
                    return Ok(EvalValue::Null);
                };
                let Some(min) = min.as_f64()? else {
                    return Ok(EvalValue::Null);
                };
                let Some(max) = max.as_f64()? else {
                    return Ok(EvalValue::Null);
                };
                if min > max {
                    return Err(ZippyError::InvalidState {
                        status: EXPRESSION_CLIP_BOUNDS_INVALID,
                    });
                }

                Ok(EvalValue::Float64(value.clamp(min, max)))
            }
            FunctionKind::Cast(kind) => {
                let value = evaluate_expr(&args[0], batch, row)?;
                cast_value(*kind, value)
            }
        },
    }
}

fn cast_value(kind: CastKind, value: EvalValue) -> Result<EvalValue> {
    match kind {
        CastKind::Float64 => match value.as_f64()? {
            Some(value) => Ok(EvalValue::Float64(value)),
            None => Ok(EvalValue::Null),
        },
        CastKind::Float32 => match value.as_f64()? {
            Some(value) => Ok(EvalValue::Float32(value as f32)),
            None => Ok(EvalValue::Null),
        },
        CastKind::Int64 => match value {
            EvalValue::Null => Ok(EvalValue::Null),
            EvalValue::String(value) => {
                value.parse::<i64>().map(EvalValue::Int64).map_err(|error| {
                    ZippyError::InvalidConfig {
                        reason: format!("failed to cast utf8 to int64 error=[{}]", error),
                    }
                })
            }
            other => other
                .as_f64()?
                .map(|value| EvalValue::Int64(value as i64))
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "failed to cast null to int64".to_string(),
                }),
        },
        CastKind::Int32 => match value {
            EvalValue::Null => Ok(EvalValue::Null),
            EvalValue::String(value) => {
                value.parse::<i32>().map(EvalValue::Int32).map_err(|error| {
                    ZippyError::InvalidConfig {
                        reason: format!("failed to cast utf8 to int32 error=[{}]", error),
                    }
                })
            }
            other => other
                .as_f64()?
                .map(|value| EvalValue::Int32(value as i32))
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "failed to cast null to int32".to_string(),
                }),
        },
        CastKind::Utf8 => value
            .as_string()
            .map(|value| value.map_or(EvalValue::Null, EvalValue::String)),
    }
}

fn build_output_array(data_type: &DataType, values: Vec<EvalValue>) -> Result<ArrayRef> {
    match data_type {
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for value in &values {
                match value.as_f64()? {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(values.len());
            for value in &values {
                match value.as_f64()? {
                    Some(value) => builder.append_value(value as f32),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for value in &values {
                match value {
                    EvalValue::Null => builder.append_null(),
                    EvalValue::Int64(value) => builder.append_value(*value),
                    other => match other.as_f64()? {
                        Some(value) => builder.append_value(value as i64),
                        None => builder.append_null(),
                    },
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for value in &values {
                match value {
                    EvalValue::Null => builder.append_null(),
                    EvalValue::Int32(value) => builder.append_value(*value),
                    other => match other.as_f64()? {
                        Some(value) => builder.append_value(value as i32),
                        None => builder.append_null(),
                    },
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
            for value in &values {
                match value.as_string()? {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unsupported expression output dtype dtype=[{:?}]",
                data_type
            ),
        }),
    }
}

fn extract_batch_value(batch: &RecordBatch, name: &str, row: usize) -> Result<EvalValue> {
    let schema = batch.schema();
    let index = schema
        .index_of(name)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing expression field field=[{}]", name),
        })?;
    let field = schema.field(index);
    let array = batch.column(index);

    if array.is_null(row) {
        return Ok(EvalValue::Null);
    }

    match field.data_type() {
        DataType::Float64 => Ok(EvalValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: format!("expression field type mismatch field=[{}]", name),
                })?
                .value(row),
        )),
        DataType::Float32 => Ok(EvalValue::Float32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: format!("expression field type mismatch field=[{}]", name),
                })?
                .value(row),
        )),
        DataType::Int64 => Ok(EvalValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: format!("expression field type mismatch field=[{}]", name),
                })?
                .value(row),
        )),
        DataType::Int32 => Ok(EvalValue::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: format!("expression field type mismatch field=[{}]", name),
                })?
                .value(row),
        )),
        DataType::Utf8 => Ok(EvalValue::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: format!("expression field type mismatch field=[{}]", name),
                })?
                .value(row)
                .to_string(),
        )),
        data_type => Err(ZippyError::SchemaMismatch {
            reason: format!(
                "unsupported expression field type at evaluation field=[{}] dtype=[{:?}]",
                name, data_type
            ),
        }),
    }
}

fn tokenize(expression: &str) -> Result<Vec<Token>> {
    let mut chars = expression.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(ch) = chars.peek().copied() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '+' => {
                chars.next();
                tokens.push(Token::Plus);
            }
            '-' => {
                chars.next();
                tokens.push(Token::Minus);
            }
            '*' => {
                chars.next();
                tokens.push(Token::Star);
            }
            '/' => {
                chars.next();
                tokens.push(Token::Slash);
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            ',' => {
                chars.next();
                tokens.push(Token::Comma);
            }
            '0'..='9' | '.' => {
                let mut value = String::new();
                while let Some(next) = chars.peek() {
                    if next.is_ascii_digit() || *next == '.' {
                        value.push(*next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let number = value
                    .parse::<f64>()
                    .map_err(|error| ZippyError::InvalidConfig {
                        reason: format!(
                            "invalid numeric literal literal=[{}] error=[{}]",
                            value, error
                        ),
                    })?;
                tokens.push(Token::Number(number));
            }
            '\'' | '"' => {
                let quote = ch;
                chars.next();
                let mut value = String::new();
                let mut closed = false;
                for next in chars.by_ref() {
                    if next == quote {
                        closed = true;
                        break;
                    }
                    value.push(next);
                }
                if !closed {
                    return Err(ZippyError::InvalidConfig {
                        reason: "unterminated string literal in expression".to_string(),
                    });
                }
                tokens.push(Token::String(value));
            }
            _ if ch.is_ascii_alphabetic() || ch == '_' => {
                let mut value = String::new();
                while let Some(next) = chars.peek() {
                    if next.is_ascii_alphanumeric() || *next == '_' {
                        value.push(*next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Ident(value));
            }
            _ => {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("unexpected character in expression char=[{}]", ch),
                })
            }
        }
    }

    tokens.push(Token::End);
    Ok(tokens)
}

fn describe_token(token: &Token) -> String {
    match token {
        Token::Ident(value) => format!("ident({value})"),
        Token::Number(value) => format!("number({value})"),
        Token::String(value) => format!("string({value})"),
        Token::Plus => "+".to_string(),
        Token::Minus => "-".to_string(),
        Token::Star => "*".to_string(),
        Token::Slash => "/".to_string(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::Comma => ",".to_string(),
        Token::End => "end".to_string(),
    }
}
