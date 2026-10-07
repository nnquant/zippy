use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// 列的数据类型定义。
#[derive(Debug, Clone)]
pub enum ColumnType {
    Int64,
    Float64,
    Utf8,
    TimestampNsTz(&'static str),
    TimestampNsTzOwned(Arc<str>),
}

impl ColumnType {
    /// Construct a timestamp column type with an owned timezone name.
    pub fn timestamp_ns_tz(timezone: impl Into<Arc<str>>) -> Self {
        Self::TimestampNsTzOwned(timezone.into())
    }

    /// Return the timezone name for timestamp columns.
    pub fn timezone(&self) -> Option<&str> {
        match self {
            Self::TimestampNsTz(timezone) => Some(timezone),
            Self::TimestampNsTzOwned(timezone) => Some(timezone.as_ref()),
            _ => None,
        }
    }
}

impl PartialEq for ColumnType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int64, Self::Int64)
            | (Self::Float64, Self::Float64)
            | (Self::Utf8, Self::Utf8) => true,
            (Self::TimestampNsTz(left), Self::TimestampNsTz(right)) => left == right,
            (Self::TimestampNsTz(left), Self::TimestampNsTzOwned(right)) => *left == right.as_ref(),
            (Self::TimestampNsTzOwned(left), Self::TimestampNsTz(right)) => left.as_ref() == *right,
            (Self::TimestampNsTzOwned(left), Self::TimestampNsTzOwned(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for ColumnType {}

/// 列规格定义。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: Arc<str>,
    pub data_type: ColumnType,
    pub nullable: bool,
}

impl ColumnSpec {
    /// 构造一个列规格。
    pub fn new(name: impl Into<Arc<str>>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
        }
    }

    /// 构造一个可空列规格。
    pub fn nullable(name: impl Into<Arc<str>>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    /// Return the column name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

/// 编译后的 schema 占位结构。
#[derive(Debug, Clone)]
pub struct CompiledSchema {
    schema_id: u64,
    columns: Arc<[ColumnSpec]>,
    arrow_metadata: Arc<ArrowMetadata>,
}

#[derive(Debug)]
struct ArrowMetadata {
    physical_schema_id: u64,
    root: HashMap<String, String>,
    fields: Vec<HashMap<String, String>>,
}

impl CompiledSchema {
    /// Retains Arrow metadata and includes it in the deterministic shared-memory identity.
    pub fn with_arrow_metadata(
        self,
        schema: &arrow::datatypes::Schema,
    ) -> Result<Self, &'static str> {
        if self.columns.len() != schema.fields().len()
            || self
                .columns
                .iter()
                .zip(schema.fields())
                .any(|(column, field)| column.name() != field.name())
        {
            return Err("metadata schema columns differ from compiled columns");
        }
        self.with_metadata(
            schema.metadata().clone(),
            schema
                .fields()
                .iter()
                .map(|field| field.metadata().clone())
                .collect(),
        )
    }

    /// Applies root and ordered field metadata without duplicating the physical type contract.
    pub fn with_metadata(
        mut self,
        root: HashMap<String, String>,
        fields: Vec<HashMap<String, String>>,
    ) -> Result<Self, &'static str> {
        if fields.len() != self.columns.len() {
            return Err("field metadata count differs from compiled columns");
        }
        self.arrow_metadata = Arc::new(ArrowMetadata {
            physical_schema_id: self.arrow_metadata.physical_schema_id,
            root,
            fields,
        });
        // Recompute from the column contract so repeated metadata application is idempotent.
        self.schema_id = self.arrow_metadata.physical_schema_id;
        for metadata in
            std::iter::once(&self.arrow_metadata.root).chain(&self.arrow_metadata.fields)
        {
            let ordered = metadata
                .iter()
                .collect::<std::collections::BTreeMap<_, _>>();
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&(ordered.len() as u64).to_le_bytes());
            for (key, value) in ordered {
                bytes.extend_from_slice(&(key.len() as u64).to_le_bytes());
                bytes.extend_from_slice(key.as_bytes());
                bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
            for byte in bytes {
                self.schema_id ^= u64::from(byte);
                self.schema_id = self.schema_id.wrapping_mul(0x100000001b3);
            }
        }
        Ok(self)
    }

    pub(crate) fn metadata(&self) -> &HashMap<String, String> {
        &self.arrow_metadata.root
    }

    pub(crate) fn field_metadata(&self, index: usize) -> &HashMap<String, String> {
        &self.arrow_metadata.fields[index]
    }

    /// 返回 schema 标识。
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// 返回编译后的列集合。
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }
}

/// 编译列规格为 schema 占位结构。
pub fn compile_schema(columns: &[ColumnSpec]) -> Result<CompiledSchema, &'static str> {
    fn write_u64(bytes: &mut Vec<u8>, value: u64) {
        bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_type_fingerprint(bytes: &mut Vec<u8>, data_type: &ColumnType) {
        match data_type {
            ColumnType::Int64 => bytes.push(1),
            ColumnType::Float64 => bytes.push(2),
            ColumnType::Utf8 => bytes.push(3),
            ColumnType::TimestampNsTz(timezone) => {
                bytes.push(4);
                write_u64(bytes, timezone.len() as u64);
                bytes.extend_from_slice(timezone.as_bytes());
            }
            ColumnType::TimestampNsTzOwned(timezone) => {
                bytes.push(4);
                write_u64(bytes, timezone.len() as u64);
                bytes.extend_from_slice(timezone.as_bytes());
            }
        }
    }

    fn fingerprint_schema(columns: &[ColumnSpec]) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut bytes = Vec::new();
        write_u64(&mut bytes, columns.len() as u64);

        for column in columns {
            write_u64(&mut bytes, column.name().len() as u64);
            bytes.extend_from_slice(column.name().as_bytes());
            bytes.push(u8::from(column.nullable));
            push_type_fingerprint(&mut bytes, &column.data_type);
        }

        let mut hash = FNV_OFFSET_BASIS;
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        hash
    }

    let mut names = HashSet::with_capacity(columns.len());
    for column in columns {
        if !names.insert(column.name()) {
            return Err("duplicate column name");
        }
    }

    let physical_schema_id = fingerprint_schema(columns);
    let fields = vec![HashMap::new(); columns.len()];
    // Apply empty metadata so bare and metadata-carrying schemas share one identity domain.
    CompiledSchema {
        schema_id: physical_schema_id,
        columns: columns.into(),
        arrow_metadata: Arc::new(ArrowMetadata {
            physical_schema_id,
            root: HashMap::new(),
            fields: fields.clone(),
        }),
    }
    .with_metadata(HashMap::new(), fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn arrow_metadata_survives_compilation_and_changes_identity_deterministically() {
        let columns = [ColumnSpec::new("key", ColumnType::Utf8)];
        let bare = compile_schema(&columns).unwrap();
        let plain = Schema::new(vec![Field::new("key", DataType::Utf8, false)]);
        assert_eq!(
            bare.clone()
                .with_arrow_metadata(&plain)
                .unwrap()
                .schema_id(),
            bare.schema_id()
        );
        let schema = Schema::new_with_metadata(
            vec![Field::new("key", DataType::Utf8, false)
                .with_metadata([("unit".to_owned(), "identifier".to_owned())].into())],
            [
                ("source".to_owned(), "fixture".to_owned()),
                ("version".to_owned(), "1".to_owned()),
            ]
            .into(),
        );
        let compiled = bare.clone().with_arrow_metadata(&schema).unwrap();
        assert_eq!(
            crate::arrow_bridge::build_arrow_schema(&compiled).as_ref(),
            &schema
        );
        assert_ne!(compiled.schema_id(), bare.schema_id());
        assert_eq!(
            compiled
                .clone()
                .with_arrow_metadata(&schema)
                .unwrap()
                .schema_id(),
            compiled.schema_id()
        );
        let mut metadata = schema.metadata().clone();
        metadata.insert("version".to_owned(), "2".to_owned());
        let changed = schema.clone().with_metadata(metadata);
        assert_ne!(
            bare.with_arrow_metadata(&changed).unwrap().schema_id(),
            compiled.schema_id()
        );
    }
}
