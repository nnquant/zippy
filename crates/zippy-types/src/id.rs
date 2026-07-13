//! Non-nil stable identifiers shared across persistent and runtime boundaries.

use std::fmt;
use std::num::NonZeroU128;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// The byte width of every stable identifier.
pub const STABLE_ID_BYTES: usize = 16;

/// An error returned when constructing a stable identifier from a nil value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("stable id must not be nil")]
pub struct NilStableIdError;

/// An error returned when parsing a stable identifier.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseStableIdError {
    /// The input does not use either the 32-digit or canonical 36-character form.
    #[error("invalid stable id length actual=[{actual}]")]
    InvalidLength {
        /// The number of bytes in the input string.
        actual: usize,
    },
    /// Hyphens are not in the canonical positions.
    #[error("invalid stable id format")]
    InvalidFormat,
    /// A non-hexadecimal digit occurs at the given normalized position.
    #[error("invalid stable id hex digit index=[{index}]")]
    InvalidHex {
        /// The zero-based position after removing canonical hyphens.
        index: usize,
    },
    /// The parsed identifier is nil, which is never a valid identity.
    #[error("stable id must not be nil")]
    Nil,
}

fn parse_stable_id(value: &str) -> Result<NonZeroU128, ParseStableIdError> {
    let normalized = match value.len() {
        32 => value.as_bytes().to_vec(),
        36 => {
            let bytes = value.as_bytes();
            if [8, 13, 18, 23]
                .into_iter()
                .any(|index| bytes[index] != b'-')
            {
                return Err(ParseStableIdError::InvalidFormat);
            }
            bytes
                .iter()
                .enumerate()
                .filter_map(|(index, byte)| {
                    if matches!(index, 8 | 13 | 18 | 23) {
                        None
                    } else {
                        Some(*byte)
                    }
                })
                .collect()
        }
        actual => return Err(ParseStableIdError::InvalidLength { actual }),
    };

    let mut bytes = [0_u8; STABLE_ID_BYTES];
    for (index, pair) in normalized.chunks_exact(2).enumerate() {
        let high = decode_hex(pair[0], index * 2)?;
        let low = decode_hex(pair[1], index * 2 + 1)?;
        bytes[index] = (high << 4) | low;
    }
    NonZeroU128::new(u128::from_be_bytes(bytes)).ok_or(ParseStableIdError::Nil)
}

fn decode_hex(byte: u8, index: usize) -> Result<u8, ParseStableIdError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ParseStableIdError::InvalidHex { index }),
    }
}

fn format_stable_id(value: NonZeroU128, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    for (index, byte) in value.get().to_be_bytes().iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            formatter.write_str("-")?;
        }
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

macro_rules! stable_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[repr(transparent)]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU128);

        impl $name {
            /// Creates an identifier from a value already proven nonzero.
            #[must_use]
            pub const fn from_nonzero(value: NonZeroU128) -> Self {
                Self(value)
            }

            /// Tries to create an identifier from a 16-byte network-order value.
            pub const fn from_bytes(
                bytes: [u8; STABLE_ID_BYTES],
            ) -> Result<Self, NilStableIdError> {
                Self::from_u128(u128::from_be_bytes(bytes))
            }

            /// Tries to create an identifier from an unsigned integer.
            pub const fn from_u128(value: u128) -> Result<Self, NilStableIdError> {
                match NonZeroU128::new(value) {
                    Some(value) => Ok(Self(value)),
                    None => Err(NilStableIdError),
                }
            }

            /// Returns the canonical 16-byte network-order representation.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; STABLE_ID_BYTES] {
                self.0.get().to_be_bytes()
            }

            /// Returns the nonzero integer representation.
            #[must_use]
            pub const fn as_nonzero(self) -> NonZeroU128 {
                self.0
            }

            /// Returns the unsigned integer representation.
            #[must_use]
            pub const fn as_u128(self) -> u128 {
                self.0.get()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_string())
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                format_stable_id(self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = ParseStableIdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_stable_id(value).map(Self)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

stable_id!(NamespaceId, "A persistent catalog namespace identifier.");
stable_id!(TableId, "A persistent catalog table identifier.");
stable_id!(FileId, "An immutable managed data-file identifier.");
stable_id!(
    RequestId,
    "An idempotent client or control request identifier."
);
stable_id!(ProducerId, "A durable ingest producer identifier.");
stable_id!(
    DurableConsumerId,
    "A durable subscription consumer identity that survives reconnects."
);
stable_id!(
    ConsumerGroupId,
    "A durable consumer-group identity shared by coordinated consumers."
);
stable_id!(
    ConnectionId,
    "A boot-scoped client or subscription connection identity."
);
stable_id!(
    LeaseId,
    "A persistent or in-memory snapshot lease identifier."
);
stable_id!(JobId, "A persistent maintenance job identifier.");
stable_id!(
    QueryId,
    "A query identifier used for status and cancellation."
);
stable_id!(SessionId, "A client or server runtime session identifier.");
stable_id!(ProcessId, "A runtime process instance identifier.");
stable_id!(SourceId, "A runtime source instance identifier.");
stable_id!(EngineId, "A runtime compute-engine instance identifier.");

#[cfg(test)]
mod tests {
    use super::{NilStableIdError, ParseStableIdError, RequestId};

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).expect("test id must be nonzero")
    }

    #[test]
    fn stable_id_round_trips_all_supported_forms() {
        let id = request_id(0x00112233_4455_6677_8899_aabbccddeeff);
        let canonical = "00112233-4455-6677-8899-aabbccddeeff";

        assert_eq!(id.to_string(), canonical);
        assert_eq!(canonical.parse::<RequestId>(), Ok(id));
        assert_eq!("00112233445566778899AABBCCDDEEFF".parse(), Ok(id));
        assert_eq!(id.as_u128(), 0x00112233_4455_6677_8899_aabbccddeeff);
        assert_eq!(RequestId::from_bytes(id.into_bytes()), Ok(id));
    }

    #[test]
    fn stable_id_has_a_golden_string_json_representation() {
        let id = request_id(1);
        let encoded = serde_json::to_string(&id).expect("serialize stable id");
        let decoded: RequestId = serde_json::from_str(&encoded).expect("deserialize stable id");

        assert_eq!(encoded, "\"00000000-0000-0000-0000-000000000001\"");
        assert_eq!(decoded, id);
    }

    #[test]
    fn stable_id_rejects_nil_from_every_public_input() {
        assert_eq!(RequestId::from_u128(0), Err(NilStableIdError));
        assert_eq!(RequestId::from_bytes([0; 16]), Err(NilStableIdError));
        assert_eq!(
            "00000000-0000-0000-0000-000000000000".parse::<RequestId>(),
            Err(ParseStableIdError::Nil)
        );
        assert!(
            serde_json::from_str::<RequestId>("\"00000000-0000-0000-0000-000000000000\"").is_err()
        );
    }

    #[test]
    fn stable_id_rejects_ambiguous_input() {
        assert_eq!(
            "00112233_4455_6677_8899_aabbccddeeff".parse::<RequestId>(),
            Err(ParseStableIdError::InvalidFormat)
        );
        assert_eq!(
            "xyz".parse::<RequestId>(),
            Err(ParseStableIdError::InvalidLength { actual: 3 })
        );
    }
}
