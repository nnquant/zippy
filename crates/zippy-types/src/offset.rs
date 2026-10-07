//! Logical log positions with explicit empty and known boundaries.

use std::fmt;

use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::{ConsumerGroupId, DurableConsumerId};

/// One valid logical position in a table log.
///
/// Offset zero is the first valid position. An empty table is represented by
/// [`OffsetWatermark::Empty`], never by a sentinel offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogOffset(u64);

impl LogOffset {
    /// Creates a logical offset, including the valid first offset zero.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying fixed-width value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next offset, or `None` when the log exhausted `u64` space.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for LogOffset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The committed end position of a table or source whose history may be absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "state", content = "offset", rename_all = "snake_case")]
pub enum OffsetWatermark {
    /// No row has ever been assigned a logical offset.
    Empty,
    /// The greatest committed logical offset is known, including offset zero.
    Known(LogOffset),
}

/// The last accepted batch sequence when sequence zero is itself valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "state", content = "sequence", rename_all = "snake_case")]
pub enum SequenceWatermark {
    /// No batch has been accepted in this producer epoch.
    Empty,
    /// The greatest accepted sequence, including valid sequence zero.
    Known(u64),
}

/// A durable consumer position with an explicit empty state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "state", content = "offset", rename_all = "snake_case")]
pub enum CommittedOffset {
    /// No logical position has been processed, including an empty snapshot.
    Empty,
    /// The greatest completely processed logical offset.
    Known(LogOffset),
}

/// The persistent record whose committed offset governs replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum ConsumerOffsetAuthority {
    /// Replay is governed by one durable consumer's individual offset.
    Consumer(DurableConsumerId),
    /// Replay is governed by the shared offset of a coordinated group.
    Group(ConsumerGroupId),
}

impl CommittedOffset {
    /// Returns the first offset after this committed position.
    ///
    /// An empty position starts at offset zero. `None` means the known position
    /// exhausted the `u64` offset space.
    #[must_use]
    pub const fn next_offset(self) -> Option<LogOffset> {
        match self {
            Self::Empty => Some(LogOffset::new(0)),
            Self::Known(offset) => offset.checked_next(),
        }
    }
}

impl OffsetWatermark {
    /// Returns the known offset, excluding the empty state.
    #[must_use]
    pub const fn known(self) -> Option<LogOffset> {
        match self {
            Self::Known(offset) => Some(offset),
            Self::Empty => None,
        }
    }
}

/// An error returned when constructing an invalid inclusive offset range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid offset range first=[{first}] last=[{last}]")]
pub struct InvalidOffsetRange {
    /// The proposed inclusive first offset.
    pub first: LogOffset,
    /// The proposed inclusive last offset.
    pub last: LogOffset,
}

/// A non-empty inclusive range of logical log offsets.
///
/// Zero-row batches have no range and therefore use `Option<OffsetRange>`.
/// This type can never encode an empty or reversed range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OffsetRange {
    first: LogOffset,
    last: LogOffset,
}

impl OffsetRange {
    /// Creates an inclusive range when `first <= last`.
    pub const fn new(first: LogOffset, last: LogOffset) -> Result<Self, InvalidOffsetRange> {
        if first.get() <= last.get() {
            Ok(Self { first, last })
        } else {
            Err(InvalidOffsetRange { first, last })
        }
    }

    /// Creates a one-position range.
    #[must_use]
    pub const fn single(offset: LogOffset) -> Self {
        Self {
            first: offset,
            last: offset,
        }
    }

    /// Returns the inclusive first offset.
    #[must_use]
    pub const fn first(self) -> LogOffset {
        self.first
    }

    /// Returns the inclusive last offset.
    #[must_use]
    pub const fn last(self) -> LogOffset {
        self.last
    }

    /// Returns the number of positions without overflowing at the full `u64` range.
    #[must_use]
    pub const fn len(self) -> u128 {
        self.last.get() as u128 - self.first.get() as u128 + 1
    }

    /// Returns false because the type cannot represent an empty range.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        false
    }

    /// Returns true when the range contains the given offset.
    #[must_use]
    pub const fn contains(self, offset: LogOffset) -> bool {
        self.first.get() <= offset.get() && offset.get() <= self.last.get()
    }
}

impl Serialize for OffsetRange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("OffsetRange", 2)?;
        state.serialize_field("first", &self.first)?;
        state.serialize_field("last", &self.last)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for OffsetRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawOffsetRange {
            first: LogOffset,
            last: LogOffset,
        }

        let raw = RawOffsetRange::deserialize(deserializer)?;
        Self::new(raw.first, raw.last).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::{CommittedOffset, LogOffset, OffsetRange, OffsetWatermark, SequenceWatermark};

    #[test]
    fn offset_zero_is_valid_but_not_an_empty_watermark() {
        let zero = LogOffset::new(0);

        assert_eq!(OffsetWatermark::Known(zero).known(), Some(zero));
        assert_eq!(OffsetWatermark::Empty.known(), None);
        assert_ne!(OffsetWatermark::Known(zero), OffsetWatermark::Empty);
        assert_ne!(SequenceWatermark::Known(0), SequenceWatermark::Empty);
        assert_eq!(CommittedOffset::Empty.next_offset(), Some(zero));
        assert_eq!(
            CommittedOffset::Known(zero).next_offset(),
            Some(LogOffset::new(1))
        );
    }

    #[test]
    fn range_rejects_reverse_order_from_constructor_and_serde() {
        assert!(OffsetRange::new(LogOffset::new(2), LogOffset::new(1)).is_err());
        assert!(serde_json::from_str::<OffsetRange>(r#"{"first":2,"last":1}"#).is_err());
    }

    #[test]
    fn offset_types_have_golden_json_forms() {
        let range = OffsetRange::new(LogOffset::new(0), LogOffset::new(2)).unwrap();

        assert_eq!(
            serde_json::to_string(&range).unwrap(),
            r#"{"first":0,"last":2}"#
        );
        assert_eq!(range.len(), 3);
        assert!(serde_json::from_str::<OffsetWatermark>(r#"{"state":"legacy_unknown"}"#).is_err());
    }
}
