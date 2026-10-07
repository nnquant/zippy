//! Lossless fixed-width encodings for SQLite Catalog integer and watermark values.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "physical v2 encodings land before their command executors"
    )
)]

use zippy_types::{
    CommittedOffset, ErrorCode, LogOffset, OffsetWatermark, Result, SequenceWatermark, ServerError,
};

const U64_ENCODED_LEN: usize = size_of::<u64>();
const TAGGED_U64_ENCODED_LEN: usize = 1 + U64_ENCODED_LEN;
const TAG_NONE_OR_EMPTY: u8 = 0;
const TAG_SOME_OR_KNOWN: u8 = 1;

/// Encodes an unsigned integer as exactly eight big-endian bytes.
#[must_use]
pub(crate) const fn encode_u64(value: u64) -> [u8; U64_ENCODED_LEN] {
    value.to_be_bytes()
}

/// Decodes an unsigned integer from exactly eight big-endian bytes.
pub(crate) fn decode_u64(encoded: &[u8]) -> Result<u64> {
    let bytes = exact_array::<U64_ENCODED_LEN>(encoded, "u64")?;
    Ok(u64::from_be_bytes(bytes))
}

/// Encodes an optional unsigned integer with an explicit tag and fixed-width payload.
#[must_use]
pub(crate) fn encode_optional_u64(value: Option<u64>) -> [u8; TAGGED_U64_ENCODED_LEN] {
    match value {
        None => encode_tagged_u64(TAG_NONE_OR_EMPTY, 0),
        Some(value) => encode_tagged_u64(TAG_SOME_OR_KNOWN, value),
    }
}

/// Decodes an optional unsigned integer from its canonical tagged representation.
pub(crate) fn decode_optional_u64(encoded: &[u8]) -> Result<Option<u64>> {
    let (tag, payload) = decode_tagged_u64(encoded, "optional u64")?;
    match tag {
        TAG_NONE_OR_EMPTY => {
            require_zero_payload(payload, "optional u64 none")?;
            Ok(None)
        }
        TAG_SOME_OR_KNOWN => Ok(Some(payload)),
        _ => Err(invalid_tag("optional u64", tag)),
    }
}

/// Encodes an offset watermark with an explicit state tag and fixed-width payload.
#[must_use]
pub(crate) fn encode_offset_watermark(value: OffsetWatermark) -> [u8; TAGGED_U64_ENCODED_LEN] {
    match value {
        OffsetWatermark::Empty => encode_tagged_u64(TAG_NONE_OR_EMPTY, 0),
        OffsetWatermark::Known(offset) => encode_tagged_u64(TAG_SOME_OR_KNOWN, offset.get()),
    }
}

/// Decodes an offset watermark from its canonical tagged representation.
pub(crate) fn decode_offset_watermark(encoded: &[u8]) -> Result<OffsetWatermark> {
    let (tag, payload) = decode_tagged_u64(encoded, "offset watermark")?;
    match tag {
        TAG_NONE_OR_EMPTY => {
            require_zero_payload(payload, "offset watermark empty")?;
            Ok(OffsetWatermark::Empty)
        }
        TAG_SOME_OR_KNOWN => Ok(OffsetWatermark::Known(LogOffset::new(payload))),
        _ => Err(invalid_tag("offset watermark", tag)),
    }
}

/// Encodes a sequence watermark with an explicit state tag and fixed-width payload.
#[must_use]
pub(crate) fn encode_sequence_watermark(value: SequenceWatermark) -> [u8; TAGGED_U64_ENCODED_LEN] {
    match value {
        SequenceWatermark::Empty => encode_tagged_u64(TAG_NONE_OR_EMPTY, 0),
        SequenceWatermark::Known(sequence) => encode_tagged_u64(TAG_SOME_OR_KNOWN, sequence),
    }
}

/// Decodes a sequence watermark from its canonical tagged representation.
pub(crate) fn decode_sequence_watermark(encoded: &[u8]) -> Result<SequenceWatermark> {
    let (tag, payload) = decode_tagged_u64(encoded, "sequence watermark")?;
    match tag {
        TAG_NONE_OR_EMPTY => {
            require_zero_payload(payload, "sequence watermark empty")?;
            Ok(SequenceWatermark::Empty)
        }
        TAG_SOME_OR_KNOWN => Ok(SequenceWatermark::Known(payload)),
        _ => Err(invalid_tag("sequence watermark", tag)),
    }
}

/// Encodes a committed offset with an explicit state tag and fixed-width payload.
#[must_use]
pub(crate) fn encode_committed_offset(value: CommittedOffset) -> [u8; TAGGED_U64_ENCODED_LEN] {
    match value {
        CommittedOffset::Empty => encode_tagged_u64(TAG_NONE_OR_EMPTY, 0),
        CommittedOffset::Known(offset) => encode_tagged_u64(TAG_SOME_OR_KNOWN, offset.get()),
    }
}

/// Decodes a committed offset from its canonical tagged representation.
pub(crate) fn decode_committed_offset(encoded: &[u8]) -> Result<CommittedOffset> {
    let (tag, payload) = decode_tagged_u64(encoded, "committed offset")?;
    match tag {
        TAG_NONE_OR_EMPTY => {
            require_zero_payload(payload, "committed offset empty")?;
            Ok(CommittedOffset::Empty)
        }
        TAG_SOME_OR_KNOWN => Ok(CommittedOffset::Known(LogOffset::new(payload))),
        _ => Err(invalid_tag("committed offset", tag)),
    }
}

fn encode_tagged_u64(tag: u8, payload: u64) -> [u8; TAGGED_U64_ENCODED_LEN] {
    let mut encoded = [0_u8; TAGGED_U64_ENCODED_LEN];
    encoded[0] = tag;
    encoded[1..].copy_from_slice(&payload.to_be_bytes());
    encoded
}

fn decode_tagged_u64(encoded: &[u8], kind: &str) -> Result<(u8, u64)> {
    let bytes = exact_array::<TAGGED_U64_ENCODED_LEN>(encoded, kind)?;
    let payload = u64::from_be_bytes(
        bytes[1..]
            .try_into()
            .expect("fixed-width tagged payload has eight bytes"),
    );
    Ok((bytes[0], payload))
}

fn exact_array<const N: usize>(encoded: &[u8], kind: &str) -> Result<[u8; N]> {
    encoded.try_into().map_err(|_| {
        corruption(format!(
            "invalid catalog encoding length kind=[{kind}] expected=[{N}] actual=[{}]",
            encoded.len()
        ))
    })
}

fn require_zero_payload(payload: u64, kind: &str) -> Result<()> {
    if payload == 0 {
        Ok(())
    } else {
        Err(corruption(format!(
            "nonzero reserved catalog payload kind=[{kind}] payload=[{payload}]"
        )))
    }
}

fn invalid_tag(kind: &str, tag: u8) -> ServerError {
    corruption(format!(
        "invalid catalog encoding tag kind=[{kind}] tag=[{tag}]"
    ))
}

fn corruption(message: String) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
}

#[cfg(test)]
mod tests {
    use zippy_types::{CommittedOffset, ErrorCode, LogOffset, OffsetWatermark, SequenceWatermark};

    use super::{
        decode_committed_offset, decode_offset_watermark, decode_optional_u64,
        decode_sequence_watermark, decode_u64, encode_committed_offset, encode_offset_watermark,
        encode_optional_u64, encode_sequence_watermark, encode_u64, TAGGED_U64_ENCODED_LEN,
        U64_ENCODED_LEN,
    };

    const BOUNDARY_VALUES: [u64; 5] = [
        0,
        1_u64 << 53,
        i64::MAX as u64,
        (i64::MAX as u64) + 1,
        u64::MAX,
    ];

    #[test]
    fn u64_round_trips_full_unsigned_boundaries() {
        for value in BOUNDARY_VALUES {
            let encoded = encode_u64(value);
            assert_eq!(encoded.len(), U64_ENCODED_LEN);
            assert_eq!(decode_u64(&encoded).unwrap(), value);
        }
    }

    #[test]
    fn big_endian_u64_encoding_preserves_unsigned_order() {
        let mut encoded = BOUNDARY_VALUES.map(encode_u64);
        encoded.reverse();
        encoded.sort();

        let decoded = encoded
            .iter()
            .map(|value| decode_u64(value).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(decoded, BOUNDARY_VALUES);
    }

    #[test]
    fn optional_u64_round_trips_none_and_all_boundary_values() {
        let none = encode_optional_u64(None);
        assert_eq!(none.len(), TAGGED_U64_ENCODED_LEN);
        assert_eq!(decode_optional_u64(&none).unwrap(), None);

        for value in BOUNDARY_VALUES {
            let encoded = encode_optional_u64(Some(value));
            assert_eq!(decode_optional_u64(&encoded).unwrap(), Some(value));
        }
    }

    #[test]
    fn offset_watermark_round_trips_every_state_and_boundary() {
        assert_eq!(
            decode_offset_watermark(&encode_offset_watermark(OffsetWatermark::Empty)).unwrap(),
            OffsetWatermark::Empty
        );
        for value in BOUNDARY_VALUES {
            let state = OffsetWatermark::Known(LogOffset::new(value));
            assert_eq!(
                decode_offset_watermark(&encode_offset_watermark(state)).unwrap(),
                state
            );
        }
    }

    #[test]
    fn sequence_watermark_round_trips_every_state_and_boundary() {
        let empty = encode_sequence_watermark(SequenceWatermark::Empty);
        assert_eq!(
            decode_sequence_watermark(&empty).unwrap(),
            SequenceWatermark::Empty
        );
        for value in BOUNDARY_VALUES {
            let state = SequenceWatermark::Known(value);
            assert_eq!(
                decode_sequence_watermark(&encode_sequence_watermark(state)).unwrap(),
                state
            );
        }
    }

    #[test]
    fn committed_offset_round_trips_every_state_and_boundary() {
        let empty = encode_committed_offset(CommittedOffset::Empty);
        assert_eq!(
            decode_committed_offset(&empty).unwrap(),
            CommittedOffset::Empty
        );
        for value in BOUNDARY_VALUES {
            let state = CommittedOffset::Known(LogOffset::new(value));
            assert_eq!(
                decode_committed_offset(&encode_committed_offset(state)).unwrap(),
                state
            );
        }
    }

    #[test]
    fn decoders_reject_wrong_width_invalid_tags_and_noncanonical_payloads() {
        assert_data_corruption(decode_u64(&[0_u8; U64_ENCODED_LEN - 1]).unwrap_err());
        assert_data_corruption(decode_u64(&[0_u8; U64_ENCODED_LEN + 1]).unwrap_err());

        for decode in [
            decode_optional_u64_as_unit as fn(&[u8]) -> zippy_types::Result<()>,
            decode_offset_watermark_as_unit,
            decode_sequence_watermark_as_unit,
            decode_committed_offset_as_unit,
        ] {
            assert_data_corruption(decode(&[0_u8; TAGGED_U64_ENCODED_LEN - 1]).unwrap_err());
            assert_data_corruption(decode(&[0_u8; TAGGED_U64_ENCODED_LEN + 1]).unwrap_err());

            let mut invalid_tag = [0_u8; TAGGED_U64_ENCODED_LEN];
            invalid_tag[0] = u8::MAX;
            assert_data_corruption(decode(&invalid_tag).unwrap_err());

            let mut noncanonical_empty = [0_u8; TAGGED_U64_ENCODED_LEN];
            noncanonical_empty[TAGGED_U64_ENCODED_LEN - 1] = 1;
            assert_data_corruption(decode(&noncanonical_empty).unwrap_err());
        }

        let mut noncanonical_legacy_unknown = [0_u8; TAGGED_U64_ENCODED_LEN];
        noncanonical_legacy_unknown[0] = 2;
        assert_data_corruption(decode_offset_watermark(&noncanonical_legacy_unknown).unwrap_err());
        noncanonical_legacy_unknown[TAGGED_U64_ENCODED_LEN - 1] = 1;
        assert_data_corruption(decode_offset_watermark(&noncanonical_legacy_unknown).unwrap_err());
    }

    fn decode_optional_u64_as_unit(encoded: &[u8]) -> zippy_types::Result<()> {
        decode_optional_u64(encoded).map(|_| ())
    }

    fn decode_offset_watermark_as_unit(encoded: &[u8]) -> zippy_types::Result<()> {
        decode_offset_watermark(encoded).map(|_| ())
    }

    fn decode_sequence_watermark_as_unit(encoded: &[u8]) -> zippy_types::Result<()> {
        decode_sequence_watermark(encoded).map(|_| ())
    }

    fn decode_committed_offset_as_unit(encoded: &[u8]) -> zippy_types::Result<()> {
        decode_committed_offset(encoded).map(|_| ())
    }

    fn assert_data_corruption(error: zippy_types::ServerError) {
        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }
}
