use std::convert::TryFrom;
use std::str;

use crate::{Result, ZippyError};

/// Magic prefix for the bus frame envelope.
pub const BUS_FRAME_MAGIC: [u8; 8] = *b"ZIPPYBF1";
/// Version of the bus frame envelope header.
pub const BUS_FRAME_VERSION: u16 = 1;
/// Flag indicating the frame carries an instrument directory.
pub const FLAG_HAS_INSTRUMENT_DIRECTORY: u16 = 0x0001;
/// Flag indicating the frame carries publish timing metadata.
pub const FLAG_HAS_TIMING_METADATA: u16 = 0x0002;

/// Publish timing metadata embedded in the bus frame envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusFrameTiming {
    pub target_publish_enter_ns: i64,
    pub writer_enter_ns: i64,
    pub arrow_encoded_ns: i64,
    pub instrument_ids_done_ns: i64,
    pub publish_start_ns: i64,
    pub publish_done_ns: i64,
}

/// Parsed bus frame classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusFrameKind<'a> {
    /// Legacy frame without envelope metadata.
    Legacy,
    /// Enveloped frame without an instrument directory.
    EnvelopedWithoutDirectory,
    /// Enveloped frame with an instrument directory.
    EnvelopedWithDirectory { instrument_ids: Vec<&'a str> },
}

/// Parsed bus frame envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedBusFrame<'a> {
    /// Frame kind.
    pub kind: BusFrameKind<'a>,
    /// Optional publish timing metadata.
    pub timing: Option<BusFrameTiming>,
    /// Arrow payload bytes.
    pub arrow_payload: &'a [u8],
}

/// Encode a bus frame envelope.
///
/// :param instrument_ids: Instrument identifiers to place in the directory.
/// :type instrument_ids: &[S]
/// :type S: AsRef<str>
/// :param arrow_payload: Raw Arrow IPC payload.
/// :type arrow_payload: &[u8]
/// :returns: Encoded bus frame bytes.
/// :rtype: crate::Result<Vec<u8>>
/// :raises crate::ZippyError::Io: If the directory is too large to encode.
pub fn encode_bus_frame<S>(instrument_ids: &[S], arrow_payload: &[u8]) -> Result<Vec<u8>>
where
    S: AsRef<str>,
{
    encode_bus_frame_with_timing(instrument_ids, arrow_payload, None)
}

/// Encode a bus frame envelope with optional publish timing metadata.
pub fn encode_bus_frame_with_timing<S>(
    instrument_ids: &[S],
    arrow_payload: &[u8],
    timing: Option<BusFrameTiming>,
) -> Result<Vec<u8>>
where
    S: AsRef<str>,
{
    let directory_bytes = instrument_ids
        .iter()
        .try_fold(0usize, |acc, instrument_id| {
            let id_len = instrument_id.as_ref().len();
            let len_field = usize::from(u16::MAX);
            if id_len > len_field {
                return Err(invalid_bus_frame("instrument id too long"));
            }

            acc.checked_add(2 + id_len)
                .ok_or_else(|| invalid_bus_frame("bus frame directory size overflow"))
        })?;

    let directory_count = u16::try_from(instrument_ids.len())
        .map_err(|_| invalid_bus_frame("too many instrument ids"))?;

    let has_directory = !instrument_ids.is_empty();
    let has_timing = timing.is_some();
    let mut flags = 0u16;
    if has_directory {
        flags |= FLAG_HAS_INSTRUMENT_DIRECTORY;
    }
    if has_timing {
        flags |= FLAG_HAS_TIMING_METADATA;
    }
    let mut encoded = Vec::with_capacity(
        BUS_FRAME_MAGIC.len()
            + 2
            + 2
            + if has_directory { 2 } else { 0 }
            + directory_bytes
            + if has_timing { 48 } else { 0 }
            + arrow_payload.len(),
    );
    encoded.extend_from_slice(&BUS_FRAME_MAGIC);
    encoded.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());
    encoded.extend_from_slice(&flags.to_le_bytes());

    if has_directory {
        encoded.extend_from_slice(&directory_count.to_le_bytes());

        for instrument_id in instrument_ids {
            let instrument_bytes = instrument_id.as_ref().as_bytes();
            let instrument_len = u16::try_from(instrument_bytes.len())
                .map_err(|_| invalid_bus_frame("instrument id too long"))?;
            encoded.extend_from_slice(&instrument_len.to_le_bytes());
            encoded.extend_from_slice(instrument_bytes);
        }
    }

    if let Some(timing) = timing {
        encoded.extend_from_slice(&timing.target_publish_enter_ns.to_le_bytes());
        encoded.extend_from_slice(&timing.writer_enter_ns.to_le_bytes());
        encoded.extend_from_slice(&timing.arrow_encoded_ns.to_le_bytes());
        encoded.extend_from_slice(&timing.instrument_ids_done_ns.to_le_bytes());
        encoded.extend_from_slice(&timing.publish_start_ns.to_le_bytes());
        encoded.extend_from_slice(&timing.publish_done_ns.to_le_bytes());
    }

    encoded.extend_from_slice(arrow_payload);
    Ok(encoded)
}

/// Patch the publish_done_ns field in-place on an already-encoded timing frame.
pub fn patch_bus_frame_publish_done(bytes: &mut [u8], publish_done_ns: i64) -> Result<()> {
    if bytes.len() < BUS_FRAME_MAGIC.len() || !bytes.starts_with(&BUS_FRAME_MAGIC) {
        return Err(invalid_bus_frame("cannot patch legacy bus frame"));
    }

    let mut cursor = BUS_FRAME_MAGIC.len();
    let version = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
    if version != BUS_FRAME_VERSION {
        return Err(invalid_bus_frame("unsupported bus frame version"));
    }

    let flags = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
    let unsupported_flags = flags & !(FLAG_HAS_INSTRUMENT_DIRECTORY | FLAG_HAS_TIMING_METADATA);
    if unsupported_flags != 0 {
        return Err(invalid_bus_frame("unsupported bus frame flags"));
    }
    if flags & FLAG_HAS_INSTRUMENT_DIRECTORY != 0 {
        let directory_count = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
        for _ in 0..directory_count {
            let id_len = usize::from(read_u16(
                bytes,
                &mut cursor,
                "truncated instrument directory",
            )?);
            let _ = read_bytes(bytes, &mut cursor, id_len, "truncated instrument directory")?;
        }
    }
    if flags & FLAG_HAS_TIMING_METADATA == 0 {
        return Err(invalid_bus_frame(
            "bus frame does not carry timing metadata",
        ));
    }

    let _ = read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?;
    let _ = read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?;
    let _ = read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?;
    let _ = read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?;
    let _ = read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?;
    let publish_done_offset = cursor;
    let _ = read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?;
    bytes[publish_done_offset..publish_done_offset + 8]
        .copy_from_slice(&publish_done_ns.to_le_bytes());
    Ok(())
}

/// Parse a bus frame envelope.
///
/// :param bytes: Raw frame bytes.
/// :type bytes: &[u8]
/// :returns: Parsed frame, or legacy payload if no magic header is present.
/// :rtype: crate::Result<ParsedBusFrame<'_>>
/// :raises crate::ZippyError::Io: If the header is truncated or invalid.
pub fn parse_bus_frame(bytes: &[u8]) -> Result<ParsedBusFrame<'_>> {
    if bytes.len() < BUS_FRAME_MAGIC.len() || !bytes.starts_with(&BUS_FRAME_MAGIC) {
        return Ok(ParsedBusFrame {
            kind: BusFrameKind::Legacy,
            timing: None,
            arrow_payload: bytes,
        });
    }

    let mut cursor = BUS_FRAME_MAGIC.len();
    let version = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
    if version != BUS_FRAME_VERSION {
        return Err(invalid_bus_frame("unsupported bus frame version"));
    }

    let flags = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
    let unsupported_flags = flags & !(FLAG_HAS_INSTRUMENT_DIRECTORY | FLAG_HAS_TIMING_METADATA);
    if unsupported_flags != 0 {
        return Err(invalid_bus_frame("unsupported bus frame flags"));
    }

    let kind = if flags & FLAG_HAS_INSTRUMENT_DIRECTORY != 0 {
        let directory_count = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
        let mut instrument_ids = Vec::with_capacity(usize::from(directory_count));
        for _ in 0..directory_count {
            let id_len = usize::from(read_u16(
                bytes,
                &mut cursor,
                "truncated instrument directory",
            )?);
            let id_bytes =
                read_bytes(bytes, &mut cursor, id_len, "truncated instrument directory")?;
            let id = str::from_utf8(id_bytes)
                .map_err(|_| invalid_bus_frame("instrument id is not valid utf-8"))?;
            instrument_ids.push(id);
        }
        BusFrameKind::EnvelopedWithDirectory { instrument_ids }
    } else {
        BusFrameKind::EnvelopedWithoutDirectory
    };

    let timing = if flags & FLAG_HAS_TIMING_METADATA != 0 {
        Some(BusFrameTiming {
            target_publish_enter_ns: read_i64(
                bytes,
                &mut cursor,
                "truncated bus frame timing metadata",
            )?,
            writer_enter_ns: read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?,
            arrow_encoded_ns: read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?,
            instrument_ids_done_ns: read_i64(
                bytes,
                &mut cursor,
                "truncated bus frame timing metadata",
            )?,
            publish_start_ns: read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?,
            publish_done_ns: read_i64(bytes, &mut cursor, "truncated bus frame timing metadata")?,
        })
    } else {
        None
    };

    Ok(ParsedBusFrame {
        kind,
        timing,
        arrow_payload: &bytes[cursor..],
    })
}

fn read_u16(bytes: &[u8], cursor: &mut usize, reason: &str) -> Result<u16> {
    let raw = read_bytes(bytes, cursor, 2, reason)?;
    let mut value = [0u8; 2];
    value.copy_from_slice(raw);
    Ok(u16::from_le_bytes(value))
}

fn read_i64(bytes: &[u8], cursor: &mut usize, reason: &str) -> Result<i64> {
    let raw = read_bytes(bytes, cursor, 8, reason)?;
    let mut value = [0u8; 8];
    value.copy_from_slice(raw);
    Ok(i64::from_le_bytes(value))
}

fn read_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    reason: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| invalid_bus_frame(reason))?;
    if end > bytes.len() {
        return Err(invalid_bus_frame(reason));
    }

    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn invalid_bus_frame(reason: &str) -> ZippyError {
    ZippyError::Io {
        reason: reason.to_string(),
    }
}
