use std::convert::TryFrom;
use std::str;

use crate::{Result, ZippyError};

/// Magic prefix for the bus frame envelope.
pub const BUS_FRAME_MAGIC: [u8; 8] = *b"ZIPPYBF1";
/// Version of the bus frame envelope header.
pub const BUS_FRAME_VERSION: u16 = 1;
/// Flag indicating the frame carries an instrument directory.
pub const FLAG_HAS_INSTRUMENT_DIRECTORY: u16 = 0x0001;

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
    /// Arrow payload bytes.
    pub arrow_payload: &'a [u8],
}

/// Encode a bus frame envelope.
///
/// :param instrument_ids: Instrument identifiers to place in the directory.
/// :type instrument_ids: &[String]
/// :param arrow_payload: Raw Arrow IPC payload.
/// :type arrow_payload: &[u8]
/// :returns: Encoded bus frame bytes.
/// :rtype: crate::Result<Vec<u8>>
/// :raises crate::ZippyError::Io: If the directory is too large to encode.
pub fn encode_bus_frame<S>(instrument_ids: &[S], arrow_payload: &[u8]) -> Result<Vec<u8>>
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
    let mut encoded = Vec::with_capacity(
        BUS_FRAME_MAGIC.len()
            + 2
            + 2
            + if has_directory { 2 } else { 0 }
            + directory_bytes
            + arrow_payload.len(),
    );
    encoded.extend_from_slice(&BUS_FRAME_MAGIC);
    encoded.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());

    if has_directory {
        encoded.extend_from_slice(&FLAG_HAS_INSTRUMENT_DIRECTORY.to_le_bytes());
        encoded.extend_from_slice(&directory_count.to_le_bytes());

        for instrument_id in instrument_ids {
            let instrument_bytes = instrument_id.as_ref().as_bytes();
            let instrument_len = u16::try_from(instrument_bytes.len())
                .map_err(|_| invalid_bus_frame("instrument id too long"))?;
            encoded.extend_from_slice(&instrument_len.to_le_bytes());
            encoded.extend_from_slice(instrument_bytes);
        }
    } else {
        encoded.extend_from_slice(&0u16.to_le_bytes());
    }

    encoded.extend_from_slice(arrow_payload);
    Ok(encoded)
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
            arrow_payload: bytes,
        });
    }

    let mut cursor = BUS_FRAME_MAGIC.len();
    let version = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
    if version != BUS_FRAME_VERSION {
        return Err(invalid_bus_frame("unsupported bus frame version"));
    }

    let flags = read_u16(bytes, &mut cursor, "truncated bus frame header")?;
    let unsupported_flags = flags & !FLAG_HAS_INSTRUMENT_DIRECTORY;
    if unsupported_flags != 0 {
        return Err(invalid_bus_frame("unsupported bus frame flags"));
    }

    if flags & FLAG_HAS_INSTRUMENT_DIRECTORY != 0 {
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
        return Ok(ParsedBusFrame {
            kind: BusFrameKind::EnvelopedWithDirectory { instrument_ids },
            arrow_payload: &bytes[cursor..],
        });
    }

    if cursor == bytes.len() {
        return Ok(ParsedBusFrame {
            kind: BusFrameKind::EnvelopedWithoutDirectory,
            arrow_payload: &[],
        });
    }

    Ok(ParsedBusFrame {
        kind: BusFrameKind::EnvelopedWithoutDirectory,
        arrow_payload: &bytes[cursor..],
    })
}

fn read_u16(bytes: &[u8], cursor: &mut usize, reason: &str) -> Result<u16> {
    let raw = read_bytes(bytes, cursor, 2, reason)?;
    let mut value = [0u8; 2];
    value.copy_from_slice(raw);
    Ok(u16::from_le_bytes(value))
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
