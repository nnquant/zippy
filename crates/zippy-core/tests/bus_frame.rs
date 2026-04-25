use zippy_core::{
    encode_bus_frame, encode_bus_frame_with_timing, parse_bus_frame, BusFrameKind, BusFrameTiming,
    ZippyError, BUS_FRAME_MAGIC, BUS_FRAME_VERSION,
};

#[test]
fn enveloped_frame_roundtrip_directory_and_payload() {
    let instrument_ids = vec!["ES", "NQ"];
    let arrow_payload = b"arrow-payload-bytes";

    let encoded = encode_bus_frame(&instrument_ids, arrow_payload).expect("encode should succeed");
    let parsed = parse_bus_frame(&encoded).expect("parse should succeed");

    match parsed.kind {
        BusFrameKind::EnvelopedWithDirectory {
            instrument_ids: parsed_ids,
        } => {
            assert_eq!(parsed_ids, vec!["ES", "NQ"]);
        }
        other => panic!("expected enveloped frame, got {:?}", other),
    }
    assert_eq!(parsed.arrow_payload, arrow_payload);
    assert_eq!(parsed.timing, None);
}

#[test]
fn enveloped_frame_roundtrip_timing_metadata_and_payload() {
    let instrument_ids = vec!["ES", "NQ"];
    let arrow_payload = b"arrow-payload-bytes";
    let timing = BusFrameTiming {
        target_publish_enter_ns: 95,
        writer_enter_ns: 100,
        arrow_encoded_ns: 110,
        instrument_ids_done_ns: 120,
        publish_start_ns: 123,
        publish_done_ns: 456,
    };

    let encoded =
        encode_bus_frame_with_timing(&instrument_ids, arrow_payload, Some(timing.clone()))
            .expect("encode should succeed");
    let parsed = parse_bus_frame(&encoded).expect("parse should succeed");

    match parsed.kind {
        BusFrameKind::EnvelopedWithDirectory {
            instrument_ids: parsed_ids,
        } => {
            assert_eq!(parsed_ids, vec!["ES", "NQ"]);
        }
        other => panic!("expected enveloped frame, got {:?}", other),
    }
    assert_eq!(parsed.timing, Some(timing));
    assert_eq!(parsed.arrow_payload, arrow_payload);
}

#[test]
fn non_magic_payload_treated_as_legacy() {
    let payload = b"plain-arrow-ipc-payload";

    let parsed = parse_bus_frame(payload).expect("parse should succeed");

    assert!(matches!(parsed.kind, BusFrameKind::Legacy));
    assert_eq!(parsed.arrow_payload, payload);
}

#[test]
fn enveloped_frame_without_directory_is_distinct() {
    let arrow_payload = b"payload-without-directory";

    let encoded = encode_bus_frame(&[] as &[&str], arrow_payload).expect("encode should succeed");
    let parsed = parse_bus_frame(&encoded).expect("parse should succeed");

    assert!(matches!(
        parsed.kind,
        BusFrameKind::EnvelopedWithoutDirectory
    ));
    assert_eq!(parsed.arrow_payload, arrow_payload);
    assert_eq!(parsed.timing, None);
}

#[test]
fn unsupported_version_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BUS_FRAME_MAGIC);
    bytes.extend_from_slice(&(BUS_FRAME_VERSION + 1).to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());

    let err = parse_bus_frame(&bytes).expect_err("unsupported version should fail");

    match err {
        ZippyError::Io { reason } => {
            assert!(
                reason.contains("unsupported bus frame version"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("expected io error, got {:?}", other),
    }
}

#[test]
fn unknown_flags_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BUS_FRAME_MAGIC);
    bytes.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0x8000u16.to_le_bytes());

    let err = parse_bus_frame(&bytes).expect_err("unknown flags should fail");

    match err {
        ZippyError::Io { reason } => {
            assert!(
                reason.contains("unsupported bus frame flags"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("expected io error, got {:?}", other),
    }
}

#[test]
fn directory_flag_missing_directory_count_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BUS_FRAME_MAGIC);
    bytes.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0x0001u16.to_le_bytes());

    let err = parse_bus_frame(&bytes).expect_err("missing directory metadata should fail");

    match err {
        ZippyError::Io { reason } => {
            assert!(
                reason.contains("truncated bus frame header"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("expected io error, got {:?}", other),
    }
}

#[test]
fn truncated_directory_entry_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BUS_FRAME_MAGIC);
    bytes.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0x0001u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&4u16.to_le_bytes());
    bytes.extend_from_slice(b"ES");

    let err = parse_bus_frame(&bytes).expect_err("truncated directory entry should fail");

    match err {
        ZippyError::Io { reason } => {
            assert!(
                reason.contains("truncated instrument directory"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("expected io error, got {:?}", other),
    }
}

#[test]
fn invalid_utf8_instrument_id_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BUS_FRAME_MAGIC);
    bytes.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0x0001u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.push(0xff);

    let err = parse_bus_frame(&bytes).expect_err("invalid utf-8 should fail");

    match err {
        ZippyError::Io { reason } => {
            assert!(
                reason.contains("not valid utf-8"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("expected io error, got {:?}", other),
    }
}

#[test]
fn truncated_header_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BUS_FRAME_MAGIC);
    bytes.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());

    let err = parse_bus_frame(&bytes).expect_err("truncated header should fail");

    match err {
        ZippyError::Io { reason } => {
            assert!(
                reason.contains("truncated"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("expected io error, got {:?}", other),
    }
}

#[test]
fn truncated_timing_metadata_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BUS_FRAME_MAGIC);
    bytes.extend_from_slice(&BUS_FRAME_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0x0002u16.to_le_bytes());
    bytes.extend_from_slice(&123i64.to_le_bytes());

    let err = parse_bus_frame(&bytes).expect_err("truncated timing metadata should fail");

    match err {
        ZippyError::Io { reason } => {
            assert!(
                reason.contains("truncated bus frame timing metadata"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("expected io error, got {:?}", other),
    }
}
