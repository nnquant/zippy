use zippy_core::{
    encode_bus_frame, parse_bus_frame, BusFrameKind, ZippyError, BUS_FRAME_MAGIC,
    BUS_FRAME_VERSION,
};

#[test]
fn enveloped_frame_roundtrip_directory_and_payload() {
    let instrument_ids = vec!["ES".to_string(), "NQ".to_string()];
    let arrow_payload = b"arrow-payload-bytes";

    let encoded = encode_bus_frame(&instrument_ids, arrow_payload).expect("encode should succeed");
    let parsed = parse_bus_frame(&encoded).expect("parse should succeed");

    match parsed.kind {
        BusFrameKind::Enveloped {
            instrument_ids: parsed_ids,
        } => {
            assert_eq!(parsed_ids, vec!["ES", "NQ"]);
        }
        other => panic!("expected enveloped frame, got {:?}", other),
    }
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
