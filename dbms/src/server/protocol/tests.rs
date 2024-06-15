use super::header::*;
use crate::{serialisation::Serialiser, SqlError};

#[test]
fn parse_message_type_basic() {
    use MessageType::*;

    fn test_header(content: u64) -> Header {
        return Header {
            flags: u64::from_be_bytes([128, 0, 0, 0, 0, 0, 0, 0]),
            content: content.to_le_bytes().into(),
        };
    }

    let inputs = [
        (1_u64, Close),
        (2, Ack),
        (3, String),
        (4, Command),
        (5, Error),
        (6, RowSet),
    ];

    inputs.into_iter().for_each(|(message_type, expected)| {
        let header = test_header(message_type);

        let parsed = ParsedHeader::try_from(header).unwrap();

        assert_eq!(
            parsed.message_type,
            Some(expected),
        );
    });
}

#[test]
fn parse_message_type_error_if_absent() {
    let header = Header {
        flags: 0,
        content: vec![].into(),
    };

    let parsed = ParsedHeader::try_from(header);

    assert!(matches!(
        parsed,
        Err(SqlError::InvalidHeader("Header must contain message type"))
    ));
}

#[test]
fn parse_serialisation_version_basic() {
    let header = Header {
        flags: u64::from_be_bytes([192, 0, 0, 0, 0, 0, 0, 0]),
        content: [1_u64.to_le_bytes(), 2_u64.to_le_bytes()].into_iter()
            .flatten()
            .collect(),
    };

    let parsed = ParsedHeader::try_from(header).unwrap();

    assert_eq!(
        parsed.serialisation_version,
        Some(Serialiser::V2),
    );
}

// TODO: This when more fields exist
// But for now because message type is required, parse_serialisation_version_basic actual is a full header
// #[test]
// fn parse_full_header() {
// }
