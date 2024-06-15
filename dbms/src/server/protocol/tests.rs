use super::messages::*;
use super::header::*;
use crate::{serialisation::Serialiser, SqlError};

mod messages {
    use sql_parse::parser::ColumnType;

    use crate::database::{Row, RowSet};

    use super::*;

    #[test]
    fn serialise_command() {
        let inputs = [
            (
                Command::Connect("sweden".into()),
                vec![
                    1,
                    6, 0, 0, 0, 0, 0, 0, 0,
                    115, 119, 101, 100, 101, 110
                ]
            ),
            (
                Command::ListDatabases,
                vec![2]
            ),
            (
                Command::ListTables,
                vec![3]
            ),
        ];

        inputs.into_iter().for_each(|(input, expected)| {
            let result: Vec<u8> = input.into();

            assert_eq!(
                result,
                expected
            );
        })
    }

    #[test]
    fn close_message_to_packet() {
        let message = Message::Close;

        let packet = Packet::from(message);

        assert!(matches!(
            packet,
            Packet {
                header: Header { message_type: MessageType::Close, .. },
                ..
            }
        ));

        assert_eq!(
            packet.body,
            vec![]
        );
    }

    #[test]
    fn ok_message_to_packet() {
        let message = Message::Ok;

        let packet = Packet::from(message);

        assert!(matches!(
            packet,
            Packet {
                header: Header { message_type: MessageType::Ok, .. },
                ..
            }
        ));

        assert_eq!(
            packet.body,
            vec![]
        );
    }

    #[test]
    fn string_message_to_packet() {
        let message = Message::Str("deez nuts".into());

        let packet = Packet::from(message);

        assert_eq!(
            packet,
            Packet {
                header: Header {
                    message_type: MessageType::Str,
                    serialisation_version: None,
                },
                body: vec![
                    9, 0, 0, 0, 0, 0, 0, 0,
                    100, 101, 101, 122, 32, 110, 117, 116, 115
                ]
            }
        );
    }

    #[test]
    fn error_message_to_packet() {
        let message = Message::Error(SqlError::InvalidParameter);

        let packet = Packet::from(message);

        assert_eq!(
            packet,
            Packet {
                header: Header {
                    message_type: MessageType::Error,
                    serialisation_version: None,
                },
                // "Error(InvalidParameter)"
                body: vec![
                    23, 0, 0, 0, 0, 0, 0, 0,
                    69, 82, 82, 79, 82, 58, 32, 73, 110, 118, 97, 108, 105, 100, 80, 97, 114, 97, 109, 101, 116, 101, 114
                ]
            }
        );
    }

    #[test]
    fn rowset_message_to_packet() {
        let message = Message::RowSet(RowSet {
            types: vec![ColumnType::Text, ColumnType::Bool],
            names: vec!["a".into(), "b".into()],
            values: vec![
                Row(vec!["first".into(), true.into()]),
                Row(vec!["second".into(), false.into()]),
            ]
        });

        let packet = Packet::from(message);

        assert!(matches!(
            packet,
            Packet {
                header: Header {
                    message_type: MessageType::RowSet,
                    ..
                },
                ..
            }
        ));

        assert_eq!(
            packet.header.serialisation_version,
            Some(SERIALISATION_MANAGER.0)
        );
    }
}

mod headers {
    use super::*;

    // Serialisation
    #[test]
    fn set_clear_and_get_header_flags() {
        let mut header = SerialisedHeader::default();

        header.set_flag(0);

        assert_eq!(
            header.flags(),
            u64::from_be_bytes([128, 0, 0, 0, 0, 0, 0, 0])
        );

        header.set_flag(10);

        assert_eq!(
            header.flags(),
            u64::from_be_bytes([128, 32, 0, 0, 0, 0, 0, 0])
        );

        assert!(header.get_flag(10));

        assert!(!header.get_flag(12));
    }

    #[test]
    fn parse_message_type_basic() {
        use MessageType::*;

        fn test_header(content: u64) -> SerialisedHeader {
            let mut result = SerialisedHeader::new(
                u64::from_be_bytes([128, 0, 0, 0, 0, 0, 0, 0]),
                vec![]
            );

            result.content = content.to_le_bytes().into();

            return result;
        }

        let inputs = [
            (1_u64, Close),
            (2, Ok),
            (3, Str),
            (4, Command),
            (5, Error),
            (6, RowSet),
        ];

        inputs.into_iter().for_each(|(message_type, expected)| {
            let header = test_header(message_type);

            let parsed = Header::try_from(header).unwrap();

            assert_eq!(
                parsed.message_type,
                expected,
            );
        });
    }

    #[test]
    fn parse_message_type_error_if_absent() {
        let header = SerialisedHeader::default();

        let parsed = Header::try_from(header);

        assert!(matches!(
            parsed,
            Err(SqlError::InvalidHeader("Header must contain message type"))
        ));
    }

    #[test]
    fn parse_serialisation_version_basic() {
        let mut header = SerialisedHeader::new(
            u64::from_be_bytes([192, 0, 0, 0, 0, 0, 0, 0]),
            vec![]
        );

        header.content = [1, 2].into();

        let parsed = Header::try_from(header).unwrap();

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

    // Deserialisation
    #[test]
    fn set_message_type_basic() {
        let header = Header {
            message_type: MessageType::Ok,
            serialisation_version: None,
        };

        let serialised: SerialisedHeader = header.into();

        assert_eq!(
            serialised.flags(),
            u64::from_be_bytes([128, 0, 0, 0, 0, 0, 0, 0])
        );

        assert_eq!(
            serialised.content,
            vec![1, 0, 0, 0, 0, 0, 0, 0, 2]
        );
    }

    #[test]
    fn set_serialisation_version_basic() {
        let header = Header {
            message_type: MessageType::Close,
            serialisation_version: Some(Serialiser::V2)
        };

        let serialised = header.serialise();

        assert_eq!(
            serialised.flags(),
            u64::from_be_bytes([192, 0, 0, 0, 0, 0, 0, 0])
        );

        assert_eq!(
            serialised.content,
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 2]
        );
    }

    // TODO: This when more fields exist
    // But for now because message type is required, parse_serialisation_version_basic actual is a full header
    // #[test]
    // fn serialise_full_header() {
    // }
}
