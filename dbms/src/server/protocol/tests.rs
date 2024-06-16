use super::messages::*;
use super::header::*;
use crate::{serialisation::{SerialisationManager, Serialiser}, SqlError};

const SERIALISATION_MANAGER: SerialisationManager = SerialisationManager(Serialiser::V2);

mod messages {
    use crate::database::RowSet;

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
        let message = MessageBody::Close;

        let packet = Message::from_message_body(message, &SERIALISATION_MANAGER);

        assert!(matches!(
            packet,
            Message {
                header: Header { message_type: MessageType::Close, .. },
                body: MessageBody::Close,
            }
        ));
    }

    #[test]
    fn ok_message_to_packet() {
        let message = MessageBody::Ok;

        let packet = Message::from_message_body(message, &SERIALISATION_MANAGER);

        assert!(matches!(
            packet,
            Message {
                header: Header { message_type: MessageType::Ok, .. },
                body: MessageBody::Ok,
            }
        ));
    }

    #[test]
    fn string_message_to_packet() {
        let message = MessageBody::Str("deez nuts".into());

        let packet = Message::from_message_body(message, &SERIALISATION_MANAGER);

        assert!(matches!(
            packet,
            Message {
                header: Header { message_type: MessageType::Str, .. },
                body: MessageBody::Str(_),
            }
        ));
    }

    #[test]
    fn error_message_to_packet() {
        let message = MessageBody::Error(SqlError::InvalidParameter);

        let packet = Message::from_message_body(message, &SERIALISATION_MANAGER);

        assert!(matches!(
            packet,
            Message {
                header: Header {
                    message_type: MessageType::Error,
                    ..
                },
                body: MessageBody::Error(_)
            }
        ));
    }

    #[test]
    fn rowset_message_to_packet() {
        let message = MessageBody::RowSet(RowSet {
            types: vec![],
            names: vec![],
            values: vec![],
        });

        let packet = Message::from_message_body(message, &SERIALISATION_MANAGER);

        assert!(matches!(
            packet,
            Message {
                header: Header {
                    message_type: MessageType::RowSet,
                    ..
                },
                body: MessageBody::RowSet(_)
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
        let mut header = RawHeader::default();

        header.set_flag(0);

        assert_eq!(
            header.flags,
            u64::from_be_bytes([128, 0, 0, 0, 0, 0, 0, 0])
        );

        header.set_flag(10);

        assert_eq!(
            header.flags,
            u64::from_be_bytes([128, 32, 0, 0, 0, 0, 0, 0])
        );

        assert!(header.get_flag(10));

        assert!(!header.get_flag(12));
    }

    #[test]
    fn parse_message_type_basic() {
        use MessageType::*;

        fn test_header(content: u64) -> RawHeader {
            let mut result = RawHeader::new(
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
        let header = RawHeader::default();

        let parsed = Header::try_from(header);

        assert!(matches!(
            parsed,
            Err(SqlError::InvalidHeader("Header must contain message type"))
        ));
    }

    #[test]
    fn parse_serialisation_version_basic() {
        let mut header = RawHeader::new(
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

        let raw = header.to_raw();

        assert_eq!(
            raw.flags,
            u64::from_be_bytes([128, 0, 0, 0, 0, 0, 0, 0])
        );

        assert_eq!(
            raw.content,
            vec![2]
        );
    }

    #[test]
    fn set_serialisation_version_basic() {
        let header = Header {
            message_type: MessageType::Close,
            serialisation_version: Some(Serialiser::V2)
        };

        let raw = header.to_raw();

        assert_eq!(
            raw.flags,
            u64::from_be_bytes([192, 0, 0, 0, 0, 0, 0, 0])
        );

        assert_eq!(
            raw.content,
            vec![1, 2]
        );
    }

    // TODO: This when more fields exist
    // But for now because message type is required, parse_serialisation_version_basic actual is a full header
    // #[test]
    // fn serialise_full_header() {
    // }
}
