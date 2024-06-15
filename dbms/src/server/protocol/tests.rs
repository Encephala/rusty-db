use super::header::*;
use crate::{serialisation::Serialiser, SqlError};

mod messages {
    use super::*;


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
            (2, Ack),
            (3, String),
            (4, Command),
            (5, ErrorMessage),
            (6, RowSet),
        ];

        inputs.into_iter().for_each(|(message_type, expected)| {
            let header = test_header(message_type);

            let parsed = Header::try_from(header).unwrap();

            assert_eq!(
                parsed.message_type,
                Some(expected),
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
            message_type: Some(MessageType::Ack),
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
            message_type: Some(MessageType::Close),
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
