use super::*;

// Quick macro to help update the tests when I add enum variants
// https://users.rust-lang.org/t/ensure-exhaustiveness-of-list-of-enum-variants/99891
macro_rules! ensure_exhaustive {
    ($type:path, $($variant:ident),* $(,)?) => {
        let _ = |dummy: $type| {
            match dummy {
                $(<$type>::$variant => ()),*
            }
        };
        [$(<$type>::$variant),*]
    }
}

// Serialisation
#[test]
fn convert_u8_to_message_type() {
    assert_eq!(
        MessageType::try_from(1).unwrap(),
        MessageType::Close,
    );

    assert_eq!(
        MessageType::try_from(6).unwrap(),
        MessageType::RowSet,
    );

    // Putting this at 7, so that when a new type is added,
    // the test should give an indication of what code I'm forgetting to update
    assert!(matches!(
        MessageType::try_from(7),
        Err(SqlError::InvalidMessageType(7))
    ));
}

#[test]
fn set_clear_and_get_header_flags() {
    let mut header = RawHeader::default();

    header.set_flag(0);

    assert_eq!(
        header.flags,
        1,
    );

    header.set_flag(10);

    assert_eq!(
        header.flags,
        u64::from_be_bytes([0, 0, 0, 0, 0, 0, 4, 1])
    );

    assert!(header.get_flag(10));

    assert!(!header.get_flag(12));
}

#[test]
fn parse_message_type_basic() {
    use MessageType::*;

    fn test_header(content: u8) -> RawHeader {
        let mut result = RawHeader::new(
            1,
            vec![]
        );

        result.content = content.to_le_bytes().into();

        return result;
    }

    let inputs = [
        (1_u8, Close),
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

// TODO: This when more fields exist
// #[test]
// fn parse_full_header() {
// }

// Deserialisation
#[test]
fn set_message_type_basic() {
    let header = Header {
        message_type: MessageType::Ok,
    };

    let raw = header.to_raw();

    assert_eq!(
        raw.flags,
        1,
    );

    assert_eq!(
        raw.content,
        vec![2]
    );
}

// TODO: This when more fields exist
// #[test]
// fn serialise_full_header() {
// }

// Is there a way to have the compiler ensure we test each variant?
// Nightly's https://doc.rust-lang.org/std/mem/fn.variant_count.html is a way,
// but that's nightly.
// https://users.rust-lang.org/t/ensure-exhaustiveness-of-list-of-enum-variants/99891/4
// Doesn't look like it, I don't want to use a macro
#[test]
fn seralise_headers() {
    ensure_exhaustive!(
        MessageType,
        Close,
        Ok,
        Str,
        Command,
        Error,
        RowSet,
    );

    let inputs = [
        MessageType::Close,
        MessageType::Ok,
        MessageType::Str,
        MessageType::Command,
        MessageType::Error,
        MessageType::RowSet,
    ];

    for (i, input) in inputs.into_iter().enumerate() {
        let header = Header {
            message_type: input,
        };

        let serialised = header.to_raw().serialise();

        assert_eq!(
            serialised,
            vec![
                // Flags
                1, 0, 0, 0, 0, 0, 0, 0,
                // Message type
                (i + 1) as u8,
            ]
        );
    };
}
