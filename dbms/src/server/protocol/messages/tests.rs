use tokio_test::io::Builder as TestIoBuilder;

use crate::serialisation::Serialiser;

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
fn close_body_to_message() {
    let message = MessageBody::Close;

    let message = Message::from_message_body(message);

    assert!(matches!(
        message,
        Message {
            header: Header { message_type: MessageType::Close, .. },
            body: MessageBody::Close,
        }
    ));
}

#[test]
fn serialise_close_message() {
    let message = Message::from_message_body(MessageBody::Close);

    let serialised = message.serialise(SerialisationManager(Serialiser::V2));

    let expected = RawHeader::new(
        1,
        vec![1]
    ).serialise();

    assert_eq!(
        serialised,
        expected
    )
}

#[test]
fn deserialise_close_message() {
    let serialised = vec![
        // Flags
        1, 0, 0, 0, 0, 0, 0, 0,
        // Message type
        1,
    ];

    let message = Message::deserialise(
        &mut serialised.as_slice(),
        SerialisationManager(Serialiser::V2)
    ).unwrap();

    assert_eq!(
        message.header,
        Header { message_type: MessageType::Close },
    );

    assert!(matches!(
        message.body,
        MessageBody::Close,
    ));
}

#[test]
fn ok_body_to_message() {
    let message = MessageBody::Ok;

    let message = Message::from_message_body(message);

    assert!(matches!(
        message,
        Message {
            header: Header { message_type: MessageType::Ok, .. },
            body: MessageBody::Ok,
        }
    ));
}

#[test]
fn serialise_ok_message() {
    let message = Message::from_message_body(MessageBody::Ok);

    let serialised = message.serialise(SerialisationManager(Serialiser::V2));

    let expected = RawHeader::new(
        1,
        vec![2]
    ).serialise();

    assert_eq!(
        serialised,
        expected
    )
}

#[test]
fn deserialise_ok_message() {
    let serialised = vec![
        // Flags
        1, 0, 0, 0, 0, 0, 0, 0,
        // Message type
        2,
    ];

    let message = Message::deserialise(
        &mut serialised.as_slice(),
        SerialisationManager(Serialiser::V2)
    ).unwrap();

    assert_eq!(
        message.header,
        Header { message_type: MessageType::Ok },
    );

    assert!(matches!(
        message.body,
        MessageBody::Ok,
    ));
}

#[test]
fn string_body_to_message() {
    let message = MessageBody::Str("deez nuts".into());

    let message = Message::from_message_body(message);

    assert!(matches!(
        message,
        Message {
            header: Header { message_type: MessageType::Str, .. },
            body: MessageBody::Str(_),
        }
    ));
}

#[test]
fn serialise_string_message() {
    let message = Message::from_message_body(
        MessageBody::Str("deez nuts".into())
    );

    let serialised = message.serialise(SerialisationManager(Serialiser::V2));

    let mut expected = vec![
        // Header
        1, 0, 0, 0, 0, 0, 0, 0,
        3,
        // String length
        9, 0, 0, 0, 0, 0, 0, 0,
    ];

    expected.extend("deez nuts".as_bytes());

    assert_eq!(
        serialised,
        expected
    );
}

#[test]
fn deserialise_string_message() {
    let mut input = vec![
        // Header
        1, 0, 0, 0, 0, 0, 0, 0,
        3,
        // String length
        9, 0, 0, 0, 0, 0, 0, 0,
    ];

    input.extend("deez nuts".as_bytes());

    let message = Message::deserialise(&mut input.as_slice(), SerialisationManager(Serialiser::V2)).unwrap();

    assert_eq!(
        message.header,
        Header { message_type: MessageType::Str }
    );

    if let MessageBody::Str(parsed_string) = message.body {
        assert_eq!(
            parsed_string,
            "deez nuts",
        );
    } else {
        panic!("Body wrong type");
    }
}

#[test]
fn error_body_to_message() {
    let message = MessageBody::Error(SqlError::InvalidParameter);

    let message = Message::from_message_body(message);

    assert!(matches!(
        message,
        Message {
            header: Header {
                message_type: MessageType::Error,
                ..
            },
            body: MessageBody::Error(_)
        }
    ));
}

// Not testing this for now, because it's same implementation as String
// #[test]
// fn serialise_error_message() {
// }

#[test]
fn command_body_to_message() {
    let commands = [
        Command::Connect("sweden".into()),
        Command::ListDatabases,
        Command::ListTables,
    ];

    commands.into_iter().for_each(|command| {
        let message = Message::from_message_body(MessageBody::Command(command));

        assert_eq!(
            message.header,
            Header { message_type: MessageType::Command }
        );

        assert!(matches!(
            message,
            Message {
                header: Header { message_type: MessageType::Command },
                body: MessageBody::Command(_),
            }
        ));
    });
}

#[test]
fn serialise_command_message() {
    let inputs = [
        (
            Command::Connect("sweden".into()),
            vec![
                // Command type
                1,
                // String length
                6, 0, 0, 0, 0, 0, 0, 0,
                // "sweden"
                115, 119, 101, 100, 101, 110,
            ]
        ),
        (Command::ListDatabases, vec![2]),
        (Command::ListTables, vec![3]),
    ];

    inputs.into_iter().for_each(|(input, extra)| {
        let message = Message::from_message_body(MessageBody::Command(input));

        let serialised = message.serialise(SerialisationManager(Serialiser::V2));

        let mut expected = vec![
            1, 0, 0, 0, 0, 0, 0, 0,
            4,
        ];

        expected.extend(
            extra
        );

        assert_eq!(
            serialised,
            expected,
        );
    });
}

#[test]
fn deserialise_command_message() {
    let inputs = [
        Command::Connect("sweden".into()),
        // Command::ListDatabases,
        // Command::ListTables,
    ];

    inputs.into_iter().for_each(|input| {
        let message = Message::from_message_body(MessageBody::Command(input.clone()));

        let serialised = message.serialise(SerialisationManager(Serialiser::V2));

        let deserialised = Message::deserialise(
            &mut serialised.as_slice(),
            SerialisationManager(Serialiser::V2),
        ).unwrap();

        assert_eq!(
            deserialised.header,
            Header { message_type: MessageType::Command }
        );

        if let MessageBody::Command(command) = deserialised.body {
            assert_eq!(
                command,
                input,
            );
        } else {
            panic!("Body wrong type");
        }
    });
}

#[test]
fn rowset_body_to_message() {
    let message = MessageBody::RowSet(RowSet {
        types: vec![],
        names: vec![],
        values: vec![],
    });

    let message = Message::from_message_body(message);

    assert!(matches!(
        message,
        Message {
            header: Header {
                message_type: MessageType::RowSet,
            },
            body: MessageBody::RowSet(_)
        }
    ));
}

#[test]
fn serialise_rowset_message() {
    let message = Message::from_message_body(
        MessageBody::RowSet(RowSet {
            types: vec![],
            names: vec![],
            values: vec![],
        })
    );

    let serialised = message.serialise(SerialisationManager(Serialiser::V2));

    let mut expected = vec![
        // Header
        1, 0, 0, 0, 0, 0, 0, 0,
        6,
    ];

    expected.extend(
        SerialisationManager(Serialiser::V2).serialise_rowset(&RowSet {
            types: vec![],
            names: vec![],
            values: vec![],
        })
    );

    assert_eq!(
        serialised,
        expected,
    );
}

#[test]
fn deserialise_rowset_message() {
    let mut input = vec![
        // Header
        1, 0, 0, 0, 0, 0, 0, 0,
        6,
    ];

    input.extend(
        SerialisationManager(Serialiser::V2).serialise_rowset(&RowSet {
            types: vec![],
            names: vec![],
            values: vec![],
        })
    );

    let message = Message::deserialise(&mut input.as_slice(), SerialisationManager(Serialiser::V2)).unwrap();

    assert_eq!(
        message.header,
        Header { message_type: MessageType::RowSet }
    );

    if let MessageBody::RowSet(rowset) = message.body {
        assert_eq!(
            rowset,
            RowSet { types: vec![], names: vec![], values: vec![] }
        );
    } else {
        panic!("Body wrong type");
    }
}

#[tokio::test]
async fn read_message() {
    let message = Message::from_message_body(MessageBody::Str("deez nuts".into()));

    let manager = SerialisationManager(Serialiser::V2);

    let serialised = message.serialise(manager);

    let mut stream = TestIoBuilder::new()
        .read(&(serialised.len() as u64).to_le_bytes())
        .read(&serialised)
        .build();

    Message::read(&mut stream, manager).await.unwrap();
}

#[tokio::test]
async fn write_message() {
    let message = Message::from_message_body(MessageBody::Command(
        Command::Connect("sweden".into())
    ));

    let manager = SerialisationManager(Serialiser::V2);

    let serialised = message.serialise(manager);

    let mut stream = TestIoBuilder::new()
        .write(&(serialised.len() as u64).to_le_bytes())
        .write(&serialised)
        .build();

    message.write(&mut stream, manager).await.unwrap();
}
