use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
};

use tokio_test::io::Builder as TestIoBuilder;

use crate::{
    persistence::NoOp,
    serialisation::{SerialisationManager, Serialiser},
    utils::tests::*,
};

use super::*;

impl Stream for tokio_test::io::Mock {}

// Only have one test actually open a listener,
// otherwise we'd have conflicts and stuff
#[tokio::test]
async fn message_read_write() {
    let test_message: Message = Message::from_message_body(MessageBody::Str("deez nuts".into()));

    let listener = TcpListener::bind("localhost:12345").await.unwrap();

    let mut stream = TcpStream::connect("localhost:12345").await.unwrap();

    let (response_stream, _) = listener.accept().await.unwrap();

    test_message
        .write(&mut stream, SerialisationManager(Serialiser::V2))
        .await
        .unwrap();

    let read_message = Message::read(
        &mut BufReader::new(response_stream),
        SerialisationManager(Serialiser::V2),
    )
    .await
    .unwrap();

    if let MessageBody::Str(value) = read_message.body {
        assert_eq!(value, "deez nuts");
    } else {
        panic!("Body wrong type");
    }
}

#[test]
fn create_get_clear_database_basic() {
    let mut runtime = test_runtime_with_values();

    let db = runtime.get_database();

    // Kinda pointless but whatever
    assert_eq!(db.unwrap().name, "test_db".into(),);

    let name = runtime.clear_database().unwrap();

    assert_eq!(name, "test_db".into(),);
}

#[test]
fn clear_database_none_selected() {
    let mut runtime = Runtime::new(NoOp);

    let result = runtime.clear_database();

    assert!(matches!(result, Err(SqlError::NoDatabaseSelected),));
}

#[tokio::test]
async fn negotiate_serialiser_version_basic() {
    let mut client = TestIoBuilder::new().write(&[2, 1, 2]).read(&[1]).build();

    let negotiated_version = Connection::negotiate_serialiser_version(&mut client)
        .await
        .unwrap();

    assert_eq!(negotiated_version, Serialiser::V1);

    let mut client = TestIoBuilder::new().write(&[2, 1, 2]).read(&[2]).build();

    let negotiated_version = Connection::negotiate_serialiser_version(&mut client)
        .await
        .unwrap();

    assert_eq!(negotiated_version, Serialiser::V2);

    let mut client = TestIoBuilder::new().write(&[2, 1, 2]).read(&[3]).build();

    let negotiated_version = Connection::negotiate_serialiser_version(&mut client).await;

    assert!(matches!(
        negotiated_version,
        Err(SqlError::IncompatibleVersion(3)),
    ));
}

#[tokio::test]
async fn setup_context_basic() {
    let mut client = TestIoBuilder::new().write(&[2, 1, 2]).read(&[1]).build();

    let context = Connection::setup_context(&mut client).await.unwrap();

    assert_eq!(context.serialiser, Serialiser::V1,);

    let mut client = TestIoBuilder::new().write(&[2, 1, 2]).read(&[0]).build();

    let result = Connection::setup_context(&mut client).await;

    assert!(matches!(result, Err(SqlError::IncompatibleVersion(0)),));
}

#[tokio::test]
async fn process_statement_basic() {
    let mut runtime = test_runtime_with_values();

    let statement = "SELECT * FROM tbl;";

    let result = process_input(statement, &mut runtime).await;

    dbg!(&result);
    assert!(matches!(result, Err(SqlError::TableDoesNotExist(_)),));

    let statement = "SELECT * FROM test_table;";

    let result = process_input(statement, &mut runtime).await.unwrap();

    let expected = runtime
        .database
        .unwrap()
        .select(
            "test_table".into(),
            crate::types::ColumnSelector::AllColumns,
            None,
        )
        .unwrap();

    assert_eq!(result, ExecutionResult::Select(expected));
}

#[tokio::test]
async fn process_statement_parse_error() {
    let mut runtime = test_runtime();

    let input = "SELECT SELECT SELECT SELECT SELECT;";

    let result = process_input(input, &mut runtime).await;

    assert!(matches!(result, Err(SqlError::ParseError)));
}

#[tokio::test]
async fn runtime_persistence_basic() {
    let mut runtime = Runtime::new(NoOp);

    let result = runtime.save().await;

    assert!(matches!(result, Err(SqlError::NoDatabaseSelected),));

    runtime.database = Some(test_db());

    assert!(runtime.drop().await.is_ok());

    assert_eq!(runtime.database, None,);

    // Always succeeds, because NoOp persistence never fails
    runtime.load(&"test_db".into()).await.unwrap();

    assert_eq!(runtime.database, Some(test_db()),);
}

#[tokio::test]
async fn special_commands_basic() {
    let mut runtime = test_runtime();

    let input = "\\c test_db";

    dbg!(&runtime);
    let result = process_input(input, &mut runtime).await.unwrap();

    assert_eq!(result, ExecutionResult::None);

    assert_eq!(runtime.database, Some(test_db()));
}

#[tokio::test]
async fn special_commands_invalid_command() {
    let mut runtime = test_runtime();

    let inputs = [("\\a", "a"), ("\\deez nuts", "deez nuts")];

    for (input, expected) in inputs {
        let result = process_input(input, &mut runtime).await;

        if let Err(SqlError::InvalidCommand(command)) = result {
            assert_eq!(command, expected);
        } else {
            dbg!(&result);
            panic!("Unexpected result");
        }
    }
}
