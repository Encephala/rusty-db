use tokio::{
    io::BufReader,
    net::{
        TcpListener,
        TcpStream,
    },
};

use tokio_test::io::Builder as TestIoBuilder;

use crate::{persistence::NoOp, serialisation::{SerialisationManager, Serialiser}, utils::tests::*};

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

    test_message.write(&mut stream, SerialisationManager(Serialiser::V2)).await.unwrap();

    let read_message = Message::read(
        &mut BufReader::new(response_stream),
        Serialiser::V2
    ).await.unwrap();

    if let MessageBody::Str(value) = read_message.body {
        assert_eq!(
            value,
            "deez nuts"
        );
    } else {
        panic!("Body wrong type");
    }
}

#[test]
fn create_drop_get_database() {
    let mut runtime = Runtime::new(NoOp);

    let db = test_db();

    runtime.create_database(db);

    let db = runtime.get_database();

    // Kinda pointless but whatever
    assert_eq!(
        db.unwrap().name,
        "test_db".into(),
    );

    let name = runtime.drop_database().unwrap();

    assert_eq!(
        name,
        "test_db".into(),
    );
}

#[tokio::test]
async fn negotiate_serialiser_version() {
    let mut client = TestIoBuilder::new()
        .write(&[2, 1, 2])
        .read(&[1])
        .build();

    let negotiated_version = Connection::negotiate_serialiser_version(&mut client).await.unwrap();

    assert_eq!(negotiated_version, Serialiser::V1);

    let mut client = TestIoBuilder::new()
        .write(&[2, 1, 2])
        .read(&[2])
        .build();

    let negotiated_version = Connection::negotiate_serialiser_version(&mut client).await.unwrap();

    assert_eq!(negotiated_version, Serialiser::V2);

    let mut client = TestIoBuilder::new()
        .write(&[2, 1, 2])
        .read(&[3])
        .build();

    let negotiated_version = Connection::negotiate_serialiser_version(&mut client).await;

    assert!(matches!(
        negotiated_version,
        Err(SqlError::IncompatibleVersion(3)),
    ));
}

#[tokio::test]
async fn setup_context() {
    let mut client = TestIoBuilder::new()
        .write(&[2, 1, 2])
        .read(&[1])
        .build();

    let context = Connection::setup_context(&mut client).await.unwrap();

    assert_eq!(
        context.serialiser,
        Serialiser::V1,
    );

    let mut client = TestIoBuilder::new()
        .write(&[2, 1, 2])
        .read(&[0])
        .build();

    let result = Connection::setup_context(&mut client).await;

    assert!(matches!(
        result,
        Err(SqlError::IncompatibleVersion(0)),
    ));
}
