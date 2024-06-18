use tokio::{
    io::BufReader,
    net::{
        TcpListener,
        TcpStream,
    },
};

use crate::serialisation::{SerialisationManager, Serialiser};

use super::{Message, MessageBody};

// Only have one test actually open a listener,
// otherwise we'd have conflicts and stuff
// #[tokio::test]
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
