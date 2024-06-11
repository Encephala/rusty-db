use tokio::{
    io::BufReader,
    net::{
        TcpListener,
        TcpStream,
    },
};

use super::Message;

// Only have one test actually open a listener,
// otherwise we'd have conflicts and stuff
#[tokio::test]
async fn message_read_write() {
    let test_message: Message = Message::from(b"deez nuts".as_slice());

    let listener = TcpListener::bind("localhost:12345").await.unwrap();

    let mut stream = TcpStream::connect("localhost:12345").await.unwrap();

    let (response_stream, _) = listener.accept().await.unwrap();

    test_message.write(&mut stream).await.unwrap();

    let read_message = Message::read(&mut BufReader::new(response_stream)).await.unwrap();

    assert_eq!(
        read_message.0,
        b"deez nuts"
    );
}
