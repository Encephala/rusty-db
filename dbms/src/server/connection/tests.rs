use tokio::net::{
    TcpListener,
    TcpStream,
};

use super::Message;

// Only have one test actually open a listener,
// otherwise we'd have conflicts and stuff
#[tokio::test]
async fn message_read_write() {
    let test_message: Message = b"deez nuts".as_slice().into();

    let listener = TcpListener::bind("localhost:12345").await.unwrap();

    let mut stream = TcpStream::connect("localhost:12345").await.unwrap();

    let (mut response_stream, _) = listener.accept().await.unwrap();

    test_message.write(&mut stream).await.unwrap();

    let read_message = Message::read(&mut response_stream).await.unwrap();

    assert_eq!(
        read_message.0,
        b"deez nuts"
    );
}

#[test]
fn test_write_message_format() {
    let test_message = b"deez nuts";

    let length = test_message.len();

    let message_to_be_written: Message = test_message.as_slice().into();

    assert_eq!(
        message_to_be_written.0.len(),
        8 + length
    );

    // Length of message
    assert_eq!(
        message_to_be_written.0.get(0..8).unwrap(),
        vec![9, 0, 0, 0, 0, 0, 0, 0]
    );

    assert_eq!(
        message_to_be_written.0.get(8..).unwrap(),
        test_message
    );
}
