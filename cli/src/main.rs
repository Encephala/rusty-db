#![allow(clippy::needless_return)]
mod serverless;

use async_std::{
    io::{prelude::BufReadExt, BufReader, WriteExt},
    net::TcpStream,
};

async fn connect_to_server() {
    let mut stream = TcpStream::connect("localhost:42069").await.unwrap();

    let mut reader = BufReader::new(&stream);

    let buffer = &mut vec![];

    reader.read_until(0xA, buffer).await.unwrap();

    println!(
        "Got welcome message {}",
        String::from_utf8(buffer.to_owned()).unwrap()
    );

    let response = "ya mum\n".to_owned().into_bytes();

    stream.write_all(response.as_slice()).await.unwrap();
}

#[tokio::main]
async fn main() {
    connect_to_server().await;
}
