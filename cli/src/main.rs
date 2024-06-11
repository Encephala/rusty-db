#![allow(clippy::needless_return)]
mod serverless;

use tokio::net::{
    TcpStream,
    ToSocketAddrs,
};

use dbms::{
    SqlError,
    serialisation::{SerialisationManager, Serialiser},
    server::Message,
};

const SERIALISATION_MANAGER: SerialisationManager = SerialisationManager::new(Serialiser::V2);

async fn session(address: impl ToSocketAddrs) -> Result<(), SqlError> {
    let mut stream = TcpStream::connect(address).await.unwrap();

    println!("connected");

    let welcome_message = Message::read(&mut stream).await?;

    println!(
        "Got welcome message: {}",
        String::from_utf8(welcome_message.0).unwrap()
    );

    let response = "ya mum\n".bytes().collect();

    Message(response).write(&mut stream).await.unwrap();
    println!("Wrote response");

    return Ok(());
}

#[tokio::main]
async fn main() {
    session("localhost:42069").await.unwrap();
}
