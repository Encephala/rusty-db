#![allow(clippy::needless_return)]
mod serverless;

use tokio::{io::BufReader, net::{
    TcpStream,
    ToSocketAddrs,
}};

use dbms::{
    SqlError,
    serialisation::{SerialisationManager, Serialiser},
    server::Message,
};

const SERIALISATION_MANAGER: SerialisationManager = SerialisationManager::new(Serialiser::V2);

async fn session(address: impl ToSocketAddrs) -> Result<(), SqlError> {
    let mut stream = TcpStream::connect(address).await.unwrap();

    let (reader, mut writer) = stream.split();

    let mut reader = BufReader::new(reader);

    let welcome_message = Message::read(&mut reader).await?;

    println!(
        "Got welcome message: {}",
        String::from_utf8(welcome_message.0).unwrap()
    );

    let select_db = "\\c sweden";
    Message::from(select_db).write(&mut writer).await?;

    let select_query = "SELECT * FROM mcdonalds;";
    Message::from(select_query).write(&mut writer).await?;

    let response = Message::read(&mut reader).await?;

    println!("{response:?}");

    return Ok(());
}

#[tokio::main]
async fn main() {
    session("localhost:42069").await.unwrap();
}
