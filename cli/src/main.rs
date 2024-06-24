#![allow(clippy::needless_return)]
use std::io::Write;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, ToSocketAddrs},
};

use dbms::{
    serialisation::{SerialisationManager, Serialiser},
    server::Message,
    utils::serialiser_version_to_serialiser,
    SqlError,
};

const SERIALISATION_MANAGER: SerialisationManager = SerialisationManager::new(Serialiser::V2);

async fn session(address: impl ToSocketAddrs) -> Result<(), SqlError> {
    let mut stream = TcpStream::connect(address).await.unwrap();

    let (reader, mut writer) = stream.split();

    let mut reader = BufReader::new(reader);

    // TODO: How do I do this?
    let number_of_serialisers = stream
        .read_u8()
        .await
        .map_err(SqlError::CouldNotReadFromConnection)?;

    let mut serialisers_buffer = vec![0_u8; number_of_serialisers as usize];

    stream
        .read_exact(&mut serialisers_buffer)
        .await
        .map_err(SqlError::CouldNotReadFromConnection)?;

    if serialisers_buffer.is_empty() {
        return Err(SqlError::InputTooShort(0, 1));
    }

    let highest_serialiser = serialisers_buffer.iter().max().unwrap();

    let serialiser = serialiser_version_to_serialiser(*highest_serialiser)?;

    println!("Chose serialiser {serialiser:?}");

    stream
        .write_u8(*highest_serialiser)
        .await
        .map_err(SqlError::CouldNotWriteToConnection)?;

    todo!();

    // loop {
    //     let input = rep_without_the_l();

    //     let message = Message::from(input);

    //     message.write(&mut writer).await?;

    //     let response = Message::read(&mut reader).await?;

    //     let deserialised_response = SERIALISATION_MANAGER.deserialise_rowset(response.0.as_slice());

    //     match deserialised_response {
    //         Ok(response) => {
    //             println!("Result: {response:?}");

    //             continue;
    //         },
    //         Err(error) => println!("Deserialisation error: {error:?}"),
    //     }

    //     match std::str::from_utf8(&response.0) {
    //         Ok(response) => println!("Text response: {response}"),
    //         Err(_) => println!("Got binary message: {:?}", response.0),
    //     }
    // }

    // return Ok(());
}

fn rep_without_the_l() -> String {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    print!(">> ");
    stdout.flush().unwrap();

    let mut input = String::new();

    stdin.read_line(&mut input).unwrap();

    // Strip the newline character from the input
    // TODO: probably should fix this by properly implementing special commands,
    // where they don't blow up if there's extra whitespace at the end
    input.pop();

    return input;
}

#[tokio::main]
async fn main() {
    session("localhost:42069").await.unwrap();
}
