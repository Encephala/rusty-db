#![allow(clippy::needless_return)]
use std::io::Write;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
};

use dbms::{
    serialisation::SerialisationManager, server::{Command, Message, MessageBody}, types::DatabaseName, utils::serialiser_version_to_serialiser, SqlError
};

async fn session(address: impl ToSocketAddrs) -> Result<(), SqlError> {
    let mut stream = TcpStream::connect(address).await.unwrap();

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

    let serialisation_manager = SerialisationManager(serialiser);

    stream
        .write_u8(*highest_serialiser)
        .await
        .map_err(SqlError::CouldNotWriteToConnection)?;

    loop {
        let input = rep_without_the_l();

        let message = if let Some(command) = parse_command(&input) {
            Message::from_message_body(MessageBody::Command(command))
        } else {
            Message::from_message_body(MessageBody::Str(input))
        };

        message.write(&mut stream, serialisation_manager).await?;

        let response = Message::read(&mut stream, serialisation_manager).await?;

        match response.body {
            // TODO: This doesn't work, because we're waiting on user input synchronously
            // That has to be made async
            MessageBody::Close => break,
            MessageBody::Ok => (),
            MessageBody::Str(message) => println!("{message}"),
            MessageBody::Command(uhoh) => panic!("Client received a command? What is going on ({uhoh:?})"),
            // Actually this never gets sent, errors get sent as a Str because serialisation is hard
            MessageBody::Error(error) => println!("ERROR: {error:?}"),
            MessageBody::RowSet(rowset) => println!("{rowset:?}"),
        }
    }

    return Ok(());
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

fn parse_command(input: &str) -> Option<Command> {
    if let Some(database_name) = input.strip_prefix("\\c ") {
        return Some(Command::Connect(DatabaseName(database_name.to_string())));
    }

    if input.starts_with("\\l") {
        return Some(Command::ListTables);
    }

    if input.starts_with("\\d") {
        return Some(Command::ListDatabases);
    }

    return None;
}

#[tokio::main]
async fn main() {
    session("localhost:42069").await.unwrap();
}
