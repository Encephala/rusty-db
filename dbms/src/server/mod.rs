use async_std::{
    io::{
        prelude::BufReadExt,
        BufReader,
        WriteExt
    },
    net::TcpStream
};

use tokio::sync::broadcast::Receiver;

use crate::{Result, SqlError};
// use sql_parse::parse_statement;

pub async fn handle_connection(mut stream: TcpStream, mut shutdown_signal: Receiver<()>) -> Result<()> {
    write_welcome(&mut stream).await?;

    let mut reader = BufReader::new(&stream);

    let buf = &mut vec![];

    loop {
        tokio::select! {
            read_result = reader.read_until(b'\n', buf) => {
                // Stream closed
                if let Ok(0) = read_result {
                    break;
                }

                // Handle read error
                read_result.map_err(SqlError::CouldNotReadFromConnection)?;

                // Handle message
                // Dummy print for now
                println!("Got message {}", std::str::from_utf8(buf).unwrap());
            },
            _ = shutdown_signal.recv() => {
                break;
            }
        }
    }

    return Ok(());
}

async fn write_welcome(stream: &mut TcpStream) -> Result<()> {
    return stream.write_all(b"HELLO\n").await
        .map_err(SqlError::CouldNotWriteToConnection);
}
