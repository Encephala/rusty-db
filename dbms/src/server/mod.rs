use std::{io::{Read, Write}, net::TcpStream};

use crate::{Result, SqlError};
// use sql_parse::parse_statement;

pub async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    write_welcome(&mut stream)?;

    let buf = &mut vec![];
    stream.read_to_end(buf)
        .map_err(SqlError::CouldNotReadFromConnection)?;

    // Handle message
    println!("Got message {}", std::str::from_utf8(buf).unwrap());

    return Ok(());
}

fn write_welcome(stream: &mut TcpStream) -> Result<()> {
    return stream.write_all(&[0x48, 0x45, 0x4C, 0x4C, 0x4F])
        .map_err(SqlError::CouldNotWriteToConnection);
}
