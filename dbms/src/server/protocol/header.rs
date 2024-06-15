use std::collections::VecDeque;

use crate::{SqlError, Result};
use crate::serialisation::Serialiser;


#[derive(Debug, PartialEq)]
pub enum MessageType {
    Close,
    Ack,
    String,
    Command,
    Error,
    RowSet,
}

#[derive(Debug)]
pub struct Header {
    pub flags: u64,
    pub content: VecDeque<u8>,
}

impl Header {
    // Asserts here over returning Results because the indices should be hardcoded,
    // so a wrong index isn't a runtime error that should be handled,
    // rather tests should catch these panics
    pub fn set_flag(&mut self, index: u8) {
        assert!(index <= 63);

        self.flags |= 1 << (63 - index);
    }

    pub fn clear_flag(&mut self, index: u8) {
        assert!(index <= 63);

        self.flags &= !(1 << (63 - index));
    }

    pub fn get_flag(&self, index: u8) -> bool {
        assert!(index <= 63);

        return (self.flags & 1 << (63 - index)) != 0;
    }
}

#[derive(Debug, Default)]
pub struct ParsedHeader {
    pub message_type: Option<MessageType>,
    pub serialisation_version: Option<Serialiser>,
}

impl TryFrom<Header> for ParsedHeader {
    type Error = SqlError;

    fn try_from(mut header: Header) -> std::result::Result<Self, Self::Error> {
        let mut result = ParsedHeader::default();

        #[allow(clippy::field_reassign_with_default)]
        { result.message_type = parse_message_type(&mut header)?; }

        if result.message_type.is_none() {
            return Err(SqlError::InvalidHeader("Header must contain message type"));
        }

        result.serialisation_version = parse_serialisation_version(&mut header)?;

        if !header.content.is_empty() {
            println!("Warning: unused fields in header detected");
        }

        return Ok(result);
    }
}

fn parse_u64(input: &mut VecDeque<u8>) -> Result<u64> {
    if input.len() < 8 {
        return Err(SqlError::InputTooShort(input.len(), 8))
    }

    // try_into to convert length(?)
    let bytes: [u8; 8] = input.drain(..8)
        .collect::<Vec<_>>()
        .as_slice()
        .try_into()
        .map_err(SqlError::SliceConversionError)?;

    let result = u64::from_le_bytes(bytes);

    return Ok(result);
}

fn parse_u8(input: &mut VecDeque<u8>) -> Result<u8> {
    if input.is_empty() {
        return Err(SqlError::InputTooShort(input.len(), 1))
    }

    let result = input.pop_front().unwrap();

    return Ok(result);
}

fn parse_message_type(header: &mut Header) -> Result<Option<MessageType>> {
    use MessageType::*;

    if !header.get_flag(0) {
        return Ok(None);
    }

    return match parse_u64(&mut header.content)? {
        1 => Ok(Some(Close)),
        2 => Ok(Some(Ack)),
        3 => Ok(Some(String)),
        4 => Ok(Some(Command)),
        5 => Ok(Some(Error)),
        6 => Ok(Some(RowSet)),
        other => Err(SqlError::InvalidMessageType(other)),
    };
}

fn parse_serialisation_version(header: &mut Header) -> Result<Option<Serialiser>> {
    if !header.get_flag(1) {
        return Ok(None);
    }

    let result: Serialiser = parse_u8(&mut header.content)?.try_into()?;

    return Ok(Some(result));
}
