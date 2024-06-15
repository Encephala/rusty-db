use std::collections::VecDeque;

use crate::{Result, SqlError};
use crate::serialisation::Serialiser;


#[derive(Debug, PartialEq)]
pub enum MessageType {
    Close,
    Ack,
    String,
    Command,
    ErrorMessage,
    RowSet,
}

impl From<&MessageType> for u8 {
    fn from(value: &MessageType) -> Self {
        use MessageType::*;

        let result = match value {
            Close => 1,
            Ack => 2,
            String => 3,
            Command => 4,
            ErrorMessage => 5,
            RowSet => 6,
        };

        return result;
    }
}

impl TryFrom<u8> for MessageType {
    type Error = SqlError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use MessageType::*;

        return match value {
            1 => Ok(Close),
            2 => Ok(Ack),
            3 => Ok(String),
            4 => Ok(Command),
            5 => Ok(ErrorMessage),
            6 => Ok(RowSet),
            _ => Err(SqlError::InvalidMessageType(value)),
        };
    }
}

#[derive(Debug, Default)]
pub struct SerialisedHeader {
    flags: u64,
    pub content: VecDeque<u8>,
}

#[cfg(test)]
impl SerialisedHeader {
    pub fn flags(&self) -> u64 {
        return self.flags;
    }
}

impl SerialisedHeader {
    pub fn new(flags: u64, content: impl Into<VecDeque<u8>>) -> Self {
        return SerialisedHeader {
            flags,
            content: content.into(),
        };
    }

    // Asserts here over returning Results because the indices should be hardcoded,
    // so a wrong index isn't a runtime error that should be handled,
    // rather tests should catch these panics
    pub fn set_flag(&mut self, index: u8) {
        assert!(index <= 63);

        self.flags |= 1 << (63 - index);
    }

    pub fn get_flag(&self, index: u8) -> bool {
        assert!(index <= 63);

        return (self.flags & 1 << (63 - index)) != 0;
    }

    fn set_message_type(&mut self, message_type: &Option<MessageType>) {
        match message_type {
            Some(message_type) => {
                self.set_flag(0);

                self.content.push_back(message_type.into());
            }
            None => panic!("Tried serialising header with `message_type` flag unset"),
        }
    }

    fn set_serialisation_version(&mut self, serialisation_version: &Option<Serialiser>) {
        if let Some(serialisation_version) = serialisation_version {
            self.set_flag(1);

            self.content.push_back(serialisation_version.into());
        }
    }
}

#[derive(Debug, Default)]
pub struct Header {
    pub message_type: Option<MessageType>,
    pub serialisation_version: Option<Serialiser>,
}


// Serialisation
impl Header {
    pub fn serialise(&self) -> SerialisedHeader {
        let mut result = SerialisedHeader::default();

        result.set_message_type(&self.message_type);

        result.set_serialisation_version(&self.serialisation_version);

        // Rev because pushing one-by-one to front reverses the order
        for value in result.content.len().to_le_bytes().into_iter().rev() {
            result.content.push_front(value);
        }

        return result;
    }
}
impl From<Header> for SerialisedHeader {
    fn from(header: Header) -> Self {
        return header.serialise();
    }
}

// Deserialisation
impl TryFrom<SerialisedHeader> for Header {
    type Error = SqlError;

    fn try_from(mut header: SerialisedHeader) -> std::result::Result<Self, Self::Error> {
        let mut result = Header::default();

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

fn parse_u8(input: &mut VecDeque<u8>) -> Result<u8> {
    if input.is_empty() {
        return Err(SqlError::InputTooShort(input.len(), 1))
    }

    let result = input.pop_front().unwrap();

    return Ok(result);
}

fn parse_message_type(header: &mut SerialisedHeader) -> Result<Option<MessageType>> {
    if !header.get_flag(0) {
        return Ok(None);
    }

    let message_type: MessageType = parse_u8(&mut header.content)?.try_into()?;

    return Ok(Some(message_type));
}

fn parse_serialisation_version(header: &mut SerialisedHeader) -> Result<Option<Serialiser>> {
    if !header.get_flag(1) {
        return Ok(None);
    }

    let result: Serialiser = parse_u8(&mut header.content)?.try_into()?;

    return Ok(Some(result));
}
