use std::collections::VecDeque;

use crate::{Result, SqlError};


#[derive(Debug, PartialEq)]
pub enum MessageType {
    Close,
    Ok,
    Str,
    Command,
    Error,
    RowSet,
}

impl From<&MessageType> for u8 {
    fn from(value: &MessageType) -> Self {
        use MessageType as MT;

        let result = match value {
            MT::Close => 1,
            MT::Ok => 2,
            MT::Str => 3,
            MT::Command => 4,
            MT::Error => 5,
            MT::RowSet => 6,
        };

        return result;
    }
}

impl TryFrom<u8> for MessageType {
    type Error = SqlError;

    fn try_from(value: u8) -> std::result::Result<Self, SqlError> {
        use MessageType as MT;

        return match value {
            1 => Ok(MT::Close),
            2 => Ok(MT::Ok),
            3 => Ok(MT::Str),
            4 => Ok(MT::Command),
            5 => Ok(MT::Error),
            6 => Ok(MT::RowSet),
            _ => Err(SqlError::InvalidMessageType(value)),
        };
    }
}

#[derive(Debug, Default)]
pub struct RawHeader {
    pub flags: u64,
    pub content: VecDeque<u8>,
}

impl RawHeader {
    pub fn new(flags: u64, content: impl Into<VecDeque<u8>>) -> Self {
        return RawHeader {
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

    fn set_message_type(&mut self, message_type: &MessageType) {
        self.set_flag(0);

        self.content.push_back(message_type.into());
    }

    fn parse_message_type(&mut self) -> Result<Option<MessageType>> {
        if !self.get_flag(0) {
            return Ok(None);
        }

        let message_type: MessageType = parse_u8(&mut self.content)?.try_into()?;

        return Ok(Some(message_type));
    }

    pub fn serialise(&self) -> Vec<u8> {
        let mut result = vec![];

        result.extend(self.flags.to_le_bytes());

        result.extend(&self.content);

        return result;
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Header {
    pub message_type: MessageType,
}


// Serialisation
impl Header {
    pub fn to_raw(&self) -> RawHeader {
        let mut result = RawHeader::default();

        result.set_message_type(&self.message_type);

        return result;
    }
}
impl From<Header> for RawHeader {
    fn from(header: Header) -> Self {
        return header.to_raw();
    }
}

// Deserialisation
impl TryFrom<RawHeader> for Header {
    type Error = SqlError;

    fn try_from(mut header: RawHeader) -> std::result::Result<Self, Self::Error> {
        let message_type = header.parse_message_type()?;

        if message_type.is_none() {
            return Err(SqlError::InvalidHeader("Header must contain message type"));
        }

        if !header.content.is_empty() {
            println!("Warning: unused fields in header detected");
        }

        return Ok(Header {
            message_type: message_type.unwrap(),
        });
    }
}

fn parse_u8(input: &mut VecDeque<u8>) -> Result<u8> {
    if input.is_empty() {
        return Err(SqlError::InputTooShort(input.len(), 1))
    }

    let result = input.pop_front().unwrap();

    return Ok(result);
}
