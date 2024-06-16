use crate::{database::RowSet, serialisation::SerialisationManager, Result, SqlError};

use super::header::{Header, MessageType, RawHeader};

fn u64_from_input(input: &mut &[u8]) -> Result<u64> {
    let result = u64::from_le_bytes(
        input[..8].try_into()
            .map_err(SqlError::SliceConversionError)?
    );

    *input = &input[8..];

    return Ok(result);
}

fn string_from_input(input: &mut &[u8]) -> Result<String> {
    let length = u64_from_input(input)? as usize;

    let string = String::from_utf8(
        input.get(..length)
            .ok_or(SqlError::InputTooShort(length, input.len()))?
            .to_vec()
    ).map_err(SqlError::NotAValidString)?;

    *input = &input[length..];

    return Ok(string);
}


#[derive(Debug)]
pub struct Message {
    pub header: Header,
    pub body: MessageBody,
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Connect(String),
    ListDatabases,
    ListTables,
}

// TODO: Test
impl From<Command> for Vec<u8> {
    fn from(value: Command) -> Self {
        let mut result = vec![];

        match value {
            Command::Connect(database_name) => {
                result.push(1);

                result.extend((database_name.len() as u64).to_le_bytes());

                result.extend(database_name.into_bytes());
            },
            Command::ListDatabases => {
                result.push(2);
            },
            Command::ListTables => {
                result.push(3);
            },
        };

        return result;
    }
}

impl Command {
    fn deserialise(input: &mut &[u8]) -> Result<Self> {
        let result = match input.first()
            .ok_or(SqlError::InputTooShort(1, input.len()))? {
                1 => {
                    let length = u64_from_input(input)? as usize;

                    let string = String::from_utf8(
                        input.get(..length)
                            .ok_or(SqlError::InputTooShort(length, input.len()))?
                            .to_vec()
                    ).map_err(SqlError::NotAValidString)?;

                    *input = &input[length..];

                    Command::Connect(string)
                }
                2 => {
                    *input = &input[1..];

                    Command::ListDatabases
                },
                3 => {
                    *input = &input[1..];

                    Command::ListTables
                }
                _ => return Err(SqlError::InvalidMessage(input.to_vec()))
        };

        return Ok(result);
    }
}

#[derive(Debug)]
pub enum MessageBody {
    Close,
    Ok,
    Str(String),
    Command(Command),
    Error(SqlError),
    RowSet(RowSet),
}

impl MessageBody {
    fn serialise(self, serialisation_manager: SerialisationManager) -> Vec<u8> {
        return match self {
            MessageBody::Close => todo!(),
            MessageBody::Ok => todo!(),
            MessageBody::Str(value) => {
                let mut result = (value.len() as u64).to_le_bytes().to_vec();

                result.extend(value.into_bytes());

                result
            },
            MessageBody::Command(value) => value.into(),
            MessageBody::Error(value) => {
                let message = format!("ERROR: {:?}", value);

                let mut result = (message.len() as u64).to_le_bytes().to_vec();

                result.extend(message.into_bytes());

                result
            },
            MessageBody::RowSet(value) => serialisation_manager.serialise_rowset(&value),
        };
    }

    fn deserialise(input: &mut &[u8], message_type: &MessageType, serialisation_manager: SerialisationManager) -> Result<Self> {
        let result = match message_type {
            MessageType::Close => {
                *input = &input[1..];

                MessageBody::Close
            },
            MessageType::Ok => {
                *input = &input[1..];

                MessageBody::Ok
            },
            MessageType::Str => {
                let string = string_from_input(input)?;

                MessageBody::Str(string)
            },
            MessageType::Command => {
                let command = Command::deserialise(input)?;

                MessageBody::Command(command)
            },
            MessageType::Error => {
                let string = string_from_input(input)?;

                // TODO: This works for now, but maybe properly convert SqlError to binary.
                // But that's really hard because it contains expressions etc.
                // So maybe better way of signalling that this was an error?
                MessageBody::Str(string)
            },
            MessageType::RowSet => {
                let result = serialisation_manager.deserialise_rowset(input)?;

                MessageBody::RowSet(result)
            }
        };

        return Ok(result);
    }
}

impl From<&MessageBody> for MessageType {
    fn from(value: &MessageBody) -> Self {
        use MessageType as MT;

        return match value {
            MessageBody::Close => MT::Close,
            MessageBody::Ok => MT::Ok,
            MessageBody::Str(_) => MT::Str,
            MessageBody::Command(_) => MT::Command,
            MessageBody::Error(_) => MT::Error,
            MessageBody::RowSet(_) => MT::RowSet
        }
    }
}

impl Message {
    pub fn from_message_body(value: MessageBody, serialisation_manager: &SerialisationManager) -> Self {
        let header = Header {
            message_type: (&value).into(),
            serialisation_version: Some(serialisation_manager.0),
        };

        return Message {
            header,
            body: value,
        };
    }
}


impl Message {
    pub fn deserialise(input: &mut &[u8], serialisation_manager: SerialisationManager) -> Result<Self> {
        let header: Header = Self::parse_raw_header(input)?.try_into()?;

        let body = MessageBody::deserialise(input, &header.message_type, serialisation_manager)?;

        if !input.is_empty() {
            println!("Warning: deserialising message ignored remaining {} bytes", input.len());
        }

        return Ok(Message {
            header,
            body,
        });
    }

    fn parse_raw_header(input: &mut &[u8]) -> Result<RawHeader> {
        let mut result = RawHeader {
            flags: u64_from_input(input)?,
            content: vec![].into(),
        };

        // Length of header in bytes
        let mut header_length = 0;

        if !result.get_flag(0) {
            return Err(SqlError::InvalidHeader("Header didn't have `message_type` flag set"));
        }

        header_length += 1;

        if result.get_flag(1) {
            header_length += 1;
        }

        result.content = input[..header_length].to_vec().into();

        *input = &input[header_length..];

        return Ok(result);
    }

    pub fn serialise(self, serialisation_manager: SerialisationManager) -> Result<Vec<u8>> {
        let mut result = vec![];

        result.extend(self.header.to_raw().serialise());

        result.extend(self.body.serialise(serialisation_manager));

        return Ok(result);
    }
}
