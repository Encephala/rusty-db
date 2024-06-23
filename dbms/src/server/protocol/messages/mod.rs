#[cfg(test)]
mod tests;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{database::RowSet, serialisation::{SerialisationManager, Serialiser}, Result, SqlError};

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
#[cfg_attr(test, derive(Clone))]
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
                    *input = &input[1..];

                    let string = string_from_input(input)?;

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
            MessageBody::Close => vec![],
            MessageBody::Ok => vec![],
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
            MessageType::Close => MessageBody::Close,
            MessageType::Ok => MessageBody::Ok,
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
    pub fn from_message_body(value: MessageBody) -> Self {
        let header = Header {
            message_type: (&value).into(),
        };

        return Message {
            header,
            body: value,
        };
    }
}

impl Message {
    pub async fn write(
        self,
        stream: &mut (impl AsyncWrite + std::marker::Unpin),
        serialisation_manager: SerialisationManager
    ) -> Result<()> {
        let serialised = self.serialise(serialisation_manager);

        stream.write_u64_le(serialised.len() as u64).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        stream.write_all(serialised.as_slice()).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        return Ok(());
    }

    fn serialise(self, serialisation_manager: SerialisationManager) -> Vec<u8> {
        let mut result = vec![];

        result.extend(self.header.to_raw().serialise());

        result.extend(self.body.serialise(serialisation_manager));

        return result;
    }

    pub async fn read(
        stream: &mut (impl AsyncReadExt + std::marker::Unpin),
        serialisation_manager: SerialisationManager
    ) -> Result<Self> {
        // TODO: Does cancel safety matter?
        // I guess not because the only other branch in tokio::select is to quit out of the program
        // Although maybe that makes client hang if it is in a write?
        // By the description of cancel safety, I think it should only leave buffer in undefined state,
        // but connection should be handled properly still?
        let length = stream.read_u64_le().await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let mut data = vec![0_u8; length as usize];
        stream.read_exact(&mut data).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let data = &mut data.as_slice();

        return Self::deserialise(data, serialisation_manager);
    }

    fn deserialise(input: &mut &[u8], serialisation_manager: SerialisationManager) -> Result<Self> {
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
}
