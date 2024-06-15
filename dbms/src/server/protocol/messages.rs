use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{database::RowSet, serialisation::{SerialisationManager, Serialiser}, Result, SqlError};

use super::header::{Header, MessageType, SerialisedHeader};

pub const SERIALISATION_MANAGER: SerialisationManager = SerialisationManager(Serialiser::V2);


#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Packet {
    pub header: Header,
    pub body: Vec<u8>,
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

// TODO: Convert String, Error etc, into Packets
#[derive(Debug)]
pub enum Message {
    Close,
    Ok,
    Str(String),
    Command(Command),
    Error(SqlError),
    RowSet(RowSet),
}

impl From<Message> for Packet {
    fn from(value: Message) -> Self {
        return match value {
            Message::Close => close_message_to_packet(),
            Message::Ok => ok_to_packet(),
            Message::Str(value) => value.into(),
            Message::Command(value) => value.into(),
            Message::Error(value) => value.into(),
            Message::RowSet(value) => value.into(),
        };
    }
}

fn close_message_to_packet() -> Packet {
    let header = Header {
        message_type: MessageType::Close,
        serialisation_version: None,
    };

    return Packet {
        header,
        body: vec![],
    };
}

fn ok_to_packet() -> Packet {
    let header = Header {
        message_type: MessageType::Ok,
        serialisation_version: None,
    };

    return Packet {
        header,
        body: vec![]
    };
}

impl From<String> for Packet {
    fn from(value: String) -> Self {
        let header = Header {
            message_type: MessageType::Str,
            // TODO: Don't hardcode this, guess I can't use the from trait?
            // Or it needs to be like a global const in this module but then it's also hardcoded
            serialisation_version: None,
        };

        let mut body = (value.len() as u64).to_le_bytes().to_vec();

        body.extend(value.into_bytes());

        return Packet {
            header,
            body,
        }
    }
}

impl From<Command> for Packet {
    fn from(value: Command) -> Self {
        let header = Header {
            message_type: MessageType::Command,
            serialisation_version: None,
        };

        return Packet {
            header,
            body: value.into(),
        }
    }
}

impl From<SqlError> for Packet {
    fn from(value: SqlError) -> Self {
        let header = Header {
            message_type: MessageType::Error,
            serialisation_version: None,
        };

        let message = format!("ERROR: {:?}", value);

        let mut body = (message.len() as u64).to_le_bytes().to_vec();

        body.extend(message.into_bytes());

        return Packet {
            header,
            body,
        }
    }
}

impl From<RowSet> for Packet {
    fn from(value: RowSet) -> Self {
        let header = Header {
            message_type: MessageType::RowSet,
            serialisation_version: Some(SERIALISATION_MANAGER.0),
        };

        return Packet {
            header,
            body: SERIALISATION_MANAGER.serialise_rowset(&value),
        };
    }
}


impl Packet {
    async fn read_header(stream: &mut (impl AsyncReadExt + std::marker::Unpin)) -> Result<(usize, Header)> {
        let header_length = stream.read_u64().await
            .map_err(SqlError::CouldNotReadFromConnection)?
            as usize;

        let flags = stream.read_u64().await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let mut content = vec![0; header_length];

        stream.read_exact(&mut content).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let serialised_header = SerialisedHeader::new(
            flags,
            content,
        );

        return Ok((header_length, serialised_header.try_into()?));
    }

    pub async fn read(stream: &mut (impl AsyncReadExt + std::marker::Unpin)) -> Result<Packet> {
        let length = stream.read_u64().await
            .map_err(SqlError::CouldNotReadFromConnection)?
            as usize;

        // TODO: Check length to prevent memory exhaustion

        let mut result = vec![0; length];

        stream.read_exact(&mut result).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let (header_length, header) = Self::read_header(stream).await?;

        let mut body = vec![0; length - header_length];
        stream.read_exact(&mut body).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        return Ok(Packet {
            header,
            body,
        });
    }

    pub async fn write(&self, stream: &mut (impl AsyncWriteExt + std::marker::Unpin)) -> Result<()> {
        let mut serialised_header = self.header.serialise().content;
        let serialised_header = serialised_header.make_contiguous();

        let total_length = (serialised_header.len() + self.body.len()) as u64;

        stream.write_all(total_length.to_le_bytes().as_slice()).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        stream.write_all(serialised_header).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        stream.write_all(self.body.as_slice()).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        // Flush to be sure, might prevent some jank bugs in the future
        stream.flush().await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        return Ok(());
    }

    fn parse(&self) -> () {
        todo!();
    }
}
