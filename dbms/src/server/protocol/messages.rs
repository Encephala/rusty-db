use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{serialisation::Serialiser, Result, SqlError};

use super::header::{MessageType, Flags, Header};


#[derive(Debug)]
pub struct Message {
    header: Header,
    body: Vec<u8>,
}

impl Message {
    pub fn empty() -> Self {
        return Self {
            header: Header::default(),
            body: vec![],
        };
    }

    // TODO: Make these testable, taking a vector or something
    async fn parse_header(stream: &mut (impl AsyncReadExt + std::marker::Unpin)) -> Result<Header> {
        let flags = Flags::new(stream.read_u64().await
            .map_err(SqlError::CouldNotReadFromConnection)?);

        let message_type = if flags.get_flag(0) {
            let result: MessageType = stream.read_u8().await
                .map_err(SqlError::CouldNotReadFromConnection)?
                .try_into()?;

            Some(result)
        } else {
            panic!("Tried deserialising header with `message_type` flag unset")
        };

        let serialisation_version = if flags.get_flag(1) {
            let result: Serialiser = stream.read_u8().await
                .map_err(SqlError::CouldNotReadFromConnection)?
                .try_into()?;

            Some(result)
        } else {
            None
        };

        return Ok(Header {
            message_type,
            serialisation_version,
        })
    }

    async fn read_body(stream: &mut (impl AsyncReadExt + std::marker::Unpin), header: &Header) -> Result<Vec<u8>> {


        todo!();
    }

    pub async fn read(stream: &mut (impl AsyncReadExt + std::marker::Unpin)) -> Result<Message> {
        let length = stream.read_u64().await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let mut result = vec![0; length as usize];

        stream.read_exact(&mut result).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let header = Self::parse_header(stream).await?;

        let body = Self::read_body(stream, &header).await?;

        return Ok(Message {
            header,
            body,
        });
    }

    // pub async fn write(&self, stream: &mut (impl AsyncWriteExt + std::marker::Unpin)) -> Result<()> {
    //     stream.write_all(self.0.as_slice()).await
    //         .map_err(SqlError::CouldNotWriteToConnection)?;

    //     stream.write_all(&[END_OF_MESSAGE]).await
    //         .map_err(SqlError::CouldNotWriteToConnection)?;

    //     // Flush to be sure, might prevent some jank bugs in the future
    //     stream.flush().await
    //         .map_err(SqlError::CouldNotWriteToConnection)?;

    //     return Ok(());
    // }

    pub fn is_empty(&self) -> bool {
        return self.body.is_empty();
    }
}
