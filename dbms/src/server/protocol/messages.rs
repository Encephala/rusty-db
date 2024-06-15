use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{Result, SqlError};

use super::header::{Header, SerialisedHeader};


#[derive(Debug)]
pub struct Message {
    header: Header,
    body: Vec<u8>,
}

// TODO: Convert String, Error etc, into Messages

impl Message {
    pub fn empty() -> Self {
        return Self {
            header: Header::default(),
            body: vec![],
        };
    }

    // TODO: Make these testable, taking a vector or something
    async fn parse_header(stream: &mut (impl AsyncReadExt + std::marker::Unpin)) -> Result<Header> {
        let length = stream.read_u64().await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let flags = stream.read_u64().await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let mut content = vec![0; length as usize];

        stream.read_exact(&mut content).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let serialised_header = SerialisedHeader::new(
            flags,
            content,
        );

        return serialised_header.try_into();
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

    pub async fn write(&self, stream: &mut (impl AsyncWriteExt + std::marker::Unpin)) -> Result<()> {
        let mut serialised_header = self.header.serialise().content;
        let serialised_header = serialised_header.make_contiguous();

        let total_length = serialised_header.len() + self.body.len();

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
}
