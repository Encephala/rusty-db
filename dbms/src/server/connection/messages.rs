
use tokio::io::{AsyncWrite, AsyncWriteExt, AsyncRead, BufReader, AsyncBufReadExt};

use crate::{
    Result, SqlError,
    evaluate::ExecutionResult,
};

#[derive(Debug, PartialEq)]
pub struct Message(pub Vec<u8>);

impl Message {
    pub fn empty() -> Self {
        return Self(vec![]);
    }

    pub fn from(message: impl Into<Vec<u8>>) -> Self {
        return Self(message.into());
    }

    pub async fn read(stream: &mut BufReader<impl AsyncRead + std::marker::Unpin>) -> Result<Message> {
        let mut result = vec![];

        stream.read_until(b'\0', &mut result).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        // Remove null character
        result.pop();

        return Ok(Message(result));
    }

    pub async fn write(&self, stream: &mut (impl AsyncWrite + std::marker::Unpin)) -> Result<()> {
        stream.write_all(self.0.as_slice()).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        stream.write_all(&[b'\0']).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        // stream.flush().await
        //     .map_err(SqlError::CouldNotWriteToConnection)?;

        return Ok(());
    }

    pub fn value(&self) -> &Vec<u8> {
        return &self.0;
    }
}

impl From<ExecutionResult> for Message {
    // TODO: Better responses
    fn from(value: ExecutionResult) -> Self {
        return match value {
            ExecutionResult::None => Message::empty(),
            an_actual_result => Message::from(
                format!("Executed:\n{an_actual_result:?}")
            ),
        };
    }
}
