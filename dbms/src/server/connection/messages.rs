use tokio::io::{AsyncWrite, AsyncWriteExt, AsyncBufReadExt};

use crate::{
    evaluate::ExecutionResult, Result, SqlError
};

use super::SERIALISATION_MANAGER;

const END_OF_MESSAGE: u8 = 255;

#[derive(Debug, PartialEq)]
pub struct Message(pub Vec<u8>);

impl Message {
    pub fn empty() -> Self {
        return Self(vec![]);
    }

    pub fn from(message: impl Into<Vec<u8>>) -> Self {
        return Self(message.into());
    }

    pub async fn read(stream: &mut (impl AsyncBufReadExt + std::marker::Unpin)) -> Result<Message> {
        let mut result = vec![];

        stream.read_until(END_OF_MESSAGE, &mut result).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        // Remove null character
        result.pop();

        return Ok(Message(result));
    }

    pub async fn write(&self, stream: &mut (impl AsyncWrite + std::marker::Unpin)) -> Result<()> {
        stream.write_all(self.0.as_slice()).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        stream.write_all(&[END_OF_MESSAGE]).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        // Flush to be sure, might prevent some jank bugs in the future
        stream.flush().await
            .map_err(SqlError::CouldNotWriteToConnection)?;

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
            ExecutionResult::Select(rowset) => {
                let bytes = SERIALISATION_MANAGER.serialise_rowset(&rowset);

                Message(bytes)
            },
            ExecutionResult::CreateDatabase(database_name) => Message::from(
                format!("Created database {}", database_name.0)
            ),
            ExecutionResult::DropDatabase(database_name) => Message::from(
                format!("Dropped database {}", database_name.0)
            ),
            ExecutionResult::Table(table) => Message::from(
                format!("Dropped table:\n{table:?}")
            ),
        };
    }
}
