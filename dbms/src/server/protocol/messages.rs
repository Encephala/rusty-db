use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    Result, SqlError
};

use super::header::ParsedHeader;


#[derive(Debug)]
pub struct Message {
    header: ParsedHeader,
    body: Vec<u8>,
}

// impl Message {
//     pub fn empty() -> Self {
//         return Self(vec![]);
//     }

//     pub fn from(message: impl Into<Vec<u8>>) -> Self {
//         return Self(message.into());
//     }

//     pub async fn read(stream: &mut (impl AsyncBufReadExt + std::marker::Unpin)) -> Result<Message> {
//         let mut result = vec![];

//         stream.read_until(END_OF_MESSAGE, &mut result).await
//             .map_err(SqlError::CouldNotReadFromConnection)?;

//         // Remove null character
//         result.pop();

//         return Ok(Message(result));
//     }

//     pub async fn write(&self, stream: &mut (impl AsyncWrite + std::marker::Unpin)) -> Result<()> {
//         stream.write_all(self.0.as_slice()).await
//             .map_err(SqlError::CouldNotWriteToConnection)?;

//         stream.write_all(&[END_OF_MESSAGE]).await
//             .map_err(SqlError::CouldNotWriteToConnection)?;

//         // Flush to be sure, might prevent some jank bugs in the future
//         stream.flush().await
//             .map_err(SqlError::CouldNotWriteToConnection)?;

//         return Ok(());
//     }

//     pub fn is_empty(&self) -> bool {
//         return self.body.is_empty();
//     }
// }
