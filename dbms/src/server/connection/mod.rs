#[cfg(test)]
mod tests;

use std::path::PathBuf;

use tokio::{
    sync::broadcast::Receiver,
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::TcpStream
};

use crate::{
    evaluate::{
        Execute,
        ExecutionResult
    },
    persistence::{
        FileSystem,
        PersistenceManager
    },
    serialisation::{
        SerialisationManager,
        Serialiser
    },
    types::DatabaseName, Database, Result, SqlError
};

use sql_parse::{parse_statement, parser::{CreateType, Statement}};

struct Runtime {
    persistence_manager: Box<dyn PersistenceManager>,
    database: Option<Database>,
}

const SERIALISATION_MANAGER: SerialisationManager = SerialisationManager::new(Serialiser::V2);

#[derive(Debug, PartialEq)]
pub struct Message(pub Vec<u8>);

impl From<&[u8]> for Message {
    fn from(value: &[u8]) -> Self {
        let mut result = u64_to_bytes(value.len() as u64);

        result.extend(value);

        return Self(result);
    }
}

impl Message {
    pub fn empty() -> Self {
        return Self(vec![]);
    }

    pub async fn read(stream: &mut TcpStream) -> Result<Message> {
        println!("Reading at {:?}", std::time::Instant::now());
        let length = &mut [0u8; 8];

        println!("Waiting to read length");
        stream.read_exact(length).await
            .map_err(SqlError::CouldNotReadFromConnection)?;
        println!("Got length {length:?}");

        let length = u64::from_le_bytes(*length);

        let mut result = vec![0u8; length as usize];

        stream.read_exact(&mut result).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        println!("Succeed at reading message {result:?}");
        return Ok(Message(result));
    }

    pub async fn write(&self, stream: &mut TcpStream) -> Result<()> {
        println!("Writing {:?} at {:?}", self.0, std::time::Instant::now());
        stream.write_all(self.0.as_slice()).await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        stream.flush().await
            .map_err(SqlError::CouldNotWriteToConnection)?;

        println!("Writing succeeded at {:?}", std::time::Instant::now());

        return Ok(());
    }
}

impl From<ExecutionResult> for Message {
    // TODO: Better responses
    fn from(value: ExecutionResult) -> Self {
        let result = match value {
            ExecutionResult::None => vec![],
            an_actual_result => format!(
                "Executed:\n{an_actual_result:?}"
            ).bytes().collect(),
        };

        return Self(result);
    }
}

pub async fn handle_connection(mut stream: TcpStream, mut shutdown_signal: Receiver<()>) -> Result<()> {
    welcome_message().write(&mut stream).await?;
    println!("Wrote welcome message");

    let mut runtime = Runtime {
        persistence_manager: Box::new(FileSystem::new(
                SERIALISATION_MANAGER,
                PathBuf::from("/tmp/rusty-db")
            )),
        database: None,
    };

    loop {
        tokio::select! {
            message = Message::read(&mut stream) => {
                let message = message?;

                // Stream closed
                if Message::empty() == message {
                    break;
                }

                // Handle message
                println!("Got message {:?}", message);

                process_statement(message.0, &mut runtime).await?;
            },
            _ = shutdown_signal.recv() => {
                break;
            }
        }
    }

    return Ok(());
}

fn welcome_message() -> Message {
    return b"deez nuts".as_slice().into();
}

fn u64_to_bytes(value: u64) -> Vec<u8> {
    return value.to_le_bytes().to_vec();
}

async fn process_statement(buffer: Vec<u8>, runtime: &mut Runtime) -> Result<ExecutionResult> {
    let input = &String::from_utf8(buffer)
        .map_err(SqlError::NotAValidString)?;

    if input.starts_with("\\c ") {
        let database_name = input.strip_prefix("\\c ").unwrap().strip_suffix('\n').unwrap();

        runtime.database = match runtime.persistence_manager.load_database(DatabaseName(database_name.into())).await {
            Ok(db) => {
                println!("Connected to database {}", db.name.0);

                Some(db)
            },
            Err(error) => {
                println!("Got execution error: {error:?}");

                None
            },
        };

        return Ok(None.into());
    }

    let statement = parse_statement(input);

    if statement.is_none() {
        println!("Failed to parse: {input}")
    }

    let statement = statement.unwrap();

    let is_create_database = matches!(statement, Statement::Create { what: CreateType::Database, .. });
    let is_drop_database = matches!(statement, Statement::Drop { what: CreateType::Database, .. });

    let result = statement.execute(runtime.database.as_mut(), runtime.persistence_manager.as_ref()).await;

    let result = match result {
        Ok(execution_result) => {
            execution_result
        },
        Err(error) => {
            println!("Got execution error: {error:?}");

            // Don't persist storage if statement failed
            None.into()
        }
    };

    if is_create_database || is_drop_database {
        return Ok(None.into());
    }

    // TODO: doing this properly, should only write changed things
    // Also I can probably do better than the `is_drop_database` above
    match runtime.persistence_manager.save_database(runtime.database.as_ref().unwrap()).await {
        Ok(_) => (),
        Err(error) => println!("Failed saving to disk: {error:?}"),
    }

    return Ok(result);
}
