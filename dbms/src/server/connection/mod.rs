#[cfg(test)]
mod tests;

use std::path::PathBuf;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt}, net::TcpStream, sync::broadcast::Receiver
};

#[cfg(test)]
use crate::persistence::NoOp;

use crate::{
    evaluate::{
        Execute,
        ExecutionResult
    }, persistence::{
        FileSystem, PersistenceManager
    }, serialisation::{
        SerialisationManager,
        Serialiser
    }, types::DatabaseName, utils::serialiser_version_to_serialiser, Database, Result, SqlError
};

use sql_parse::{parse_statement, parser::Statement};

use super::protocol::{Message, MessageBody};

// Easiest way to make a type alias, `impl` isn't stable in type aliases
trait Stream: AsyncRead + AsyncWrite + std::marker::Unpin {}
impl Stream for TcpStream {}

#[derive(Debug)]
pub struct Runtime {
    persistence_manager: Box<dyn PersistenceManager>,
    database: Option<Database>,
}

#[cfg(test)]
impl Runtime {
    pub fn new_test() -> Self {
        return Self {
            persistence_manager: Box::new(NoOp),
            database: None,
        };
    }
}

impl Runtime {
    pub fn new(persistence_manager: impl PersistenceManager + 'static) -> Self {
        return Self {
            persistence_manager: Box::new(persistence_manager),
            database: None
        };
    }

    pub fn create_database(&mut self, database: Database) {
        self.database = Some(database);

        // TODO: Persistence?
    }

    pub fn get_database(&mut self) -> Option<&mut Database> {
        return self.database.as_mut();
    }

    pub fn clear_database(&mut self) -> Result<DatabaseName> {
        if self.database.is_none() {
            return Err(SqlError::NoDatabaseSelected);
        }

        let name = self.database.as_ref().unwrap().name.clone();

        self.database = None;

        return Ok(name);
    }

    // I think these two methods make sense?
    pub async fn save(&mut self) -> Result<()> {
        if let Some(database) = &self.database {
            return self.persistence_manager.save_database(database).await;
        } else {
            return Err(SqlError::NoDatabaseSelected);
        }
    }

    pub async fn load(&mut self, database_name: &DatabaseName) -> Result<()> {
        let result = self.persistence_manager.load_database(database_name).await?;

        self.database = Some(result);

        return Ok(());
    }

    pub async fn drop(&mut self) -> Result<()> {
        if let Some(database) = &self.database {
            self.persistence_manager.drop_database(&database.name).await?;

            // Note: Only clears own database if dropping succeeded
            self.database = None;

            return Ok(());
        } else {
            return Err(SqlError::NoDatabaseSelected);
        }
    }
}


pub struct Connection {
    stream: TcpStream,
    shutdown_receiver: Receiver<()>,
    context: Context,
}

#[derive(Debug)]
pub struct Context {
    serialiser: Serialiser,
    runtime: Runtime,
}

impl Connection {
    pub async fn new(mut stream: TcpStream, shutdown_receiver: Receiver<()>) -> Result<Self> {
        let context = Connection::setup_context(&mut stream).await?;

        return Ok(Connection {
            stream,
            shutdown_receiver,
            context,
        });
    }

    /// Negotiates connection parameters.
    ///
    /// Returns a [`Context`] object populated with these parameters
    /// as well as other (default) parameters.
    // I don't quite like this function name
    async fn setup_context(stream: &mut impl Stream) -> Result<Context> {
        let serialiser = Connection::negotiate_serialiser_version(stream).await?;

        let runtime = Runtime {
            persistence_manager: Box::new(FileSystem::new(
                    SerialisationManager(serialiser),
                    PathBuf::from("/tmp/rusty-db"),
                )),
            database: None,
        };

        return Ok(Context {
            serialiser,
            runtime,
        });
    }

    async fn negotiate_serialiser_version(stream: &mut impl Stream) -> Result<Serialiser> {
        let available_serialiser_versions = [1, 2];

        stream.write_all((available_serialiser_versions.len() as u8).to_le_bytes().as_slice()).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        stream.write_all(available_serialiser_versions.as_slice()).await
            .map_err(SqlError::CouldNotReadFromConnection)?;


        let mut serialiser_version_buffer = [0_u8];

        stream.read_exact(&mut serialiser_version_buffer).await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let [decided_version] = serialiser_version_buffer;

        let serialiser = serialiser_version_to_serialiser(decided_version)?;

        return Ok(serialiser);
    }

    pub async fn handle(mut self) -> Result<()> {
        loop {
            tokio::select! {
                message = Message::read(&mut self.stream, self.context.serialiser) => {
                    // This breaks out of the loop
                    let message = message?;

                    // Handle message
                    if let MessageBody::Str(input) = message.body {
                        let _execution_result = process_input(&input, &mut self.context.runtime).await?;
                    };

                    // TODO: Construct a proper response message and send it over
                },
                _ = self.shutdown_receiver.recv() => {
                    // TODO: Inform client of shutdown
                    break;
                }
            }
        }

        return Ok(());
    }
}

async fn process_input(input: &str, runtime: &mut Runtime) -> Result<ExecutionResult> {
    println!("Executing: {input}");

    if input.starts_with('\\') {
        return handle_special_commands(input, runtime).await;
    }

    let statement = parse_input(input)?;

    return statement.execute(runtime).await;
}

async fn handle_special_commands(input: &str, runtime: &mut Runtime) -> Result<ExecutionResult> {
    if let Some(database_name) = input.strip_prefix("\\c ") {
        let database = runtime.persistence_manager.load_database(&DatabaseName(database_name.into())).await?;

        runtime.database = Some(database);

        return Ok(ExecutionResult::None);
    }

    return Err(SqlError::InvalidCommand(input[1..].to_string()));
}

fn parse_input(input: &str) -> Result<Statement> {
    let statement = parse_statement(input);

    if statement.is_none() {
        println!("Failed to parse: {input}");

        return Err(SqlError::ParseError)
    }

    return Ok(statement.unwrap());
}
