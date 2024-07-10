#[cfg(test)]
mod tests;

use std::path::PathBuf;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast::Receiver,
};

#[cfg(test)]
use crate::persistence::NoOp;

use crate::{
    evaluate::{Execute, ExecutionResult},
    persistence::{FileSystem, PersistenceManager},
    serialisation::{SerialisationManager, Serialiser},
    types::DatabaseName,
    utils::serialiser_version_to_serialiser,
    Database, Result, SqlError,
};

use sql_parse::parse_statement;

use super::{
    protocol::{Command, Message, MessageBody},
    Stream,
};

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
            database: None,
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
        let result = self
            .persistence_manager
            .load_database(database_name)
            .await?;

        self.database = Some(result);

        return Ok(());
    }

    // TODO: We have to be able to drop a database that we're not connected to right now,
    // but then uhh
    // (besides consistency and isolation and stuff)
    // what interface makes sense? Just naming it `drop` doesn't make sense,
    // also this function checks and clears self.database and that's not necessary in that case.
    // When I wrote this code, I wasn't thinking about how it would be used.
    pub async fn drop(&mut self) -> Result<()> {
        if let Some(database) = &self.database {
            self.persistence_manager
                .drop_database(&database.name)
                .await?;

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
        let serialiser: Serialiser = Connection::negotiate_serialiser_version(stream).await?;

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

        stream
            .write_all(
                (available_serialiser_versions.len() as u8)
                    .to_le_bytes()
                    .as_slice(),
            )
            .await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        stream
            .write_all(available_serialiser_versions.as_slice())
            .await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let mut serialiser_version_buffer = [0_u8];

        stream
            .read_exact(&mut serialiser_version_buffer)
            .await
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let [decided_version] = serialiser_version_buffer;

        let serialiser = serialiser_version_to_serialiser(decided_version)?;

        return Ok(serialiser);
    }

    pub async fn handle(mut self) -> Result<()> {
        loop {
            tokio::select! {
                // TODO: This probably shouldn't be sermanager(self.context.serialiser), put sermanager somewhere?
                // Persistence manager also uses it
                message = Message::read(&mut self.stream, SerialisationManager(self.context.serialiser)) => {
                    // This breaks out of the loop
                    let message = message?;

                    // Handle message
                    let result = match message.body {
                        MessageBody::Close => break,
                        MessageBody::Ok => Ok(ExecutionResult::None),
                        MessageBody::Str(statement) => handle_statement(&statement, &mut self.context.runtime).await,
                        MessageBody::Command(command) => handle_special_commands(command, &mut self.context.runtime).await,
                        MessageBody::Error(error) => {
                            println!("ERROR: {error:?}");

                            Ok(ExecutionResult::None)
                        },
                        MessageBody::RowSet(rowset) => {
                            println!("Server received a rowset? What does that mean ({rowset:?})");

                            Ok(ExecutionResult::None)
                        },
                    };

                    // TODO: respond
                    // Have to convert an ExecutionResult into a MessageBody

                    // For now, just debug printing as message and yeeting it over hell yeah
                    let response = Message::from_message_body(MessageBody::Str(format!("{result:?}")));

                    response.write(&mut self.stream, SerialisationManager(self.context.serialiser)).await?;
                },
                _ = self.shutdown_receiver.recv() => {
                    let message = Message::from_message_body(MessageBody::Close);

                    message.write(&mut self.stream, SerialisationManager(self.context.serialiser)).await?;

                    break;
                }
            }
        }

        return Ok(());
    }
}

async fn handle_special_commands(
    command: Command,
    runtime: &mut Runtime,
) -> Result<ExecutionResult> {
    match command {
        Command::Connect(database_name) => {
            runtime.load(&database_name).await?;

            return Ok(ExecutionResult::None);
        }
        Command::ListDatabases => {
            todo!();
        }
        Command::ListTables => {
            let database = runtime
                .database
                .as_ref()
                .ok_or(SqlError::NoDatabaseSelected)?;

            let names = database.tables.keys().cloned().collect();

            return Ok(ExecutionResult::ListTables(names));
        }
    }
}

async fn handle_statement(input: &str, runtime: &mut Runtime) -> Result<ExecutionResult> {
    let statement = parse_statement(input);

    if statement.is_none() {
        println!("Failed to parse: {input}");

        return Err(SqlError::ParseError);
    }

    return statement.unwrap().execute(runtime).await;
}
