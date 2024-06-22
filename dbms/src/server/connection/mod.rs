#[cfg(test)]
mod tests;

use std::{net::SocketAddr, path::PathBuf};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::broadcast::Receiver
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

use sql_parse::{parse_statement, parser::{CreateType, Statement}};

use super::protocol::{Message, MessageBody};

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

    pub fn drop_database(&mut self) -> Result<DatabaseName> {
        if self.database.is_none() {
            return Err(SqlError::NoDatabaseSelected);
        }

        let name = self.database.as_ref().unwrap().name.clone();

        self.database = None;

        // TODO: Persistence

        return Ok(name);
    }

    // I think these two methods sense?
    pub async fn persist(&mut self) -> Result<()> {
        todo!();
    }

    pub async fn load_persisted(&mut self, _database_name: DatabaseName) -> Result<&DatabaseName> {
        todo!();
    }
}


pub struct Connection {
    stream: TcpStream,
    shutdown_receiver: Receiver<()>,
    context: Context,
}

#[derive(Debug)]
pub struct Context {
    peer_address: SocketAddr,
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
    async fn setup_context(stream: &mut TcpStream) -> Result<Context> {
        let peer_address = stream.peer_addr()
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let serialiser = Connection::negotiate_serialiser_version(stream).await?;

        let runtime = Runtime {
            persistence_manager: Box::new(FileSystem::new(
                    SerialisationManager(serialiser),
                    PathBuf::from("/tmp/rusty-db"),
                )),
            database: None,
        };

        return Ok(Context {
            peer_address,
            serialiser,
            runtime,
        });
    }

    async fn negotiate_serialiser_version(stream: &mut TcpStream) -> Result<Serialiser> {
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
                    if let MessageBody::Str(statement) = message.body {
                        let _execution_result = process_statement(statement, &mut self.context.runtime).await?;
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

async fn process_statement(input: String, runtime: &mut Runtime) -> Result<ExecutionResult> {
    println!("Executing: {input}");

    if input.starts_with("\\c ") {
        let database_name = input.strip_prefix("\\c ").unwrap();

        runtime.database = match runtime.persistence_manager.load_database(&DatabaseName(database_name.into())).await {
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

    let statement = parse_statement(&input);

    if statement.is_none() {
        println!("Failed to parse: {input}");
        return Ok(None.into());
    }

    let statement = statement.unwrap();

    let is_create_database = matches!(statement, Statement::Create { what: CreateType::Database, .. });
    let is_drop_database = matches!(statement, Statement::Drop { what: CreateType::Database, .. });

    let result = statement.execute(runtime).await;

    let result = match result {
        Ok(execution_result) => {
            execution_result
        },
        Err(error) => {
            println!("Got execution error: {error:?}");

            // Don't persist storage if statement failed, so early return
            return Ok(None.into());
        }
    };

    // If it was one of these two statements, we currently don't have a valid DB selected,
    // so skip persistence
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
