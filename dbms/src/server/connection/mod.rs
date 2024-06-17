#[cfg(test)]
mod tests;

use std::{net::SocketAddr, path::PathBuf};

use tokio::{
    net::TcpStream, sync::broadcast::Receiver
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

use super::protocol::{Message, MessageBody};

struct Runtime {
    persistence_manager: Box<dyn PersistenceManager>,
    database: Option<Database>,
}

pub struct Connection {
    stream: TcpStream,
    shutdown_receiver: Receiver<()>,
    context: Context,
}

pub struct Context {
    peer_address: SocketAddr,
    serialiser: Serialiser,
    runtime: Runtime,
}

impl Connection {
    pub async fn new(mut stream: TcpStream, shutdown_receiver: Receiver<()>) -> Result<Self> {
        // TODO: Negotiate connection parameters
        let context = Connection::negotiate_parameters(&mut stream).await?;

        return Ok(Connection {
            stream,
            shutdown_receiver,
            context,
        });
    }

    async fn negotiate_parameters(stream: &mut TcpStream) -> Result<Context> {
        let peer_address = stream.peer_addr()
            .map_err(SqlError::CouldNotReadFromConnection)?;

        let serialiser = todo!();

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

    pub async fn handle(mut self) -> Result<()> {
        println!("Handling connection in {:?}", std::thread::current());

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

    let statement = parse_statement(&input);

    if statement.is_none() {
        println!("Failed to parse: {input}");
        return Ok(None.into());
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
