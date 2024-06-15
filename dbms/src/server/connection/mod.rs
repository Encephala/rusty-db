#[cfg(test)]
mod tests;

use std::path::PathBuf;

use tokio::{
    io::BufReader,
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

use super::protocol::Packet;

struct Runtime {
    persistence_manager: Box<dyn PersistenceManager>,
    database: Option<Database>,
}

const SERIALISATION_MANAGER: SerialisationManager = SerialisationManager::new(Serialiser::V2);

pub async fn handle_connection(mut stream: TcpStream, mut shutdown_signal: Receiver<()>) -> Result<()> {
    let (reader, mut writer) = stream.split();

    let mut reader = BufReader::new(reader);

    // welcome_message().write(&mut writer).await?;

    let mut runtime = Runtime {
        persistence_manager: Box::new(FileSystem::new(
                SERIALISATION_MANAGER,
                PathBuf::from("/tmp/rusty-db")
            )),
        database: None,
    };

    loop {
        tokio::select! {
            // message = Message::read(&mut reader) => {
            //     let message = message?;

            //     // Stream closed
            //     if message.is_empty() {
            //         println!("Client {:?} disconnected", stream.peer_addr().unwrap());

            //         break;
            //     }

            //     // Handle message
            //     let _execution_result = process_statement(message, &mut runtime).await?;

            //     // TODO: Construct a proper response message and send it over
            // },
            _ = shutdown_signal.recv() => {
                // TODO: Inform client of shutdown
                break;
            }
        }
    }

    return Ok(());
}

// fn welcome_message() -> Message {
//     return Message::from(b"deez nuts".as_slice());
// }

async fn process_statement(buffer: Vec<u8>, runtime: &mut Runtime) -> Result<ExecutionResult> {
    let input = &String::from_utf8(buffer)
        .map_err(SqlError::NotAValidString)?;

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

    let statement = parse_statement(input);

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
