#![allow(clippy::needless_return)]

use std::io::Write;
use std::path::PathBuf;

use sql_parse::{Lexer, parse_statement, Statement, CreateType};
use dbms::{Execute, Database, DatabaseName, ExecutionResult, PersistenceManager, FileSystem, SerialisationManager, Serialiser};

async fn repl() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    let mut database: Option<Database> = None;

    let persistence_manager: Box<_> = FileSystem::new(
        SerialisationManager::new(Serialiser::V2),
        PathBuf::from("/tmp/rusty-db"),
    ).into();

    loop {
        print!(">> ");

        stdout.flush().unwrap();

        let mut input = String::new();

        stdin.read_line(&mut input).unwrap();

        if input == "\\q\n" {
            break;
        } else if input.is_empty() {
            println!();
            break;
        }

        // TODO: Standardise handling these special commands
        if input.starts_with("\\c ") {
            let database_name = input.strip_prefix("\\c ").unwrap().strip_suffix('\n').unwrap();

            database = match persistence_manager.load_database(DatabaseName(database_name.into())).await {
                Ok(db) => {
                    println!("Connected to database {}", db.name.0);

                    Some(db)
                },
                Err(error) => {
                    println!("Got execution error: {error:?}");

                    None
                },
            };

            continue;
        }

        if input.starts_with("\\l ") {
            let tokens = Lexer::lex(input.strip_prefix("\\l ").unwrap());

            println!("Lexed: {tokens:?}");

            continue;
        }

        let statement = parse_statement(&input);

        if input.starts_with("\\p ") {
            let statement = parse_statement(input.strip_prefix("\\p ").unwrap());

            println!("Parsed: {statement:?}");

            continue;
        }

        if let Some(statement) = statement {
            let is_create_database = matches!(statement, Statement::Create { what: CreateType::Database, .. });
            let is_drop_database = matches!(statement, Statement::Drop { what: CreateType::Database, .. });

            let result = statement.execute(database.as_mut(), persistence_manager.as_ref()).await;

            match result {
                Ok(result) => {
                    match result {
                        ExecutionResult::None => (),
                        an_actual_result => println!("Executed:\n{an_actual_result:?}"),
                    }
                },
                Err(error) => {
                    println!("Got execution error: {error:?}");

                    // Don't persist storage if statement failed
                    continue;
                }
            }

            if is_create_database || is_drop_database {
                continue;
            }

            // TODO: doing this properly, should only write changed things
            // Also I can probably do better than the `is_drop_database` above
            match persistence_manager.save_database(database.as_ref().unwrap()).await {
                Ok(_) => (),
                Err(error) => println!("Failed saving to disk: {error:?}"),
            }
        } else {
            println!("Failed to parse: {input}");
        }
    }
}

#[tokio::main]
async fn main() {
    repl().await;
}
