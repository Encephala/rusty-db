#![allow(clippy::needless_return)]

use std::io::Write;
use std::path::PathBuf;

use sql_parse::{Lexer, parse_statement};
use dbms::{Execute, Database, DatabaseName, ExecutionResult, PersistenceManager, FileSystem};

fn repl() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    let mut database: Option<Database> = None;

    let persistence_manager = Box::new(FileSystem::new(PathBuf::from("/tmp/rusty-db")));

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

        if input.starts_with("\\c ") {
            let database_name = input.strip_prefix("\\c ").unwrap().strip_suffix('\n').unwrap();

            database = match persistence_manager.load_database(DatabaseName(database_name.into())) {
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

        if let Some(statement) = statement {
            let result = statement.execute(database.as_mut(), persistence_manager.as_ref());

            match persistence_manager.save_database(database.as_ref().unwrap()) {
                Ok(_) => (),
                Err(error) => println!("Failed saving to disk: {error:?}"),
            }

            match result {
                Ok(result) => {
                    match result {
                        ExecutionResult::None => (),
                        an_actual_result => println!("Executed:\n{an_actual_result:?}"),
                    }
                },
                Err(error) => {
                    println!("Got execution error: {error:?}");
                }
            }
        } else {
            println!("Failed to parse: {input}");
        }
    }
}

fn main() {
    repl();
}
