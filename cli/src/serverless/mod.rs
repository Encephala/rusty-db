use std::io::Write;
use std::path::PathBuf;

use sql_parse::{
    lexer::Lexer,
    parse_statement,
    parser::{Statement, CreateType}
};
use dbms::{
    evaluate::{Execute, ExecutionResult}, persistence::FileSystem, serialisation::{SerialisationManager, Serialiser}, server::Runtime, types::DatabaseName
};

pub async fn _repl() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    let persistence_manager = FileSystem::new(
        SerialisationManager::new(Serialiser::V2),
        PathBuf::from("/tmp/rusty-db"),
    );

    let mut runtime = Runtime::new(persistence_manager);

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

            match runtime.load_persisted(DatabaseName(database_name.into())).await {
                Ok(name) => {
                    println!("Connected to database {}", name.0);
                },
                Err(error) => {
                    println!("Got execution error: {error:?}");
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

            let result = statement.execute(&mut runtime).await;

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
            match runtime.persist().await {
                Ok(_) => (),
                Err(error) => println!("Failed saving to disk: {error:?}"),
            }
        } else {
            println!("Failed to parse: {input}");
        }
    }
}
