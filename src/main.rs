#![allow(clippy::needless_return)]

use std::io::Write;

use sql_parse::{Lexer, parse_statement};
use dbms::{Execute, RuntimeEnvironment, ExecutionResult};

fn repl() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    let mut environment = RuntimeEnvironment::new();

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

        if input.starts_with("\\l") {
            let tokens = Lexer::lex(input.strip_prefix("\\l").unwrap());

            println!("Lexed: {tokens:?}");

            continue;
        }

        let statement = parse_statement(&input);

        if input.starts_with("\\p") {
            let statement = parse_statement(&input);

            println!("Parsed: {statement:?}");

            continue;
        }

        if let Some(statement) = statement {
            let result = statement.execute(&mut environment);

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
