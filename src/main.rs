#![allow(clippy::needless_return)]

use std::io::Write;

use sql_parse::parse_statement;
use dbms::{Execute, RuntimeEnvironment};

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

        let statement = parse_statement(&input);

        if let Some(statement) = statement {
            let result = statement.execute(&mut environment);

            if let Err(error) = result {
                println!("Got execution error: {error:?}");
            }
        } else {
            println!("Failed to parse {input}");
        }
    }
}

fn main() {
    repl();
}
