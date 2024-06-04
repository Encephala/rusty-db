use super::combinators::Chain;
use super::expressions::{AllColumn, Array, Expression, ExpressionParser, Identifier, Where, Value};
use super::utils::check_and_skip;
use crate::lexer::Token;

#[derive(Debug, PartialEq)]
pub enum Statement {
    Select {
        columns: Expression,
        table: Expression,
        where_clause: Option<Expression>,
    },
    Create {
        what: CreateType,
        name: Expression,
    },
    Insert {
        into: Expression,
        values: Expression, // Expression::Array
    },
    Update {
        from: Expression,
        columns: Expression,
        values: Expression,
        where_clause: Option<Expression>,
    },
    Delete {
        from: Expression,
        where_clause: Option<Expression>,
    },
    Drop {
        what: CreateType,
        name: Expression,
    },
}

#[derive(Debug, PartialEq)]
pub enum CreateType {
    Database,
    Table,
}

pub trait StatementParser {
    fn parse(&self, input: &[Token]) -> Option<Statement>;
}


pub struct Select;
impl StatementParser for Select {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Select)?;

        let columns = Identifier.multiple().or(AllColumn).parse(input)?;

        check_and_skip(input, Token::From)?;

        let table = Identifier.parse(input)?;

        let where_clause = Where.parse(input);

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Select {
            columns,
            table,
            where_clause,
        });
    }
}


fn parse_table_or_database(input: &mut &[Token]) -> Option<CreateType> {
        let which = match input.get(0)? {
            Token::Table => Some(CreateType::Table),
            Token::Database => Some(CreateType::Database),
            _ => None,
        }?;

        *input = &input[1..];

        return Some(which);
}

pub struct Create;
impl StatementParser for Create {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Create)?;

        let what = parse_table_or_database(input)?;

        let name = Identifier.parse(input)?;

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Create {
            what,
            name
        });
    }
}


pub struct Insert;
impl StatementParser for Insert {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Insert)?;

        check_and_skip(input, Token::Into)?;

        let into = Identifier.parse(input)?;

        check_and_skip(input, Token::Values)?;

        let values = Array.parse(input)?;

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Insert {
           into,
           values
        });
    }
}


pub struct Update;
impl StatementParser for Update {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Update)?;

        let from = Identifier.parse(input)?;

        check_and_skip(input, Token::Set)?;

        #[derive(Debug)]
        struct ColumnValuePair;
        impl ExpressionParser for ColumnValuePair {
            fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
                let column = Identifier.parse(input)?;

                check_and_skip(input, Token::Equals)?;

                let value = Value.parse(input)?;

                return Some(Expression::ColumnValuePair {
                    column: column.into(),
                    value: value.into(),
                });
            }
        }

        let pairs = ColumnValuePair.multiple().parse(input)?;

        let mut columns = vec![];
        let mut values = vec![];

        // Collect pairs into separate vectors
        // Will always match
        if let Expression::Array(pairs) = pairs {
            (columns, values) = pairs.into_iter().map(destructure_column_value_pair).unzip();
        }

        let where_clause = Where.parse(input);

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Update {
            from,
            columns: Expression::Array(columns),
            values: Expression::Array(values),
            where_clause,
        });
    }
}

fn destructure_column_value_pair(pair: Expression) -> (Expression, Expression) {
    // Will always pass
    if let Expression::ColumnValuePair { column, value } = pair {
        return (*column, *value);
    }

    panic!("split_column_value_pairs called with something other than a ColumnValuePair");
}


pub struct Delete;
impl StatementParser for Delete {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Delete)?;

        check_and_skip(input, Token::From)?;

        let from = Identifier.parse(input)?;

        let where_clause = Where.parse(input);

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Delete {
            from,
            where_clause,
        });
    }
}


#[derive(Debug)]
pub struct Drop;
impl StatementParser for Drop {
    fn parse(&self, mut input: &[Token]) -> Option<Statement> {
        let input = &mut input;

        check_and_skip(input, Token::Drop)?;

        let what = parse_table_or_database(input)?;

        let name = Identifier.parse(input)?;

        check_and_skip(input, Token::Semicolon)?;

        return Some(Statement::Drop {
            what,
            name,
        })
    }
}


#[cfg(test)]
mod tests {
    use crate::lexer::Lexer;
    use super::super::expressions::InfixOperator;
    use super::*;

    use {Expression as E, Statement as S};


    pub fn test_all_cases(parser: impl StatementParser, inputs: &[(&str, Option<Statement>)]) {
        inputs.iter().for_each(|test_case| {
            let result = parser.parse(Lexer::lex(test_case.0).as_slice());

            assert_eq!(result, test_case.1);
        });
    }


    #[test]
    fn select_basic() {
        let inputs = [
            ("SELECT bla from asdf;", S::Select {
                columns: E::Array(vec![E::Ident("bla".into())]),
                table: E::Ident("asdf".into()),
                where_clause: None,
            }),
            ("SELECT * from asdf;", S::Select {
                columns: E::AllColumns,
                table: E::Ident("asdf".into()),
                where_clause: None,
            }),
        ];

        inputs.into_iter().for_each(|test_case| {
            let result = Select.parse(&Lexer::lex(test_case.0));

            assert_eq!(result, Some(test_case.1));
        });
    }

    #[test]
    fn select_with_where() {
        let input = "SELECT bla FROM asdf WHERE a > b;";

        let result = Select.parse(Lexer::lex(input).as_slice());

        assert_eq!(
            result,
            Some(S::Select {
                columns: E::Array(vec![E::Ident("bla".into())]),
                table: E::Ident("asdf".into()),
                where_clause: Some(E::Where {
                    left: E::Ident("a".into()).into(),
                    operator: InfixOperator::GreaterThan,
                    right: E::Ident("b".into()).into(),
                })
            })
        )
    }

    #[test]
    fn create_basic() {
        let inputs = [
            ("CREATE DATABASE epic_db;", Some(S::Create {
                what: CreateType::Database,
                name: E::Ident("epic_db".into()),
            })),
            ("CREATE TABLE name;", Some(S::Create{
                what: CreateType::Table,
                name: E::Ident("name".into()),
            })),
            ("CREATE TABLE blabla, blabla;", None),
            ("CREATE TABLE oops_no_semicolon", None),
            ("CREATE blabla;", None),
            ("CREATE TABLE 123", None),
        ];

        test_all_cases(Create, &inputs);
    }

    #[test]
    fn insert_basic() {
        let inputs = [
            ("INSERT INTO bla VALUES (1, 'hey', 420.69);", Some(S::Insert {
                into: E::Ident("bla".into()),
                values: E::Array(vec![
                    E::Int(1),
                    E::Str("hey".into()),
                    E::Decimal(420, 69),
                ])
            })),
            ("INSERT INTO bla VALUES (1, 'hey', 420.69);", Some(S::Insert {
                into: E::Ident("bla".into()),
                values: E::Array(vec![
                    E::Int(1),
                    E::Str("hey".into()),
                    E::Decimal(420, 69),
                ])
            })),
            // Can't forget semicolon
            ("INSERT INTO bla VALUES ()", None),
            // Can't forget `INTO`
            ("INSERT bla VALUES ();", None),
        ];

        test_all_cases(Insert, &inputs);
    }

    #[test]
    fn update_basic() {
        let inputs = [
            ("UPDATE tbl SET col = 1;", Some(S::Update {
                from: E::Ident("tbl".into()),
                columns: E::Array(vec![E::Ident("col".into())]),
                values: E::Array(vec![E::Int(1)]),
                where_clause: None,
            })),
            ("update tbl set col = 1 where other = 2;", Some(S::Update {
                from: E::Ident("tbl".into()),
                columns: E::Array(vec![E::Ident("col".into())]),
                values: E::Array(vec![E::Int(1)]),
                where_clause: Some(E::Where {
                    left: E::Ident("other".into()).into(),
                    operator: InfixOperator::Equals,
                    right: E::Int(2).into(),
                }),
            })),
            ("UPDATE tbl set col1 = 1, col2 = 'value';", Some(S::Update {
                from: E::Ident("tbl".into()),
                columns: E::Array(vec![
                    E::Ident("col1".into()),
                    E::Ident("col2".into()),
                ]),
                values: E::Array(vec![
                    E::Int(1),
                    E::Str("value".into()),
                ]),
                where_clause: None,
            })),
            // Must end in semicolon
            ("UPDATE tbl set col1 = 1, col2 = 'value'", None),
        ];

        test_all_cases(Update, &inputs);
    }

    #[test]
    fn delete_basic() {
        let inputs = [
            ("DELETE FROM tbl;", Some(S::Delete {
                from: E::Ident("tbl".into()),
                where_clause: None,
            })),
            ("DELETE FROM tbl WHERE col = 1;", Some(S::Delete {
                from: E::Ident("tbl".into()),
                where_clause: Some(E::Where {
                    left: E::Ident("col".into()).into(),
                    operator: InfixOperator::Equals,
                    right: E::Int(1).into(),
                }),
            })),
            // Must end in semicolon
            ("DELETE FROM tbl", None),
        ];

        test_all_cases(Delete, &inputs);
    }

    #[test]
    fn drop_basic() {
        let inputs = [
            ("DROP TABLE tbl;", Some(S::Drop {
                what: CreateType::Table,
                name: E::Ident("tbl".into()),
            })),
            ("DROP DATABASE db;", Some(S::Drop {
                what: CreateType::Database,
                name: E::Ident("db".into()),
            })),
            // Must end in semicolon
            ("DROP TABLE tbl", None),
        ];

        test_all_cases(Drop, &inputs);
    }
}
