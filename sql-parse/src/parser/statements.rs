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
