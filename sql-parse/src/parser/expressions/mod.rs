#[cfg(test)]
mod tests;

use super::combinators::Chain;
use super::utils::check_and_skip;
use crate::lexer::Token;

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum Expression {
    Type(ColumnType),
    ColumnDefinition(String, ColumnType),
    ForeignKeyConstraint {
        column: Box<Expression>,
        foreign_table: Box<Expression>,
        foreign_column: Box<Expression>,
    },
    AllColumns,
    Ident(String),
    Int(usize),
    Decimal(usize, usize),
    Str(String),
    Bool(bool),
    Where {
        left: Box<Expression>,
        operator: InfixOperator,
        right: Box<Expression>,
    },
    Array(Vec<Expression>),
    ColumnValuePair {
        column: Box<Expression>,
        value: Box<Expression>,
    },
}
use Expression as E;

#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq))]
pub enum InfixOperator {
    Equals,
    NotEqual,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ColumnType {
    Int,
    Decimal,
    Text,
    Bool,
}

impl InfixOperator {
    fn parse(input: &mut &[Token]) -> Option<Self> {
        use InfixOperator as I;

        let operator = match input.first()? {
            Token::Equals => Some(I::Equals),
            Token::NotEquals => Some(I::NotEqual),
            Token::LessThan => Some(I::LessThan),
            Token::LessThanEqual => Some(I::LessThanEqual),
            Token::GreaterThan => Some(I::GreaterThan),
            Token::GreaterThanEqual => Some(I::GreaterThanEqual),
            _ => None,
        };

        if let Some(operator) = operator {
            *input = &input[1..];

            return Some(operator);
        }

        return None;
    }
}

pub trait ExpressionParser: std::fmt::Debug {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression>;
}

#[derive(Debug)]
pub struct Int;
impl ExpressionParser for Int {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Token::Int(value) = input.first()? {
            *input = &input[1..];

            return Some(E::Int(*value));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct Decimal;
impl ExpressionParser for Decimal {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Token::Decimal(whole, fractional) = input.first()? {
            *input = &input[1..];

            return Some(E::Decimal(*whole, *fractional));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct Number;
impl ExpressionParser for Number {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        return Int.or(Decimal).parse(input);
    }
}

#[derive(Debug)]
pub struct Str;
impl ExpressionParser for Str {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        if let Token::Str(value) = input.first()? {
            *input = &input[1..];

            return Some(E::Str(value.clone()));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct Bool;
impl ExpressionParser for Bool {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Token::Bool(value) = input.first()? {
            *input = &input[1..];

            return Some(E::Bool(*value));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct Type;
impl ExpressionParser for Type {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        let result = match input.first()? {
            Token::TypeInt => Some(E::Type(ColumnType::Int)),
            Token::TypeDecimal => Some(E::Type(ColumnType::Decimal)),
            Token::TypeBool => Some(E::Type(ColumnType::Bool)),
            Token::TypeText => Some(E::Type(ColumnType::Text)),
            _ => None,
        };

        if result.is_some() {
            *input = &input[1..];
        }

        return result;
    }
}

#[derive(Debug)]
pub struct Identifier;
impl ExpressionParser for Identifier {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        if let Token::Ident(name) = input.first()? {
            *input = &input[1..];

            return Some(E::Ident(name.clone()));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct ForeignKeyConstraint;
impl ExpressionParser for ForeignKeyConstraint {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        check_and_skip(input, Token::Foreign)?;

        check_and_skip(input, Token::Key)?;

        check_and_skip(input, Token::LParenthesis)?;

        let own_column = Identifier.parse(input)?;

        check_and_skip(input, Token::RParenthesis)?;

        check_and_skip(input, Token::References)?;

        let table_name = Identifier.parse(input)?;

        check_and_skip(input, Token::LParenthesis)?;

        let column = Identifier.parse(input)?;

        check_and_skip(input, Token::RParenthesis)?;

        return Some(E::ForeignKeyConstraint {
            column: Box::new(own_column),
            foreign_table: Box::new(table_name),
            foreign_column: Box::new(column),
        });
    }
}

#[derive(Debug)]
pub struct ColumnDefinition;
impl ExpressionParser for ColumnDefinition {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        let name = Identifier.parse(input);

        if let Some(name) = name {
            let column_type = Type.parse(input)?;

            if let (E::Ident(name), E::Type(column_type)) = (name, column_type) {
                return Some(E::ColumnDefinition(name, column_type));
            } else {
                panic!("Return types got all messed up")
            }
        }

        return ForeignKeyConstraint.parse(input);
    }
}

#[derive(Debug)]
pub struct AllColumn;
impl ExpressionParser for AllColumn {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::Asterisk)?;

        return Some(E::AllColumns);
    }
}

#[derive(Debug)]
pub struct Column;
impl ExpressionParser for Column {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        return Identifier.or(AllColumn).multiple().parse(input);
    }
}

#[derive(Debug)]
pub struct Where;
// TODO: This doesn't differentiate between a failed parsing of `WHERE` clause,
// and the absence of a `WHERE` clause.
// Again, have to move to Result<Option> or Option<Result>
// That's gonna make me sad about not being able to use ? everywhere though :(
impl ExpressionParser for Where {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::Where)?;

        let parser = Identifier.or(Value);

        let left = parser.parse(input)?.into();

        let operator = InfixOperator::parse(input)?;

        let right = parser.parse(input)?.into();

        return Some(E::Where {
            left,
            operator,
            right,
        });
    }
}

#[derive(Debug)]
pub struct Value;
impl ExpressionParser for Value {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        return Str.or(Number).or(Bool).parse(input);
    }
}

#[derive(Debug)]
pub struct Array;
impl ExpressionParser for Array {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::LParenthesis)?;

        // TODO: Make this parse any expression rather than hardcoded `Value`
        let expressions = Value.multiple().parse(input)?;

        check_and_skip(input, Token::RParenthesis)?;

        return Some(expressions);
    }
}
