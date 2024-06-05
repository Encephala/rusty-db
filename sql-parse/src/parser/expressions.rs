use super::combinators::Chain;
use super::utils::check_and_skip;
use crate::lexer::Token;

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Type(ColumnType),
    AllColumns,
    Ident(String),
    Int(usize),
    Decimal(usize, usize),
    Str(String),
    Bool(bool),
    Where { left: Box<Expression>, operator: InfixOperator, right: Box<Expression> },
    Array(Vec<Expression>),
    ColumnValuePair { column: Box<Expression>, value: Box<Expression> },
}
use Expression as E;


#[derive(Debug, PartialEq, Clone, Copy)]
pub enum InfixOperator {
    Equals,
    NotEqual,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
}

#[derive(Debug, PartialEq, Clone, Copy)]
// TODO: VarChar would be cool but idk how to handle that
pub enum ColumnType {
    Int,
    Decimal,
    Text,
    Bool,
}

impl InfixOperator {
    fn parse(input: &mut &[Token]) -> Option<Self> {
        use InfixOperator as I;

        let operator = match input.get(0)? {
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
pub struct IntLiteral;
impl ExpressionParser for IntLiteral {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Token::Int(value) = input.get(0)? {
            *input = &input[1..];

            return Some(E::Int(*value));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct DecimalLiteral;
impl ExpressionParser for DecimalLiteral {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Token::Decimal(whole, fractional) = input.get(0)? {
            *input = &input[1..];

            return Some(E::Decimal(*whole, *fractional));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct NumberLiteral;
impl ExpressionParser for NumberLiteral {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        return IntLiteral.or(DecimalLiteral).parse(input);
    }
}

#[derive(Debug)]
pub struct StrLiteral;
impl ExpressionParser for StrLiteral {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        if let Token::Str(value) = input.get(0)? {
            *input = &input[1..];

            return Some(E::Str(value.clone()));
        }

        return None;
    }
}


#[derive(Debug)]
pub struct BoolLiteral;
impl ExpressionParser for BoolLiteral {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Token::Bool(value) = input.get(0)? {
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
        let result = match input.get(0)? {
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
        if let Token::Ident(name) = input.get(0)? {
            *input = &input[1..];

            return Some(E::Ident(name.clone()));
        }

        return None;
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

        let parser = Identifier.or(NumberLiteral);

        let left = parser.parse(input)?.into();

        let operator = InfixOperator::parse(input)?;

        let right = parser.parse(input)?.into();

        return Some(E::Where { left, operator, right });
    }
}


#[derive(Debug)]
pub struct Value;
impl ExpressionParser for Value {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        return StrLiteral.or(NumberLiteral).parse(input);
    }
}


#[derive(Debug)]
pub struct Array;
impl ExpressionParser for Array {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::LParenthesis)?;

        // TODO: Make this parse any expression rather than hardcoded str or number
        let expressions = Value.multiple().parse(input)?;

        check_and_skip(input, Token::RParenthesis)?;

        return Some(expressions);
    }
}
