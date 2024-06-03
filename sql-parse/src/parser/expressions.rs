use super::combinators::Chain;
use super::utils::check_and_skip;
use crate::lexer::Token;

#[derive(Debug, PartialEq)]
pub enum Expression {
    AllColumns,
    Ident(String),
    Int(usize),
    Decimal(usize, usize),
    Str(String),
    Where { left: Box<Expression>, operator: InfixOperator, right: Box<Expression> },
    Array(Vec<Expression>),
}
use Expression as E;


#[derive(Debug, PartialEq)]
pub enum InfixOperator {
    Equals,
    NotEqual,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
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
pub struct Number;
impl ExpressionParser for Number {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        if let Some(Token::Int(value)) = input.get(0) {
            *input = &input[1..];

            return Some(E::Int(*value));
        }

        if let Some(Token::Decimal(whole, fractional)) = input.get(0) {
            *input = &input[1..];

            return Some(E::Decimal(*whole, *fractional));
        }

        return None;
    }
}

#[derive(Debug)]
pub struct Str;
impl ExpressionParser for Str {
    fn parse(&self, input: &mut &[Token]) -> Option<E> {
        if let Some(Token::Str(value)) = input.get(0) {
            *input = &input[1..];

            return Some(E::Str(value.clone()));
        }

        return None;
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
impl ExpressionParser for Where {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::Where)?;

        let parser = Identifier.or(Number);

        let left = parser.parse(input)?.into();

        let operator = InfixOperator::parse(input)?;

        let right = parser.parse(input)?.into();

        return Some(E::Where { left, operator, right });
    }
}


#[derive(Debug)]
pub struct Array;
impl ExpressionParser for Array {
    fn parse(&self, input: &mut &[Token]) -> Option<Expression> {
        check_and_skip(input, Token::LParenthesis)?;

        // TODO: Make this parse any expression rather than hardcoded str or number
        let expressions = Str.or(Number).multiple().parse(input)?;

        check_and_skip(input, Token::RParenthesis)?;

        return Some(expressions);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;


    pub fn test_all_cases(parser: impl ExpressionParser, inputs: &[(&str, Option<Expression>)]) {
        inputs.iter().for_each(|test_case| {
            let result = parser.parse(&mut Lexer::lex(test_case.0).as_slice());

            assert_eq!(result, test_case.1);
        });
    }


    #[test]
    fn number_parser_basic() {
        let inputs = [
            ("1", Some(E::Int(1))),
            ("69420", Some(E::Int(69420))),
            ("asdf", None),
            ("5.321", Some(E::Decimal(5, 321))),
            ("5.3.2.1", None),
        ];

        test_all_cases(Number, &inputs);
    }

    #[test]
    fn identifier_parser_basic() {
        let inputs = [
            ("blablabla", Some(E::Ident("blablabla".into()))),
            ("Bl_a", Some(E::Ident("Bl_a".into()))),
            ("1abc", None),
        ];

        test_all_cases(Identifier, &inputs);
    }

    #[test]
    fn parse_all_columns_character() {
        let inputs = [
            ("*", Some(E::AllColumns)),
            ("asdf", None)
        ];

        test_all_cases(AllColumn, &inputs);
    }

    #[test]
    fn column_parser_basic() {
        let inputs = [
            ("*", Some(E::Array(vec![E::AllColumns]))),
            ("column_name", Some(E::Array(vec![E::Ident("column_name".into())]))),
            ("otherColumnName", Some(E::Array(vec![E::Ident("otherColumnName".into())]))),
            ("a, b", Some(E::Array(vec![
                E::Ident("a".into()),
                E::Ident("b".into()),
            ]))),
            // No trailing commas
            ("a, b,", None),
        ];

        test_all_cases(Column, &inputs);
    }

    #[test]
    fn where_parser_basic() {
        let inputs = [
            ("WHERE a = 5", Some(E::Where {
                left: E::Ident("a".into()).into(),
                operator: InfixOperator::Equals,
                right: E::Int(5).into()
            })),
            ("WHERE column >= other_column", Some(E::Where {
                left: E::Ident("column".into()).into(),
                operator: InfixOperator::GreaterThanEqual,
                right: E::Ident("other_column".into()).into()
            })),
            ("WHERE 10 <> 5", Some(E::Where {
                left: E::Int(10).into(),
                operator: InfixOperator::NotEqual,
                right: E::Int(5).into()
            })),
            ("WHERE column", None),
            ("column <> other_column", None),
            ("WHERE * = 0", None),
        ];

        test_all_cases(Where, &inputs);
    }

    #[test]
    fn array_basic() {
        let inputs = [
            ("(1, 2.3, 'hey', 4)", Some(E::Array(vec![
                E::Int(1),
                E::Decimal(2, 3),
                E::Str("hey".into()),
                E::Int(4),
            ]))),
            // Allow trailing commas
            ("(1,)", Some(E::Array(vec![
                E::Int(1),
            ]))),
        ];

        test_all_cases(Array, &inputs);
    }
}
