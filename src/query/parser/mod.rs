pub mod lexer;
pub mod ast;
pub mod validator;

pub use lexer::{Lexer, Token, LexerError};
pub use ast::{AstError, Query, TimeRange, FilterExpr, TagFilter, TagFilterOp, FunctionCall, SelectExpr};
pub use validator::{ValidationError, QueryValidator, Schema};

use std::iter::Peekable;
use std::slice::Iter;

pub struct Parser<'a> {
    tokens: Peekable<Iter<'a, Token>>,
    validator: Option<QueryValidator>,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: &'a [Token]) -> Self {
        Self {
            tokens: tokens.iter().peekable(),
            validator: None,
        }
    }

    pub fn with_validator(mut self, validator: QueryValidator) -> Self {
        self.validator = Some(validator);
        self
    }

    pub fn parse(&mut self) -> Result<Query, AstError> {
        let mut query = Query::new();

        // Parse SELECT clause
        self.expect_token(Token::Select)?;
        query.select = self.parse_select_list()?;

        // Parse FROM clause
        self.expect_token(Token::From)?;
        if let Token::Identifier(name) = self.next_token()?.clone() {
            query.from = name;
        } else {
            return Err(AstError::InvalidFunctionCall("Expected table name after FROM".to_string()));
        }

        // Parse WHERE clause (optional)
        if self.peek_token() == Some(&&Token::Where) {
            self.next_token()?;
            query.filter = Some(self.parse_filter()?);
        }

        // Parse GROUP BY clause (optional)
        if self.peek_token() == Some(&&Token::GroupBy) {
            self.next_token()?;
            query.group_by = self.parse_identifier_list()?;
        }

        // Parse ORDER BY clause (optional)
        if self.peek_token() == Some(&&Token::OrderBy) {
            self.next_token()?;
            query.order_by = self.parse_order_by()?;
        }

        // Parse LIMIT clause (optional)
        if self.peek_token() == Some(&&Token::Limit) {
            self.next_token()?;
            if let Token::NumberLiteral(limit) = self.next_token()?.clone() {
                query.limit = Some(limit as usize);
            } else {
                return Err(AstError::InvalidFunctionCall("Expected number after LIMIT".to_string()));
            }
        }

        // Parse OFFSET clause (optional)
        if self.peek_token() == Some(&&Token::Offset) {
            self.next_token()?;
            if let Token::NumberLiteral(offset) = self.next_token()?.clone() {
                query.offset = Some(offset as usize);
            } else {
                return Err(AstError::InvalidFunctionCall("Expected number after OFFSET".to_string()));
            }
        }

        // Validate the query if a validator is provided
        if let Some(validator) = &self.validator {
            validator.validate(&query).map_err(|e| {
                AstError::InvalidFunctionCall(format!("Validation error: {}", e))
            })?;
        }

        Ok(query)
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectExpr>, AstError> {
        let mut select_list = Vec::new();
        
        loop {
            let expr = self.parse_select_expr()?;
            select_list.push(expr);

            if self.peek_token() == Some(&&Token::Comma) {
                self.next_token()?;
            } else {
                break;
            }
        }

        Ok(select_list)
    }

    fn parse_select_expr(&mut self) -> Result<SelectExpr, AstError> {
        let function = self.parse_function_call()?;
        let alias = if self.peek_token() == Some(&&Token::As) {
            self.next_token()?;
            if let Token::Identifier(name) = self.next_token()?.clone() {
                Some(name)
            } else {
                return Err(AstError::InvalidFunctionCall("Expected identifier after AS".to_string()));
            }
        } else {
            None
        };

        Ok(SelectExpr { function, alias })
    }

    fn parse_function_call(&mut self) -> Result<FunctionCall, AstError> {
        let name = if let Token::Identifier(name) = self.next_token()?.clone() {
            name
        } else {
            return Err(AstError::InvalidFunctionCall("Expected function name".to_string()));
        };

        self.expect_token(Token::LParen)?;
        let args = self.parse_function_args()?;
        self.expect_token(Token::RParen)?;

        Ok(FunctionCall { name, args })
    }

    fn parse_function_args(&mut self) -> Result<Vec<ast::FunctionArg>, AstError> {
        let mut args = Vec::new();
        
        loop {
            let arg = match self.peek_token() {
                Some(&&Token::Identifier(_)) => {
                    // Lookahead for nested function call
                    let name = if let Token::Identifier(name) = self.next_token()?.clone() {
                        name
                    } else {
                        unreachable!()
                    };
                    if self.peek_token() == Some(&&Token::LParen) {
                        self.next_token()?; // consume LParen
                        let nested_args = self.parse_function_args()?;
                        self.expect_token(Token::RParen)?;
                        ast::FunctionArg::FunctionCall(Box::new(FunctionCall {
                            name,
                            args: nested_args,
                        }))
                    } else {
                        ast::FunctionArg::Identifier(name)
                    }
                }
                Some(&&Token::NumberLiteral(_)) => {
                    if let Token::NumberLiteral(value) = self.next_token()?.clone() {
                        ast::FunctionArg::NumberLiteral(value)
                    } else {
                        unreachable!()
                    }
                }
                Some(&&Token::StringLiteral(_)) => {
                    if let Token::StringLiteral(value) = self.next_token()?.clone() {
                        ast::FunctionArg::StringLiteral(value)
                    } else {
                        unreachable!()
                    }
                }
                _ => return Err(AstError::InvalidFunctionCall("Invalid function argument".to_string())),
            };
            args.push(arg);

            if self.peek_token() == Some(&&Token::Comma) {
                self.next_token()?;
            } else {
                break;
            }
        }

        Ok(args)
    }

    fn parse_filter(&mut self) -> Result<FilterExpr, AstError> {
        let mut expr = self.parse_filter_term()?;

        while let Some(token) = self.peek_token() {
            match token {
                Token::And => {
                    self.next_token()?;
                    let right = self.parse_filter_term()?;
                    expr = FilterExpr::And(Box::new(expr), Box::new(right));
                }
                Token::Or => {
                    self.next_token()?;
                    let right = self.parse_filter_term()?;
                    expr = FilterExpr::Or(Box::new(expr), Box::new(right));
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_filter_term(&mut self) -> Result<FilterExpr, AstError> {
        if self.peek_token() == Some(&&Token::Not) {
            self.next_token()?;
            let expr = self.parse_filter_term()?;
            return Ok(FilterExpr::Not(Box::new(expr)));
        }

        if self.peek_token() == Some(&&Token::LParen) {
            self.next_token()?;
            let expr = self.parse_filter()?;
            self.expect_token(Token::RParen)?;
            return Ok(expr);
        }

        let key = if let Token::Identifier(key) = self.next_token()?.clone() {
            key
        } else {
            return Err(AstError::InvalidTagFilter("Expected tag key".to_string()));
        };

        let op = match self.next_token()? {
            Token::Eq => TagFilterOp::Eq,
            Token::Neq => TagFilterOp::Neq,
            _ => return Err(AstError::InvalidTagFilter("Expected comparison operator".to_string())),
        };

        let value = match self.next_token()? {
            Token::StringLiteral(value) => value.clone(),
            Token::Identifier(value) => value.clone(),
            _ => return Err(AstError::InvalidTagFilter("Expected string or identifier".to_string())),
        };

        Ok(FilterExpr::TagFilter(TagFilter { key, op, value }))
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, AstError> {
        let mut identifiers = Vec::new();
        
        loop {
            if let Token::Identifier(name) = self.next_token()?.clone() {
                identifiers.push(name);
            } else {
                return Err(AstError::InvalidFunctionCall("Expected identifier".to_string()));
            }

            if self.peek_token() == Some(&&Token::Comma) {
                self.next_token()?;
            } else {
                break;
            }
        }

        Ok(identifiers)
    }

    fn parse_order_by(&mut self) -> Result<Vec<(String, bool)>, AstError> {
        let mut order_by = Vec::new();
        
        loop {
            if let Token::Identifier(name) = self.next_token()?.clone() {
                let descending = if self.peek_token() == Some(&&Token::Desc) {
                    self.next_token()?;
                    true
                } else if self.peek_token() == Some(&&Token::Asc) {
                    self.next_token()?;
                    false
                } else {
                    false
                };
                order_by.push((name, descending));
            } else {
                return Err(AstError::InvalidFunctionCall("Expected identifier in ORDER BY".to_string()));
            }

            if self.peek_token() == Some(&&Token::Comma) {
                self.next_token()?;
            } else {
                break;
            }
        }

        Ok(order_by)
    }

    fn next_token(&mut self) -> Result<&Token, AstError> {
        self.tokens.next().ok_or_else(|| {
            AstError::InvalidFunctionCall("Unexpected end of input".to_string())
        })
    }

    fn peek_token(&mut self) -> Option<&&Token> {
        self.tokens.peek()
    }

    fn expect_token(&mut self, expected: Token) -> Result<(), AstError> {
        let token = self.next_token()?;
        if token == &expected {
            Ok(())
        } else {
            Err(AstError::InvalidFunctionCall(format!(
                "Expected {:?}, got {:?}",
                expected, token
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Query, SelectExpr, FunctionCall, FunctionArg, FilterExpr, TagFilter, TagFilterOp};

    #[test]
    fn test_parse_basic_query() {
        let input = "SELECT avg(value) as avg_val FROM metrics WHERE region = 'us-west' GROUP BY datacenter ORDER BY avg_val DESC LIMIT 10";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = Parser::new(&tokens);
        let query = parser.parse().unwrap();

        assert_eq!(query.from, "metrics");
        assert_eq!(query.select.len(), 1);
        assert_eq!(query.group_by.len(), 1);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_parse_with_validation() {
        let input = "SELECT avg(value) as avg_val FROM metrics WHERE region = 'us-west' GROUP BY value ORDER BY avg_val DESC LIMIT 10";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();

        // Create schema with known fields
        let mut schema = Schema::new();
        schema.add_tag_key("region".to_string());
        schema.add_value_field("value".to_string());

        let validator = QueryValidator::new().with_schema(schema);
        let mut parser = Parser::new(&tokens).with_validator(validator);
        
        let query = parser.parse().unwrap();
        assert_eq!(query.from, "metrics");
        assert_eq!(query.select.len(), 1);
        assert_eq!(query.group_by.len(), 1);
    }

    #[test]
    fn test_parse_with_invalid_validation() {
        let input = "SELECT unknown_func(value) FROM metrics WHERE unknown_tag = 'us-west'";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();

        // Create schema with known fields
        let mut schema = Schema::new();
        schema.add_tag_key("region".to_string());
        schema.add_value_field("value".to_string());

        let validator = QueryValidator::new().with_schema(schema);
        let mut parser = Parser::new(&tokens).with_validator(validator);
        
        assert!(parser.parse().is_err());
    }

    #[test]
    fn test_operator_precedence() {
        let input = "SELECT avg(value) FROM metrics WHERE region = 'us-west' AND env = 'prod' OR env = 'staging'";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = Parser::new(&tokens);
        let query = parser.parse().unwrap();

        if let Some(FilterExpr::Or(left, right)) = query.filter {
            // Verify that AND has higher precedence than OR
            assert!(matches!(left.as_ref(), FilterExpr::And(_, _)));
            assert!(matches!(right.as_ref(), FilterExpr::TagFilter(_)));
        } else {
            panic!("Expected OR expression");
        }
    }

    #[test]
    fn test_edge_cases() {
        // Test empty SELECT list
        let input = "SELECT FROM metrics";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = Parser::new(&tokens);
        assert!(parser.parse().is_err());

        // Test nested function calls
        let input = "SELECT avg(sum(value)) FROM metrics";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = Parser::new(&tokens);
        let query = parser.parse().unwrap();
        assert_eq!(query.select.len(), 1);

        // Test complex boolean expressions
        let input = "SELECT avg(value) FROM metrics WHERE NOT (region = 'us-west' AND env = 'prod')";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = Parser::new(&tokens);
        let query = parser.parse().unwrap();
        assert!(matches!(query.filter, Some(FilterExpr::Not(_))));
    }
} 