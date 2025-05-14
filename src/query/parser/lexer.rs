use std::iter::Peekable;
use std::str::Chars;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LexerError {
    #[error("Unexpected character: {0}")]
    UnexpectedChar(char),
    #[error("Invalid number format: {0}")]
    InvalidNumber(String),
    #[error("Unterminated string literal")]
    UnterminatedString,
    #[error("Invalid identifier: {0}")]
    InvalidIdentifier(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    GroupBy,
    OrderBy,
    Limit,
    Offset,
    And,
    Or,
    Not,
    As,
    By,
    Desc,
    Asc,
    
    // Operators
    Eq,        // =
    Neq,       // !=
    Gt,        // >
    Lt,        // <
    Gte,       // >=
    Lte,       // <=
    Plus,      // +
    Minus,     // -
    Star,      // *
    Slash,     // /
    Percent,   // %
    
    // Punctuation
    Comma,     // ,
    Dot,       // .
    LParen,    // (
    RParen,    // )
    LBracket,  // [
    RBracket,  // ]
    Semicolon, // ;
    
    // Literals
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(f64),
    
    // Special
    EOF,
}

pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    current_pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input: input.chars().peekable(),
            current_pos: 0,
        }
    }
    
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        let mut tokens = Vec::new();
        
        while let Some(token) = self.next_token()? {
            tokens.push(token);
        }
        
        tokens.push(Token::EOF);
        Ok(tokens)
    }
    
    fn next_token(&mut self) -> Result<Option<Token>, LexerError> {
        self.skip_whitespace();
        
        if let Some(&c) = self.input.peek() {
            let token = match c {
                // Single character tokens
                '=' => {
                    self.input.next();
                    Token::Eq
                }
                '!' => {
                    self.input.next();
                    if let Some('=') = self.input.peek() {
                        self.input.next();
                        Token::Neq
                    } else {
                        return Err(LexerError::UnexpectedChar('!'));
                    }
                }
                '>' => {
                    self.input.next();
                    if let Some('=') = self.input.peek() {
                        self.input.next();
                        Token::Gte
                    } else {
                        Token::Gt
                    }
                }
                '<' => {
                    self.input.next();
                    if let Some('=') = self.input.peek() {
                        self.input.next();
                        Token::Lte
                    } else {
                        Token::Lt
                    }
                }
                '+' => {
                    self.input.next();
                    Token::Plus
                }
                '-' => {
                    self.input.next();
                    Token::Minus
                }
                '*' => {
                    self.input.next();
                    Token::Star
                }
                '/' => {
                    self.input.next();
                    Token::Slash
                }
                '%' => {
                    self.input.next();
                    Token::Percent
                }
                ',' => {
                    self.input.next();
                    Token::Comma
                }
                '.' => {
                    self.input.next();
                    Token::Dot
                }
                '(' => {
                    self.input.next();
                    Token::LParen
                }
                ')' => {
                    self.input.next();
                    Token::RParen
                }
                '[' => {
                    self.input.next();
                    Token::LBracket
                }
                ']' => {
                    self.input.next();
                    Token::RBracket
                }
                ';' => {
                    self.input.next();
                    Token::Semicolon
                }
                
                // String literals
                '"' | '\'' => self.parse_string()?,
                
                // Numbers and identifiers
                c if c.is_ascii_digit() => self.parse_number()?,
                c if c.is_ascii_alphabetic() || c == '_' => self.parse_identifier()?,
                
                // Unexpected character
                c => return Err(LexerError::UnexpectedChar(c)),
            };
            
            Ok(Some(token))
        } else {
            Ok(None)
        }
    }
    
    fn skip_whitespace(&mut self) {
        while let Some(&c) = self.input.peek() {
            if c.is_whitespace() {
                self.input.next();
                self.current_pos += 1;
            } else {
                break;
            }
        }
    }
    
    fn parse_string(&mut self) -> Result<Token, LexerError> {
        let quote = self.input.next().unwrap();
        let mut string = String::new();
        
        while let Some(&c) = self.input.peek() {
            if c == quote {
                self.input.next();
                return Ok(Token::StringLiteral(string));
            }
            string.push(self.input.next().unwrap());
        }
        
        Err(LexerError::UnterminatedString)
    }
    
    fn parse_number(&mut self) -> Result<Token, LexerError> {
        let mut number = String::new();
        let mut has_decimal = false;
        
        while let Some(&c) = self.input.peek() {
            match c {
                '0'..='9' => {
                    number.push(self.input.next().unwrap());
                }
                '.' if !has_decimal => {
                    has_decimal = true;
                    number.push(self.input.next().unwrap());
                }
                _ => break,
            }
        }
        
        number.parse::<f64>()
            .map(Token::NumberLiteral)
            .map_err(|_| LexerError::InvalidNumber(number))
    }
    
    fn peek_word(&mut self) -> String {
        let mut word = String::new();
        let mut chars = self.input.clone();
        
        while let Some(c) = chars.next() {
            if c.is_ascii_alphanumeric() || c == '_' {
                word.push(c);
            } else {
                break;
            }
        }
        
        word
    }
    
    fn consume_chars(&mut self, count: usize) {
        for _ in 0..count {
            self.input.next();
        }
    }
    
    fn parse_identifier(&mut self) -> Result<Token, LexerError> {
        let mut identifier = String::new();
        
        while let Some(&c) = self.input.peek() {
            if c.is_ascii_alphanumeric() || c == '_' {
                identifier.push(self.input.next().unwrap());
            } else {
                break;
            }
        }
        
        // Check for compound keywords (GROUP BY, ORDER BY)
        let token = match identifier.to_lowercase().as_str() {
            "select" => Token::Select,
            "from" => Token::From,
            "where" => Token::Where,
            "group" => {
                self.skip_whitespace();
                if self.peek_word().to_lowercase() == "by" {
                    self.consume_chars(2); // Consume "by"
                    Token::GroupBy
                } else {
                    Token::Identifier(identifier)
                }
            }
            "order" => {
                self.skip_whitespace();
                if self.peek_word().to_lowercase() == "by" {
                    self.consume_chars(2); // Consume "by"
                    Token::OrderBy
                } else {
                    Token::Identifier(identifier)
                }
            }
            "limit" => Token::Limit,
            "offset" => Token::Offset,
            "and" => Token::And,
            "or" => Token::Or,
            "not" => Token::Not,
            "as" => Token::As,
            "by" => Token::By,
            "desc" => Token::Desc,
            "asc" => Token::Asc,
            _ => Token::Identifier(identifier),
        };
        
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_tokens() {
        let input = "SELECT * FROM metrics WHERE value > 42.5";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens, vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Identifier("metrics".to_string()),
            Token::Where,
            Token::Identifier("value".to_string()),
            Token::Gt,
            Token::NumberLiteral(42.5),
            Token::EOF,
        ]);
    }
    
    #[test]
    fn test_string_literals() {
        let input = r#"SELECT * FROM "my metrics" WHERE name = 'test'"#;
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens, vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::StringLiteral("my metrics".to_string()),
            Token::Where,
            Token::Identifier("name".to_string()),
            Token::Eq,
            Token::StringLiteral("test".to_string()),
            Token::EOF,
        ]);
    }
    
    #[test]
    fn test_complex_query() {
        let input = "SELECT avg(value) as avg_val FROM metrics WHERE region = 'us-west' AND value > 100 GROUP BY datacenter ORDER BY avg_val DESC LIMIT 10";
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens, vec![
            Token::Select,
            Token::Identifier("avg".to_string()),
            Token::LParen,
            Token::Identifier("value".to_string()),
            Token::RParen,
            Token::As,
            Token::Identifier("avg_val".to_string()),
            Token::From,
            Token::Identifier("metrics".to_string()),
            Token::Where,
            Token::Identifier("region".to_string()),
            Token::Eq,
            Token::StringLiteral("us-west".to_string()),
            Token::And,
            Token::Identifier("value".to_string()),
            Token::Gt,
            Token::NumberLiteral(100.0),
            Token::GroupBy,
            Token::Identifier("datacenter".to_string()),
            Token::OrderBy,
            Token::Identifier("avg_val".to_string()),
            Token::Desc,
            Token::Limit,
            Token::NumberLiteral(10.0),
            Token::EOF,
        ]);
    }
    
    #[test]
    fn test_error_handling() {
        let input = "SELECT * FROM metrics WHERE value > @";
        let mut lexer = Lexer::new(input);
        let result = lexer.tokenize();
        
        assert!(matches!(result, Err(LexerError::UnexpectedChar('@'))));
    }
} 