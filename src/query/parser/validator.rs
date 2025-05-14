use thiserror::Error;
use std::collections::HashSet;

use super::ast::{Query, FunctionCall, FunctionArg, FilterExpr, TagFilter, AstError};

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Unknown function: {0}")]
    UnknownFunction(String),
    #[error("Invalid argument count for function {0}: expected {1}, got {2}")]
    InvalidArgumentCount(String, usize, usize),
    #[error("Invalid argument type for function {0}: {1}")]
    InvalidArgumentType(String, String),
    #[error("Unknown tag key: {0}")]
    UnknownTagKey(String),
    #[error("Invalid tag value type: {0}")]
    InvalidTagValueType(String),
    #[error("Invalid order by field: {0}")]
    InvalidOrderByField(String),
    #[error("Invalid group by field: {0}")]
    InvalidGroupByField(String),
}

/// Registry of known functions and their signatures
pub struct FunctionRegistry {
    functions: HashSet<String>,
    // TODO: Add function signatures with argument types
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut functions = HashSet::new();
        // Add built-in functions
        functions.insert("avg".to_string());
        functions.insert("sum".to_string());
        functions.insert("min".to_string());
        functions.insert("max".to_string());
        functions.insert("count".to_string());
        functions.insert("rate".to_string());
        functions.insert("stddev".to_string());
        functions.insert("percentile".to_string());
        
        Self { functions }
    }

    pub fn validate_function(&self, name: &str) -> Result<(), ValidationError> {
        if !self.functions.contains(name) {
            return Err(ValidationError::UnknownFunction(name.to_string()));
        }
        Ok(())
    }

    pub fn validate_arguments(&self, call: &FunctionCall) -> Result<(), ValidationError> {
        self.validate_function(&call.name)?;

        // Basic argument count validation
        match call.name.as_str() {
            "avg" | "sum" | "min" | "max" | "count" | "rate" => {
                if call.args.len() != 1 {
                    return Err(ValidationError::InvalidArgumentCount(
                        call.name.clone(),
                        1,
                        call.args.len(),
                    ));
                }
                Ok(())
            }
            "percentile" => {
                if call.args.len() != 2 {
                    return Err(ValidationError::InvalidArgumentCount(
                        call.name.clone(),
                        2,
                        call.args.len(),
                    ));
                }
                // Validate second argument is a number
                if let FunctionArg::NumberLiteral(_) = &call.args[1] {
                    Ok(())
                } else {
                    Err(ValidationError::InvalidArgumentType(
                        call.name.clone(),
                        "Second argument must be a number".to_string(),
                    ))
                }
            }
            _ => Ok(()),
        }
    }
}

/// Schema information for validation
pub struct Schema {
    pub tag_keys: HashSet<String>,
    pub value_fields: HashSet<String>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tag_keys: HashSet::new(),
            value_fields: HashSet::new(),
        }
    }

    pub fn add_tag_key(&mut self, key: String) {
        self.tag_keys.insert(key);
    }

    pub fn add_value_field(&mut self, field: String) {
        self.value_fields.insert(field);
    }

    pub fn validate_tag_key(&self, key: &str) -> Result<(), ValidationError> {
        if !self.tag_keys.contains(key) {
            return Err(ValidationError::UnknownTagKey(key.to_string()));
        }
        Ok(())
    }

    pub fn validate_value_field(&self, field: &str) -> Result<(), ValidationError> {
        if !self.value_fields.contains(field) {
            return Err(ValidationError::InvalidOrderByField(field.to_string()));
        }
        Ok(())
    }
}

pub struct QueryValidator {
    function_registry: FunctionRegistry,
    schema: Schema,
}

impl QueryValidator {
    pub fn new() -> Self {
        Self {
            function_registry: FunctionRegistry::new(),
            schema: Schema::new(),
        }
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = schema;
        self
    }

    pub fn validate(&self, query: &Query) -> Result<(), ValidationError> {
        // Collect select aliases
        let mut select_aliases = std::collections::HashSet::new();
        for expr in &query.select {
            if let Some(alias) = &expr.alias {
                select_aliases.insert(alias.clone());
            }
        }

        // Validate SELECT expressions
        for expr in &query.select {
            self.validate_function_call(&expr.function)?;
        }

        // Validate WHERE clause
        if let Some(filter) = &query.filter {
            self.validate_filter(filter)?;
        }

        // Validate GROUP BY fields
        for field in &query.group_by {
            if !self.schema.value_fields.contains(field) && !select_aliases.contains(field) {
                return Err(ValidationError::InvalidGroupByField(field.clone()));
            }
        }

        // Validate ORDER BY fields
        for (field, _) in &query.order_by {
            if !self.schema.value_fields.contains(field) && !select_aliases.contains(field) {
                return Err(ValidationError::InvalidOrderByField(field.clone()));
            }
        }

        Ok(())
    }

    fn validate_function_call(&self, call: &FunctionCall) -> Result<(), ValidationError> {
        self.function_registry.validate_arguments(call)?;

        // Validate function arguments
        for arg in &call.args {
            match arg {
                FunctionArg::Identifier(name) => {
                    self.schema.validate_value_field(name)?;
                }
                FunctionArg::FunctionCall(nested_call) => {
                    self.validate_function_call(nested_call)?;
                }
                _ => {} // Numbers and strings are always valid
            }
        }

        Ok(())
    }

    fn validate_filter(&self, filter: &FilterExpr) -> Result<(), ValidationError> {
        match filter {
            FilterExpr::TagFilter(tag_filter) => {
                self.schema.validate_tag_key(&tag_filter.key)?;
                // TODO: Add tag value type validation
            }
            FilterExpr::And(left, right) => {
                self.validate_filter(left)?;
                self.validate_filter(right)?;
            }
            FilterExpr::Or(left, right) => {
                self.validate_filter(left)?;
                self.validate_filter(right)?;
            }
            FilterExpr::Not(expr) => {
                self.validate_filter(expr)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Query, SelectExpr, FunctionCall, FunctionArg, FilterExpr, TagFilter, TagFilterOp};

    fn create_test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.add_tag_key("region".to_string());
        schema.add_tag_key("env".to_string());
        schema.add_value_field("value".to_string());
        schema.add_value_field("count".to_string());
        schema
    }

    #[test]
    fn test_valid_query() {
        let schema = create_test_schema();
        let validator = QueryValidator::new().with_schema(schema);

        let query = Query {
            select: vec![
                SelectExpr {
                    function: FunctionCall {
                        name: "avg".to_string(),
                        args: vec![FunctionArg::Identifier("value".to_string())],
                    },
                    alias: Some("avg_value".to_string()),
                }
            ],
            from: "metrics".to_string(),
            time_range: None,
            filter: Some(FilterExpr::TagFilter(TagFilter {
                key: "region".to_string(),
                op: TagFilterOp::Eq,
                value: "us-west".to_string(),
            })),
            group_by: vec!["value".to_string()],
            order_by: vec![("avg_value".to_string(), true)],
            limit: Some(10),
            offset: None,
        };

        assert!(validator.validate(&query).is_ok());
    }

    #[test]
    fn test_unknown_function() {
        let schema = create_test_schema();
        let validator = QueryValidator::new().with_schema(schema);

        let query = Query {
            select: vec![
                SelectExpr {
                    function: FunctionCall {
                        name: "unknown_func".to_string(),
                        args: vec![FunctionArg::Identifier("value".to_string())],
                    },
                    alias: None,
                }
            ],
            from: "metrics".to_string(),
            time_range: None,
            filter: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert!(matches!(
            validator.validate(&query),
            Err(ValidationError::UnknownFunction(_))
        ));
    }

    #[test]
    fn test_invalid_tag_key() {
        let schema = create_test_schema();
        let validator = QueryValidator::new().with_schema(schema);

        let query = Query {
            select: vec![
                SelectExpr {
                    function: FunctionCall {
                        name: "avg".to_string(),
                        args: vec![FunctionArg::Identifier("value".to_string())],
                    },
                    alias: None,
                }
            ],
            from: "metrics".to_string(),
            time_range: None,
            filter: Some(FilterExpr::TagFilter(TagFilter {
                key: "unknown_tag".to_string(),
                op: TagFilterOp::Eq,
                value: "us-west".to_string(),
            })),
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert!(matches!(
            validator.validate(&query),
            Err(ValidationError::UnknownTagKey(_))
        ));
    }

    #[test]
    fn test_invalid_argument_count() {
        let schema = create_test_schema();
        let validator = QueryValidator::new().with_schema(schema);

        let query = Query {
            select: vec![
                SelectExpr {
                    function: FunctionCall {
                        name: "avg".to_string(),
                        args: vec![
                            FunctionArg::Identifier("value".to_string()),
                            FunctionArg::Identifier("count".to_string()),
                        ],
                    },
                    alias: None,
                }
            ],
            from: "metrics".to_string(),
            time_range: None,
            filter: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert!(matches!(
            validator.validate(&query),
            Err(ValidationError::InvalidArgumentCount(_, _, _))
        ));
    }
} 