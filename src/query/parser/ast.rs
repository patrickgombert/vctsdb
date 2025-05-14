use thiserror::Error;

#[derive(Debug, Error)]
pub enum AstError {
    #[error("Invalid time range expression: {0}")]
    InvalidTimeRange(String),
    #[error("Invalid tag filter expression: {0}")]
    InvalidTagFilter(String),
    #[error("Invalid function call: {0}")]
    InvalidFunctionCall(String),
}

#[derive(Debug, Clone)]
pub enum TimeRange {
    Absolute {
        start: i64,  // Unix timestamp in nanoseconds
        end: i64,
    },
    Relative {
        offset: i64,  // Offset in nanoseconds
        duration: i64,  // Duration in nanoseconds
    },
    Last {
        duration: i64,  // Duration in nanoseconds
    },
}

#[derive(Debug, Clone)]
pub enum TagFilterOp {
    Eq,
    Neq,
    Regex,
    NotRegex,
}

#[derive(Debug, Clone)]
pub struct TagFilter {
    pub key: String,
    pub op: TagFilterOp,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum FilterExpr {
    TagFilter(TagFilter),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
}

#[derive(Debug, Clone)]
pub enum FunctionArg {
    Identifier(String),
    NumberLiteral(f64),
    StringLiteral(String),
    FunctionCall(Box<FunctionCall>),
}

#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<FunctionArg>,
}

#[derive(Debug, Clone)]
pub struct SelectExpr {
    pub function: FunctionCall,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub select: Vec<SelectExpr>,
    pub from: String,
    pub time_range: Option<TimeRange>,
    pub filter: Option<FilterExpr>,
    pub group_by: Vec<String>,
    pub order_by: Vec<(String, bool)>,  // (field, descending)
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl Query {
    pub fn new() -> Self {
        Self {
            select: Vec::new(),
            from: String::new(),
            time_range: None,
            filter: None,
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query() {
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
            time_range: Some(TimeRange::Last {
                duration: 3600_000_000_000, // 1 hour in nanoseconds
            }),
            filter: Some(FilterExpr::TagFilter(TagFilter {
                key: "region".to_string(),
                op: TagFilterOp::Eq,
                value: "us-west".to_string(),
            })),
            group_by: vec!["datacenter".to_string()],
            order_by: vec![("avg_value".to_string(), true)],
            limit: Some(10),
            offset: None,
        };

        // Verify the query structure
        assert_eq!(query.from, "metrics");
        assert_eq!(query.group_by.len(), 1);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_complex_filter() {
        let filter = FilterExpr::And(
            Box::new(FilterExpr::TagFilter(TagFilter {
                key: "region".to_string(),
                op: TagFilterOp::Eq,
                value: "us-west".to_string(),
            })),
            Box::new(FilterExpr::Or(
                Box::new(FilterExpr::TagFilter(TagFilter {
                    key: "env".to_string(),
                    op: TagFilterOp::Eq,
                    value: "prod".to_string(),
                })),
                Box::new(FilterExpr::TagFilter(TagFilter {
                    key: "env".to_string(),
                    op: TagFilterOp::Eq,
                    value: "staging".to_string(),
                })),
            )),
        );

        // Verify the filter structure
        if let FilterExpr::And(left, right) = &filter {
            if let FilterExpr::TagFilter(tag_filter) = left.as_ref() {
                assert_eq!(tag_filter.key, "region");
                assert_eq!(tag_filter.value, "us-west");
            } else {
                panic!("Expected TagFilter");
            }
        } else {
            panic!("Expected And");
        }
    }
} 