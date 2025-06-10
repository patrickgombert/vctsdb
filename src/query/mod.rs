//! Query module for VCTSDB
//! Handles query parsing, planning, and execution.

pub mod executor;
pub mod parser;
pub mod planner;

pub use parser::ast::{Query, TimeRange, FilterExpr, TagFilter, TagFilterOp, FunctionCall, SelectExpr};
pub use executor::{QueryExecutor, ExecutionConfig, ExecutionError, ExecutionResult};

#[cfg(test)]
mod tests {
    
}
