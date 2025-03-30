//! Query module for VCTSDB
//! Handles query parsing, planning, and execution.

pub mod executor;
pub mod parser;
pub mod planner;

#[cfg(test)]
mod tests {
    use super::*;
}
