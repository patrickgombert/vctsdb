use std::collections::HashMap;
use thiserror::Error;

use crate::query::parser::ast::{Query, FilterExpr, TagFilter, TimeRange};
use crate::storage::index::IndexInfo;

#[derive(Debug, Error)]
pub enum PlanningError {
    #[error("No suitable index found for query: {0}")]
    NoSuitableIndex(String),
    #[error("Invalid time range: {0}")]
    InvalidTimeRange(String),
    #[error("Invalid filter expression: {0}")]
    InvalidFilter(String),
}

#[derive(Debug, Clone)]
pub struct IndexSelection {
    pub index_name: String,
    pub time_range: TimeRange,
    pub filter: Option<FilterExpr>,
    pub estimated_rows: usize,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub index_selections: Vec<IndexSelection>,
    pub group_by: Vec<String>,
    pub order_by: Vec<(String, bool)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub struct QueryPlanner {
    available_indexes: HashMap<String, IndexInfo>,
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {
            available_indexes: HashMap::new(),
        }
    }

    pub fn register_index(&mut self, name: String, info: IndexInfo) {
        self.available_indexes.insert(name, info);
    }

    pub fn plan_query(&self, query: &Query) -> Result<QueryPlan, PlanningError> {
        // Select appropriate indexes based on time range and filters
        let index_selections = self.select_indexes(query)?;

        // Create the query plan
        Ok(QueryPlan {
            index_selections,
            group_by: query.group_by.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit,
            offset: query.offset,
        })
    }

    fn select_indexes(&self, query: &Query) -> Result<Vec<IndexSelection>, PlanningError> {
        let mut selections = Vec::new();

        // Get time range from query
        let time_range = query.time_range.clone().ok_or_else(|| {
            PlanningError::InvalidTimeRange("Query must specify a time range".to_string())
        })?;

        // Find indexes that can satisfy the query's time range and filters
        for (name, info) in &self.available_indexes {
            if self.can_satisfy_query(name, info, &time_range, &query.filter) {
                let estimated_rows = self.estimate_rows(info, &time_range, &query.filter);
                
                selections.push(IndexSelection {
                    index_name: name.clone(),
                    time_range: time_range.clone(),
                    filter: query.filter.clone(),
                    estimated_rows,
                });
            }
        }

        if selections.is_empty() {
            return Err(PlanningError::NoSuitableIndex(
                "No index can satisfy the query requirements".to_string(),
            ));
        }

        // Sort selections by estimated row count to prefer more selective indexes
        selections.sort_by_key(|s| s.estimated_rows);

        Ok(selections)
    }

    fn can_satisfy_query(
        &self,
        index_name: &str,
        info: &IndexInfo,
        time_range: &TimeRange,
        filter: &Option<FilterExpr>,
    ) -> bool {
        // Check if index covers the time range
        if !info.covers_time_range(time_range) {
            return false;
        }

        // Check if index can satisfy the filter
        if let Some(filter) = filter {
            if !info.can_satisfy_filter(filter) {
                return false;
            }
        }

        true
    }

    fn estimate_rows(
        &self,
        info: &IndexInfo,
        time_range: &TimeRange,
        filter: &Option<FilterExpr>,
    ) -> usize {
        // Get base estimate from time range
        let mut estimate = info.estimate_rows_in_range(time_range);

        // Apply filter selectivity if present
        if let Some(filter) = filter {
            estimate = (estimate as f64 * info.estimate_filter_selectivity(filter)) as usize;
        }

        estimate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::IndexInfo;
    use crate::query::parser::ast::{TimeRange, FilterExpr, TagFilter, TagFilterOp};

    fn create_test_index() -> IndexInfo {
        IndexInfo {
            name: "test_index".to_string(),
            time_range: TimeRange::Absolute {
                start: 0,
                end: 1000000000000, // 1000 seconds
            },
            tag_keys: vec!["region".to_string(), "env".to_string()],
            estimated_rows: 1000,
        }
    }

    #[test]
    fn test_basic_query_planning() {
        let mut planner = QueryPlanner::new();
        planner.register_index("test_index".to_string(), create_test_index());

        let query = Query {
            select: vec![],
            from: "metrics".to_string(),
            time_range: Some(TimeRange::Absolute {
                start: 0,
                end: 1000000000000, // within the index's range
            }),
            filter: Some(FilterExpr::TagFilter(TagFilter {
                key: "region".to_string(),
                op: TagFilterOp::Eq,
                value: "us-west".to_string(),
            })),
            group_by: vec!["region".to_string()],
            order_by: vec![("value".to_string(), true)],
            limit: Some(10),
            offset: None,
        };

        let plan = planner.plan_query(&query).unwrap();
        assert_eq!(plan.index_selections.len(), 1);
        assert_eq!(plan.index_selections[0].index_name, "test_index");
    }

    #[test]
    fn test_no_suitable_index() {
        let planner = QueryPlanner::new();
        let query = Query {
            select: vec![],
            from: "metrics".to_string(),
            time_range: Some(TimeRange::Last {
                duration: 3600_000_000_000,
            }),
            filter: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert!(matches!(
            planner.plan_query(&query),
            Err(PlanningError::NoSuitableIndex(_))
        ));
    }
}
