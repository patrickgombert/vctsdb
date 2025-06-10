use crate::query::parser::ast::{TimeRange, FilterExpr, TagFilter, TagFilterOp};
use std::collections::HashMap;
use crate::storage::data::DataPoint;

/// Information about an index for a time series
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// The name of the time series
    pub name: String,
    /// The time range of the index
    pub time_range: TimeRange,
    /// The tag keys in the index
    pub tag_keys: Vec<String>,
    /// The estimated number of rows in the index
    pub estimated_rows: usize,
}

impl IndexInfo {
    /// Creates a new index info
    pub fn new(name: String, time_range: TimeRange, tag_keys: Vec<String>, estimated_rows: usize) -> Self {
        Self {
            name,
            time_range,
            tag_keys,
            estimated_rows,
        }
    }

    /// Updates the index info with a new data point
    pub fn update(&mut self, point: &DataPoint) {
        // Implementation of update method
    }

    /// Checks if the index contains a timestamp
    pub fn contains_timestamp(&self, timestamp: i64) -> bool {
        // Implementation of contains_timestamp method
        false
    }

    /// Checks if the index overlaps with a time range
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        // Implementation of overlaps method
        false
    }

    pub fn covers_time_range(&self, query_range: &TimeRange) -> bool {
        match (&self.time_range, query_range) {
            (TimeRange::Absolute { start: s1, end: e1 }, TimeRange::Absolute { start: s2, end: e2 }) => {
                s2 >= s1 && e2 <= e1
            }
            (TimeRange::Absolute { start: s1, end: e1 }, TimeRange::Last { duration }) => {
                let query_start = e1 - duration;
                query_start >= *s1
            }
            (TimeRange::Absolute { start: s1, end: e1 }, TimeRange::Relative { offset, duration }) => {
                let query_start = e1 - offset;
                let query_end = query_start + duration;
                query_start >= *s1 && query_end <= *e1
            }
            _ => false,
        }
    }

    pub fn can_satisfy_filter(&self, filter: &FilterExpr) -> bool {
        match filter {
            FilterExpr::TagFilter(tag_filter) => {
                self.tag_keys.contains(&tag_filter.key)
            }
            FilterExpr::And(left, right) => {
                self.can_satisfy_filter(left) && self.can_satisfy_filter(right)
            }
            FilterExpr::Or(left, right) => {
                self.can_satisfy_filter(left) && self.can_satisfy_filter(right)
            }
            FilterExpr::Not(expr) => {
                self.can_satisfy_filter(expr)
            }
        }
    }

    pub fn estimate_rows_in_range(&self, range: &TimeRange) -> usize {
        match (&self.time_range, range) {
            (TimeRange::Absolute { start: s1, end: e1 }, TimeRange::Absolute { start: s2, end: e2 }) => {
                let total_duration = (e1 - s1) as f64;
                let query_duration = (e2 - s2) as f64;
                ((self.estimated_rows as f64 * query_duration) / total_duration) as usize
            }
            (TimeRange::Absolute { start: s1, end: e1 }, TimeRange::Last { duration }) => {
                let total_duration = (e1 - s1) as f64;
                let query_duration = *duration as f64;
                ((self.estimated_rows as f64 * query_duration) / total_duration) as usize
            }
            (TimeRange::Absolute { start: s1, end: e1 }, TimeRange::Relative { offset, duration }) => {
                let total_duration = (e1 - s1) as f64;
                let query_duration = *duration as f64;
                ((self.estimated_rows as f64 * query_duration) / total_duration) as usize
            }
            _ => self.estimated_rows,
        }
    }

    pub fn estimate_filter_selectivity(&self, filter: &FilterExpr) -> f64 {
        match filter {
            FilterExpr::TagFilter(tag_filter) => {
                match tag_filter.op {
                    TagFilterOp::Eq => 0.1,
                    TagFilterOp::Neq => 0.9,
                    TagFilterOp::Regex => 0.3,
                    TagFilterOp::NotRegex => 0.7,
                }
            }
            FilterExpr::And(left, right) => {
                self.estimate_filter_selectivity(left) * self.estimate_filter_selectivity(right)
            }
            FilterExpr::Or(left, right) => {
                let s1 = self.estimate_filter_selectivity(left);
                let s2 = self.estimate_filter_selectivity(right);
                s1 + s2 - (s1 * s2)
            }
            FilterExpr::Not(expr) => {
                1.0 - self.estimate_filter_selectivity(expr)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> IndexInfo {
        IndexInfo {
            name: "test_index".to_string(),
            time_range: TimeRange::Absolute {
                start: 0,
                end: 1000000000000,
            },
            tag_keys: vec!["region".to_string(), "env".to_string()],
            estimated_rows: 1000,
        }
    }

    #[test]
    fn test_time_range_coverage() {
        let index = create_test_index();
        let query_range = TimeRange::Absolute {
            start: 100000000000,
            end: 200000000000,
        };
        assert!(index.covers_time_range(&query_range));
        let query_range = TimeRange::Last {
            duration: 360000000000,
        };
        assert!(index.covers_time_range(&query_range));
        let query_range = TimeRange::Absolute {
            start: 2000000000000,
            end: 3000000000000,
        };
        assert!(!index.covers_time_range(&query_range));
    }

    #[test]
    fn test_filter_satisfaction() {
        let index = create_test_index();
        let filter = FilterExpr::TagFilter(TagFilter {
            key: "region".to_string(),
            op: TagFilterOp::Eq,
            value: "us-west".to_string(),
        });
        assert!(index.can_satisfy_filter(&filter));
        let filter = FilterExpr::TagFilter(TagFilter {
            key: "datacenter".to_string(),
            op: TagFilterOp::Eq,
            value: "dc1".to_string(),
        });
        assert!(!index.can_satisfy_filter(&filter));
        let filter = FilterExpr::And(
            Box::new(FilterExpr::TagFilter(TagFilter {
                key: "region".to_string(),
                op: TagFilterOp::Eq,
                value: "us-west".to_string(),
            })),
            Box::new(FilterExpr::TagFilter(TagFilter {
                key: "env".to_string(),
                op: TagFilterOp::Eq,
                value: "prod".to_string(),
            })),
        );
        assert!(index.can_satisfy_filter(&filter));
    }

    #[test]
    fn test_row_estimation() {
        let index = create_test_index();
        let range = TimeRange::Absolute {
            start: 100000000000,
            end: 200000000000,
        };
        let estimate = index.estimate_rows_in_range(&range);
        assert!(estimate > 0 && estimate < index.estimated_rows);
        let range = TimeRange::Last {
            duration: 360000000000,
        };
        let estimate = index.estimate_rows_in_range(&range);
        assert!(estimate > 0 && estimate < index.estimated_rows);
    }

    #[test]
    fn test_filter_selectivity() {
        let index = create_test_index();
        let filter = FilterExpr::TagFilter(TagFilter {
            key: "region".to_string(),
            op: TagFilterOp::Eq,
            value: "us-west".to_string(),
        });
        let selectivity = index.estimate_filter_selectivity(&filter);
        assert!(selectivity > 0.0 && selectivity < 1.0);
        let filter = FilterExpr::And(
            Box::new(FilterExpr::TagFilter(TagFilter {
                key: "region".to_string(),
                op: TagFilterOp::Eq,
                value: "us-west".to_string(),
            })),
            Box::new(FilterExpr::TagFilter(TagFilter {
                key: "env".to_string(),
                op: TagFilterOp::Eq,
                value: "prod".to_string(),
            })),
        );
        let selectivity = index.estimate_filter_selectivity(&filter);
        assert!(selectivity > 0.0 && selectivity < 1.0);
    }
} 