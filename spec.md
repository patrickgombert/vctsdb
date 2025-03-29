# Time Series Database Specification

## Overview
The goal is to build a single-node, high-performance time series database for system metrics with the following key features:
- Infinite data retention.
- High cardinality data support.
- Near real-time data ingestion and querying.
- Aggregations (averages, percentiles, rates of change) as core features.
- Custom query language, resembling InfluxQL.
- Plug-and-play architecture for multiple ingestion formats.

This database will run on a laptop with low peak load requirements, focusing on efficient storage and querying for high cardinality system metrics data.

---

## Key Requirements

### 1. **Data Storage and Retention**
- **Storage Model**: Traditional time-series storage model (e.g., log-structured storage, LSM trees).
- **Retention**: The database will retain all raw data indefinitely without transformation.
- **Cardinality**: The system must handle high cardinality data without specialized optimizations.
  
### 2. **Data Ingestion**
- **Multiple Formats**: Support for multiple ingestion formats (e.g., JSON, CSV, protobuf, or any custom formats) in a plug-and-play manner.
- **Ingestion Mechanism**: Data must be ingested in real-time, but high performance is the priority over strict real-time needs.

### 3. **Query Language**
- **Custom Query Language**: The database will have a custom query language that resembles InfluxQL to some extent.
  - Should support complex aggregations such as averages, percentiles, and rates of change.
  - Basic syntax should include `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `ORDER BY`, and aggregation functions.
  - Should support time-based functions (e.g., time intervals, rolling windows).
  - Data correlation across different series should be possible.

### 4. **Aggregations**
- **Types of Aggregations**: 
  - Averages
  - Percentiles (configurable)
  - Rates of change (derivatives, differences)
- **Query Performance**: Aggregations should be optimized for performance, even with high cardinality data.
  
### 5. **Error Handling and Recovery**
- **Crash Recovery**: The system must support crash recovery. All acknowledged data should be durably persisted.
- **Data Durability**: The database should ensure that once data is acknowledged, it is persisted and won't be lost in the event of a crash.
- **Error Handling**: Handle edge cases such as missing data, invalid queries, and ingestion failures with graceful error reporting.

### 6. **Performance and Resource Management**
- **Low Resource Requirements**: The database will run on a laptop and should be lightweight with minimal resource consumption.
- **Automatic Resource Scaling**: It should automatically scale its resource usage based on available memory, CPU, and disk space, without requiring manual configuration.
  
### 7. **Scalability**
- **Single-Node Architecture**: For now, the database will be single-node. There are no immediate requirements for multi-node support, but future scalability should be considered if needed.
  
---

## Architecture Overview

### 1. **Storage Engine**
- Use a traditional time-series storage engine, such as:
  - **Log-Structured Merge Trees (LSM Trees)** for efficient writes.
  - **Columnar storage** to optimize read performance for time-series data.
  
### 2. **Data Ingestion Layer**
- Support multiple ingestion formats with a plug-and-play architecture.
- Utilize a modular ingestion pipeline that can accept various formats and transform them into the database’s internal format.
  
### 3. **Query Processor**
- Implement a custom query engine that supports:
  - **SELECT** queries with aggregation functions.
  - **Time-based queries** for retrieving metrics over specific periods.
  - **Correlations** across multiple metrics or series.
  
### 4. **Persistence Layer**
- Use durable storage mechanisms (e.g., write-ahead logs, data files on disk) to ensure that acknowledged data is never lost.
  
### 5. **Recovery Mechanism**
- Implement crash recovery using techniques like write-ahead logs (WAL) or transaction logs to ensure data consistency in case of failure.

---

## Data Handling and Performance Optimization

### 1. **Compression**
- Apply basic compression techniques to minimize storage usage, particularly for repetitive data.
- Optimize for storage efficiency without compromising query performance.

### 2. **Indexing**
- Use a traditional index structure, such as a time-based index, for quick lookups.
- Avoid complex multi-dimensional indexing to keep the system simple and performant.

---

## Error Handling Strategies

### 1. **Graceful Error Handling**
- Log errors and provide helpful feedback when queries fail (e.g., invalid syntax, missing data).
- For ingestion failures, retry mechanisms should be in place to ensure data is not lost.
  
### 2. **Crash Recovery**
- Upon restart, the system should reprocess any unacknowledged data from the last successful state.
  
### 3. **Data Integrity Checks**
- Validate the consistency of data during ingestion and ensure that no partial writes are committed.

---

## Testing Plan

### 1. **Unit Tests**
- Test the core database functionalities (data ingestion, query execution, aggregations).
- Unit tests for error handling and edge cases (e.g., invalid data formats, malformed queries).
  
### 2. **Integration Tests**
- Test the ingestion pipeline with various formats (JSON, CSV, etc.).
- Test querying capabilities to ensure proper aggregation and correlation.

### 3. **Performance Tests**
- Test the system with high cardinality data to evaluate query performance and storage efficiency.
- Load test with different data ingestion rates and query complexities.

### 4. **Crash Recovery Tests**
- Simulate crashes and test recovery to ensure no data is lost and the system can resume from the last known state.

---

## Future Considerations
- **Horizontal Scaling**: While the system is designed for single-node, consideration should be given to future scaling mechanisms, such as sharding or replication, if needed.
- **Data Downsampling**: If downsampling is needed in the future, it should be implemented as an optional feature that aggregates historical data into summaries (e.g., daily averages).

---

This specification covers the key requirements and design choices for the time series database. It outlines the features to be implemented, along with a testing strategy to ensure reliability and performance. The developer should have a clear starting point for implementing the system with these guidelines.
