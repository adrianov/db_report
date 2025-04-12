# DB Report: Database Analyzer

## 1. High-level Goals

DB Report is a Ruby-based tool designed to analyze database structure and content, providing insights and statistics to help developers, database administrators, and analysts better understand their databases. The key goals of the application include:

- **Database Structure Exploration**: Analyze tables, views, and materialized views to understand schema structure
- **Data Pattern Discovery**: Identify patterns, value distributions, and anomalies in database content
- **Performance Insight**: Gain insights that can help with database optimization
- **Cross-Platform Compatibility**: Support for PostgreSQL databases (with extensibility for other database types)
- **Efficient Analysis**: Parallel processing for analyzing large databases efficiently
- **Flexible Output**: Multiple output formats for different use cases (JSON, summary text, compact view, GPT-friendly format)

## 2. User-focused Functionality

### 2.1 Core Features

- **Database Analysis**: Analyze table structure, column types, nullability, and uniqueness constraints
- **Statistical Analysis**: Generate statistics about column values (min/max/avg, frequency distributions)
- **Value Searching**: Search for specific values across all tables and columns
- **View Support**: Analyze both regular and materialized views, including their definitions and dependencies
- **Data Sampling**: Intelligent sampling of large tables and views for efficient analysis
- **Parallel Processing**: Analyze multiple tables simultaneously for faster results

### 2.2 Output Formats

- **Summary Format**: Detailed text output for human reading
- **Compact Format**: Condensed tabular output for terminal display
- **JSON Format**: Structured machine-readable output that can be further processed
- **GPT Format**: Markdown-formatted output optimized for AI tools and chatbots

### 2.3 Command-Line Options

| Option | Description |
|--------|-------------|
| `-d, --database-url URL` | Database connection URL (Sequel format) |
| `-e, --environment ENV` | Environment section in config/database.yml |
| `-b, --database NAME` | Specific database name (overrides config/URL) |
| `-l, --list-databases` | List available databases and exit |
| `-o, --output FILE` | Output report to file instead of stdout |
| `-t, --tables TBLS` | Analyze only specific tables (comma-separated) |
| `-f, --format FMT` | Output format: json/summary/gpt/compact |
| `-p, --pool SIZE` | Max connections pool size |
| `--timeout SECS` | Database connection timeout |
| `--parallel-processes NUM` | Number of parallel processes to use |
| `-s, --search-value VALUE` | Search for specific value in all tables |
| `--include-views` | Include regular views in analysis |
| `--include-materialized-views` | Include materialized views in analysis |
| `--skip-tables` | Skip analysis of regular tables |
| `--refresh-materialized-view=NAME` | Refresh the materialized view before analysis |
| `--debug` | Show detailed debug information |
| `-h, --help` | Show help message |

### 2.4 Usage Examples

#### Basic Database Analysis

```bash
# Analyze all tables in the database specified in config/database.yml
db_report

# Analyze a specific database using a connection URL
db_report --database-url postgres://user:password@localhost/mydb

# Analyze specific tables only
db_report --tables users,products,orders

# Output report to JSON file
db_report --format json --output report.json
```

#### View and Materialized View Analysis

```bash
# Include regular views in analysis
db_report --include-views

# Include materialized views in analysis
db_report --include-materialized-views

# Include both types of views
db_report --include-views --include-materialized-views

# Analyze only views, excluding tables
db_report --include-views --include-materialized-views --skip-tables
```

#### Search for Values

```bash
# Search for a specific value across all tables
db_report --search-value "John"

# Search in specific tables only
db_report --tables users,customers --search-value "john@example.com"
```

#### Performance Optimization

```bash
# Specify number of parallel processes for analysis
db_report --parallel-processes 4

# Increase connection pool size for better parallelism
db_report --pool 8
```

## 3. Technical Implementation Details

### 3.1 Architecture

DB Report follows a modular architecture that separates concerns into distinct components:

1. **DbReportApp**: Main application controller that manages overall workflow
2. **Config**: Configuration management and database connection settings
3. **Connector**: Database connection handling
4. **Analyzer**: Core analysis engine
   - **Base**: Foundation for all analysis operations
   - **SchemaHelper**: Schema inspection and metadata retrieval
   - **FrequencyAnalyzer**: Frequency distribution analysis
   - **AggregateFunctions**: Aggregate statistical calculations
5. **Reporter**: Output formatting and report generation

### 3.2 Database Connection

- Uses Sequel gem for database connectivity
- Supports connection via:
  - Direct URL (command-line option)
  - Environment variable (`DATABASE_URL`)
  - Configuration file (`config/database.yml`)
- Connection pooling for parallel processing
- Connection validation for PostgreSQL

### 3.3 Analysis Process

1. **Table Selection**: Determine which tables to analyze based on user options
2. **Schema Analysis**: Extract schema information (columns, types, constraints)
3. **Statistical Analysis**: Calculate aggregate statistics for each column
   - Basic stats: min/max/avg, null count, distinct values
   - Type-specific analysis: string lengths, number distributions, etc.
4. **Frequency Analysis**: Analyze value frequency distribution
   - Most common values
   - Least common values
   - Special handling for JSON and array data
5. **View Analysis**:
   - View definition extraction
   - Dependency tracking
   - Performance-optimized sampling
   - Materialized view refresh status

### 3.4 Parallel Processing

- Uses Ruby's `parallel` gem for multi-process parallelism
- Creates separate database connections for each worker process
- Automatically determines optimal number of processes based on CPU cores
- Thread-safe design to prevent race conditions

### 3.5 Key Algorithms

1. **Batch Frequency Analysis**: Groups columns by type for efficient querying
2. **Adaptive Sampling**: Adjusts sample size based on table/view size and complexity
3. **Value Search Optimization**: Uses combined UNION queries to efficiently search across columns
4. **View Complexity Detection**: Identifies complex view definitions to optimize analysis approach

### 3.6 Technologies Used

- **Language**: Ruby (2.5+)
- **Database Connectivity**: Sequel ORM
- **Database Adapters**:
  - PostgreSQL (`pg` gem)
  - MySQL optional support (`mysql2` gem)
  - SQLite optional support (`sqlite3` gem)
- **Utility Libraries**:
  - `colorize` for terminal output formatting
  - `terminal-table` for tabular display
  - `progress_bar` for visual feedback
  - `parallel` for multi-process execution

## 4. Building and Running

### 4.1 Prerequisites

- Ruby 2.5 or newer
- Database adapter gem(s) for your database type:
  - PostgreSQL: `pg` gem
  - MySQL: `mysql2` gem (optional)
  - SQLite: `sqlite3` gem (optional)
- Required gems (bundled with the application)

### 4.2 Installation

```bash
# Clone the repository
git clone https://github.com/user/db_report.git
cd db_report

# Install dependencies
bundle install
```

### 4.3 Configuration

Create a `config/database.yml` file with your database connection settings:

```yaml
development:
  primary:
    adapter: postgresql
    host: localhost
    database: my_development_db
    username: postgres
    password: password
    
production:
  primary:
    adapter: postgresql
    host: db.example.com
    database: my_production_db
    username: app_user
    password: <%= ENV['DB_PASSWORD'] %>
```

### 4.4 Running the Application

```bash
# Basic usage with config file
ruby db_report.rb

# Or make it executable and run directly
chmod +x db_report.rb
./db_report.rb
```

### 4.5 Output Examples

#### Compact Format (Default)
```
Database Analysis Summary
Parameter        Value
Adapter          postgres
Type             postgres
Version          PostgreSQL 14.5
Generated        2025-04-12T16:20:47+03:00
Duration         1.25s
Tables Analyzed  3

Table: public.users (Rows: 1250)
Column           Type            Nulls (%)      Stats                 Found
id               integer         0 (0.0%)       Min: 1, Max: 1250     
name             varchar(100)    0 (0.0%)       AvgLen: 12.5          YES
email            varchar(150)    12 (1.0%)      AvgLen: 18.7          

## 5. Performance Considerations

### 5.1 General Performance Optimization

- **Parallel Processing**: The application uses parallel processes to analyze multiple tables simultaneously, significantly improving performance on multi-core systems.
- **Connection Pooling**: Configure appropriate pool sizes (`--pool` option) based on your hardware and database server capacity.
- **Memory Usage**: For very large databases, monitor memory usage as statistics for many columns are stored in memory during analysis.
- **Database Impact**: The analysis process issues multiple queries that can impact database performance. Consider scheduling analysis during off-peak hours for production databases.

### 5.2 Large Database Analysis

When analyzing large databases (tables with millions of rows), consider these optimizations:

- **Table Selection**: Use the `-t` option to analyze only specific tables rather than all tables.
- **Sampling**: For very large tables, the tool automatically samples data rather than reading entire tables.
- **Column Focus**: For frequency analysis, the tool prioritizes columns likely to have meaningful distributions.
- **Batch Processing**: Multiple columns with compatible types are analyzed in batches using UNION queries.
- **Schema-Only Analysis**: Consider analyzing schema information first without data statistics if you have very large tables.

### 5.3 View-Specific Performance

Views have special performance considerations:

#### Regular Views

- Views execute their underlying query when accessed, potentially causing resource-intensive operations.
- Complex views with multiple joins or aggregations are automatically sampled at lower rates.
- Views with recursive CTEs, window functions, or LATERAL joins receive special handling to prevent excessive resource usage.
- For views with complex definitions, consider using `--view-sample-size` to explicitly control sampling.

#### Materialized Views

- Materialized views store pre-computed results, making analysis more efficient than regular views.
- The tool reports refresh timestamps, helping identify stale materialized views.
- Use `--refresh-materialized-view` to refresh a materialized view before analysis if needed.
- Analysis of large materialized views uses larger sample sizes than regular views, as the data is pre-computed.

### 5.4 Connection Handling

- For parallel processing, the tool creates multiple database connections (one per worker process).
- Each connection is properly validated and released after use.
- The connection timeout can be configured (`--timeout` option) to handle slow network connections.
- Connection pooling is managed to prevent exhausting available database connections.

## 6. Best Practices

### 6.1 Effective Usage Patterns

- **Start Small**: Begin with a subset of tables to understand the output format and statistics.
- **Regular Analysis**: Schedule periodic analyses to track how your database evolves over time.
- **Output Management**: Use `-o` to save outputs and compare results between runs.
- **Format Selection**: Choose the most appropriate output format for your needs:
  - `compact` for quick terminal viewing
  - `json` for further data processing
  - `gpt` for sharing with AI assistants
  - `summary` for detailed human reading

### 6.2 Workflow Integration

- **CI/CD Integration**: Add database analysis as part of your continuous integration pipeline.
- **Migration Validation**: Run analysis before and after complex schema migrations to verify data integrity.
- **Documentation Generation**: Use the output to generate up-to-date database documentation.
- **Monitoring Integration**: Compare analysis results with monitoring systems to correlate performance issues with data patterns.

### 6.3 Security Considerations

- **Connection Strings**: Avoid hardcoding database credentials; use environment variables or secure configuration stores.
- **Output Files**: Be cautious with report outputs as they may contain sensitive data patterns.
- **Search Values**: When searching for values, be aware that the search value appears in logs and reports.
- **Access Control**: Ensure the user running the application has appropriate read-only access to the database.

### 6.4 Troubleshooting

#### Common Issues and Solutions

- **Connection Problems**:
  - Verify database credentials and connection parameters
  - Check network connectivity and firewall settings
  - Ensure the database server is running and accepting connections

- **Performance Issues**:
  - Try reducing the number of parallel processes (`--parallel-processes`)
  - Analyze only specific tables with `-t` option
  - Increase connection timeout with `--timeout` for slow networks
  - Monitor database server load during analysis

- **Memory Usage**:
  - Analyze fewer tables at once for large databases
  - Ensure adequate memory on the machine running the analysis
  - Close other memory-intensive applications during analysis

- **Errors During Analysis**:
  - Use `--debug` to get detailed error information
  - Check database server logs for additional error context
  - Verify that database user has proper permissions on all tables/views

- **View-Specific Issues**:
  - For complex view analysis failures, try `--view-sample-size` with a lower value
  - If materialized view refresh fails, check if you have proper permissions
  - For dependency tracking errors, verify that all referenced objects exist

## 7. Future Development Roadmap

The DB Report tool has several planned enhancements:

- **Additional Database Support**: Expand full feature support for MySQL, Oracle, and SQL Server
- **Schema Diff**: Compare schema and statistics between different database environments
- **Historical Trend Analysis**: Track changes in database statistics over time
- **Advanced Data Pattern Detection**: Identify potential data quality issues and anomalies
- **Index Analysis**: Recommendations for index creation based on data patterns
- **Foreign Key Visualization**: Generate relationship diagrams based on schema
- **Performance Query Analysis**: Identify potentially slow queries based on data distribution
- **Enhanced View Support**:
  - Recursive dependency analysis for complex view hierarchies
  - View refresh scheduling recommendations
  - Query performance analysis for views
  - Comparison of materialized vs regular views for the same query

---

*This document was last updated on 2025-04-12*
