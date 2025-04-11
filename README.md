# Database Report Tool (Sequel Edition)

A Ruby utility for analyzing database structure and generating detailed statistics about tables and columns using the Sequel gem.

## Features

- Analyzes all tables in a database or specific tables (including handling schemas)
- Provides statistics for each column including:
  - Abstract data type (`:string`, `:integer`, etc.) and DB-specific type (`VARCHAR`, `INT`, etc.)
  - Min/max/avg values or lengths/item counts
  - Null count and percentage
  - Most frequent and least frequent values (for applicable types)
  - Distinct value count (for applicable types)
  - Type-specific metrics (array lengths, boolean distributions, etc.)
- Works with PostgreSQL, MySQL, and SQLite (via Sequel adapters). **Note:** Currently, thorough testing has primarily focused on PostgreSQL.
- Output as JSON or a human-readable summary format
- Debugging mode with detailed SQL logging
- Colorized console output

## Installation

1.  Clone this repository
2.  Ensure you have Ruby and Bundler installed.
3.  Install dependencies:

```bash
bundle install
```

## Usage

Run the script with:

```bash
ruby db_report.rb [options]
```

### Command Line Options

```
-d, --database-url URL           Database connection URL (Sequel format, overrides config)
-e, --environment ENV            Environment section in config/database.yml (default: development)
-b, --database NAME              Specific database name (overrides config/URL)
-l, --list-databases             List available databases and exit
-o, --output FILE                Output report to file instead of stdout
-t, --tables TBL1,TBL2,...       Analyze only specific tables (comma-separated, use schema.table if needed)
-f, --format FMT                 Output format: json/summary (default: json)
-p, --pool SIZE                  Max connections pool size (default: 5)
--timeout SECS                   Database connection timeout (default: 10)
--debug                          Show detailed debug information and SQL logging
-h, --help                       Show this help message
```

### Examples

Analyze all tables found in the default search path for the development environment:
```bash
ruby db_report.rb
```

Analyze specific tables, including one with a schema:
```bash
ruby db_report.rb -t users,analytics.events,products
```

Connect using a specific database URL:
```bash
ruby db_report.rb -d postgres://user:pass@host:port/my_db
```

Output a summary report to the console:
```bash
ruby db_report.rb -f summary
```

Output a JSON report to a file:
```bash
ruby db_report.rb -o reports/db_stats_$(date +%F).json
```

Enable debug logging:
```bash
ruby db_report.rb --debug
```

### Database Connection

The script determines the database connection configuration using the following priority:

1.  `--database-url` command line option: Uses the provided Sequel-compatible URL directly.
2.  `DATABASE_URL` environment variable: Uses the URL from the environment.
3.  `config/database.yml`: Loads configuration from this file based on the `--environment` (or default 'development').
    - It can parse standard Rails YAML structure, including ERB tags.
    - If both `DATABASE_URL` and `config/database.yml` are present, `DATABASE_URL` parameters will override file parameters.
4.  The `--database` option overrides the database name specified in any of the above configurations.
5.  The `--pool` and `--timeout` options override defaults or values from the configuration sources.

**Note for Rails Users:** The script is designed to work seamlessly with standard Rails `config/database.yml` files and respects the `RAILS_ENV` environment variable for selecting the default environment if `--environment` is not provided.

## Output Example (JSON)

The JSON output includes metadata and detailed stats per table/column:

```json
{
  "metadata": {
    "generated_at": "2024-07-28T10:00:00+00:00",
    "database_adapter": "postgresql",
    "database_type": "postgres",
    "database_version": "PostgreSQL 15.3 (...) ...",
    "analyzed_tables": [
      "public.users",
      "public.orders"
    ],
    "analysis_duration_seconds": 15.72
  },
  "tables": {
    "public.users": {
      "id": {
        "type": "integer",
        "db_type": "integer",
        "count": 10000,
        "null_count": 0,
        "min": 1,
        "max": 10000,
        "is_unique": true
      },
      "email": {
        "type": "string",
        "db_type": "character varying",
        "count": 10000,
        "null_count": 0,
        "min": 12, // Min length
        "max": 255, // Max length
        "avg": 32.4, // Avg length
        "distinct_count": 9995,
        "most_frequent": {
          "test@example.com": 5
        },
        "least_frequent": {
          "user9999@example.com": 1
        },
        "is_unique": false
      },
      "created_at": {
        "type": "datetime",
        "db_type": "timestamp without time zone",
        "count": 10000,
        "null_count": 0,
        "min": "2023-01-10T00:00:00+00:00",
        "max": "2024-07-28T09:59:00+00:00",
        "distinct_count": 9876,
        "is_unique": false
      }
      // ... other columns
    }
    // ... other tables
  }
}
```

## Requirements

- Ruby 2.7+
- Bundler
- Sequel gem (`~> 5.0`)
- Corresponding database adapter gem (`pg`, `mysql2`, or `sqlite3`)

## License

MIT License

Copyright (c) 2025 Peter Adrianov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
