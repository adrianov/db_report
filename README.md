# Database Report Tool

A Ruby utility for analyzing database structure and generating detailed statistics about tables and columns.

## Features

- Analyzes all tables in a database or specific tables
- Provides statistics for each column including:
  - Data type information
  - Min/max/avg values or lengths
  - Null count and frequency
  - Most frequent and least frequent values
  - Type-specific metrics (array lengths, boolean distributions, etc.)
- Supports sampling for large databases
- Works with PostgreSQL, MySQL, and SQLite
- Output as JSON or summary format
- Progress indicators for long operations
- Colorized console output

## Installation

1. Clone this repository
2. Install dependencies:

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
-d, --database-url URL           Database URL
-e, --environment ENV            Rails environment (default: development)
-o, --output FILE                Output to file instead of stdout
-t, --tables TABLE1,TABLE2       Analyze only specific tables (comma-separated)
-s, --sample SIZE                Sample size (rows or percentage for PostgreSQL)
-f, --format FORMAT              Output format: json or summary (default: json)
-h, --help                       Show this help message
```

### Examples

Analyze all tables in development environment:
```bash
ruby db_report.rb
```

Analyze specific tables:
```bash
ruby db_report.rb -t users,orders,products
```

Sample 1000 rows from each table:
```bash
ruby db_report.rb -s 1000
```

Output to a file:
```bash
ruby db_report.rb -o reports/db_stats.json
```

Display human-readable summary:
```bash
ruby db_report.rb -f summary
```

### Database Connection

The script will try to connect to the database in the following order:
1. Using the `DATABASE_URL` environment variable
2. Using the `--database-url` command line option
3. Using the Rails `config/database.yml` file

## Output Example

The JSON output includes:

```json
{
  "metadata": {
    "generated_at": "2023-06-15T14:32:45+00:00",
    "database_adapter": "PostgreSQL",
    "tables_analyzed": 3,
    "sample_size": null
  },
  "users": {
    "id": {
      "type": "integer",
      "min": 1,
      "max": 10045,
      "avg": null,
      "count": 10000,
      "null_count": 0,
      "most_frequent": {},
      "least_frequent": {}
    },
    "email": {
      "type": "string",
      "min": 12,
      "max": 255,
      "avg": 32.4,
      "count": 10000,
      "null_count": 0,
      "most_frequent": {
        "test@example.com": 5
      },
      "least_frequent": {
        "user9999@example.com": 1
      }
    }
  }
}
```

## Requirements

- Ruby 2.7+
- ActiveRecord
- Database adapter gem (pg, mysql2, or sqlite3)

## License

MIT
