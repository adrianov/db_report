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
- Search capability to find specific values across tables and columns
- Works with PostgreSQL, MySQL, and SQLite (via Sequel adapters). **Note:** Currently, thorough testing has primarily focused on PostgreSQL.
- Output as JSON, compact, summary, or GPT-friendly format
- Debugging mode with detailed SQL logging
- Colorized console output
- Parallel processing for faster analysis of multiple tables (enabled by default)

## Installation

1.  Clone this repository
2.  Ensure you have Ruby and Bundler installed.
3.  Install dependencies:

```bash
bundle install
```

### ZSH Alias (Optional)

For convenience, you can add an alias to your `.zshrc` file:

```bash
# Add to your ~/.zshrc
alias dbreport="/path/to/db_report.rb"
```

Then reload your shell configuration:

```bash
source ~/.zshrc
```

## Usage

Run the script with:

```bash
ruby db_report.rb [options]
```

Or if you added the alias:

```bash
dbreport [options]
```

### Command Line Options

```
-d, --database-url URL           Database connection URL (Sequel format, overrides config)
-e, --environment ENV            Environment section in config/database.yml (default: development)
-b, --database NAME              Specific database name (overrides config/URL database component)
-l, --list-databases             List available databases and exit
-o, --output FILE                Output report to file instead of stdout
-t, --tables TBLS                Analyze only specific tables (comma-separated)
-f, --format FMT                 Output format: json/summary/gpt/compact (default: compact)
-p, --pool SIZE                  Max connections pool size (default: 5)
    --timeout SECS               Database connection timeout (default: 10)
    --parallel-processes NUM     Number of parallel processes to use for table analysis (default: auto-detect)
-s, --search-value VALUE         Search for specific value in all tables and columns
    --debug                      Show detailed debug information and SQL logging
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

Specify the number of parallel processes to use:
```bash
ruby db_report.rb --parallel-processes 4
```

Enable debug logging:
```bash
ruby db_report.rb --debug
```

Search for a specific value across all tables:
```bash
ruby db_report.rb --search-value "Freund"
```

Search for a value in specific tables:
```bash
ruby db_report.rb --search-value "Freund" --tables words,translations
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

### Parallel Processing

Parallel processing is enabled by default to significantly speed up analysis when dealing with multiple tables. Each table is analyzed in a separate process, utilizing all available CPU cores.

- By default, the number of parallel processes is auto-detected based on your system's processor count.
- You can specify a custom number of processes with `--parallel-processes NUM`.
- Each process creates its own database connection, so ensure your database server can handle the additional connections.

**Note:** When using parallel processing, the total number of database connections can be high (processes × pool size). Adjust your database server's connection limit accordingly.

### Search Feature

The search feature allows you to find specific values within your database tables and columns. This is useful for locating where certain data is stored or to verify the existence of specific values.

#### Usage

```bash
ruby db_report.rb --search-value VALUE [options]
```

The search feature can be combined with other options to narrow down the scope:

```bash
ruby db_report.rb --search-value "Freund" --tables words
```

#### Example Output

When a search is performed, the tool will output a summary of where the value was found:

```
Table: words (Rows: 6)
Column           Type                          Nulls (%)   Distinct   Stats                                                                                     Found
id               uuid                          0 (0.0%)               Min: 29cc3963-690..., Max: ca114a89-e07..., AvgLen: 36.0
word             character varying             0 (0.0%)    6          Min: Freund, Max: мир, AvgLen: 4.8, MostFreq: Freund (1)                                  YES
language         character varying             0 (0.0%)    6          Min: de, Max: ru, AvgLen: 2.0, MostFreq: NULL (1)
translations     text[]                        0 (0.0%)               Min: 3, Max: 10, AvgItems: 4.2
transcription    character varying             0 (0.0%)    6          Min: a.ˈmoɾ, Max: ˈka.za, AvgLen: 5.8, MostFreq: NULL (1)
etymology        text                          0 (0.0%)    6          Min: From Latin a..., Max: Old English hāl, AvgLen: 24.3, MostFreq: From Latin a... (1)
synonyms         text[]                        0 (0.0%)               Min: 2, Max: 3, AvgItems: 2.7
antonyms         text[]                        0 (0.0%)               Min: 1, Max: 2, AvgItems: 1.8
examples         text[]                        0 (0.0%)               Min: 1, Max: 3, AvgItems: 2.7
relatedPhrases   text[]                        0 (0.0%)               Min: 1, Max: 2, AvgItems: 1.8
isActive         boolean                       0 (0.0%)    1          Min: 1, Max: 1, True%: 100.0, MostFreq: true (6)
createdAt        timestamp without time zone   0 (0.0%)    6          Min: 2025-04-05T0..., Max: 2025-04-05T0..., MostFreq: 2025-04-05 0... (1)
updatedAt        timestamp without time zone   0 (0.0%)    6          Min: 2025-04-05T0..., Max: 2025-04-05T0..., MostFreq: NULL (1)


Search Summary
Value 'Freund' found in 1 column(s):
  - words.word
```

The `Found` column in the output indicates which columns contain the search value, and a summary is provided at the end showing all matches.

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
    "analysis_duration_seconds": 15.72,
    "parallel_processing": true,
    "parallel_processes": 8
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
- Parallel gem (`~> 1.0`) for parallel processing

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
