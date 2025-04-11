#!/usr/bin/env ruby
# frozen_string_literal: true

require 'sequel' # Changed from active_record
require 'yaml'
require 'optparse'
require 'json'
require 'fileutils'
require 'time'
require 'uri'
require 'erb'
require 'bigdecimal' # Ensure BigDecimal is required for make_json_safe
require 'set'
require 'logger' # For Sequel logging

# Optional dependencies
HAS_COLORIZE = begin
  require 'colorize'
  true
rescue LoadError
  false
end

# Configuration defaults
DEFAULT_ENVIRONMENT = ENV.fetch('RAILS_ENV', 'development')
DEFAULT_POOL_SIZE = ENV.fetch('DB_POOL', 5).to_i # Sequel uses :max_connections
DEFAULT_CONNECT_TIMEOUT = 10 # Sequel uses :connect_timeout
DEFAULT_SAMPLE_SIZE = nil # No sampling by default
DEFAULT_OUTPUT_FORMAT = 'json'
CONFIG_FILE_PATH = File.join(Dir.pwd, 'config', 'database.yml').freeze
OUTPUT_FORMATS = %w[json summary].freeze
# Changed internal tables for Sequel
SEQUEL_INTERNAL_TABLES = [
  'schema_info',                 # Default table name for Sequel migrations (optional)
  'sequel_migrations'            # Common alternative name
].freeze

# Global database connection object
$db = nil
$debug = false # Global debug flag

# --- Utility Methods ---

# Helper method for colored console output
def colored_output(text, color = :default, mode = nil)
  return text unless HAS_COLORIZE

  text = text.colorize(color)
  text = text.send(mode) if mode
  text
end

# Abort script with a colored error message
def abort_with_error(message, error_code = 1)
  abort colored_output("ERROR: #{message}", :red, :bold)
end

# Print a colored warning message
def print_warning(message)
  puts colored_output("Warning: #{message}", :yellow)
end

# Print a colored info message
def print_info(message, color = :green, mode = nil)
  puts colored_output(message, color, mode)
end

# Print debug information if enabled
def print_debug(message)
  puts colored_output(message, :cyan) if $debug
end

# Helper to parse table name into schema and table parts
def parse_table_identifier(table_name_string)
  parts = table_name_string.to_s.split('.', 2)
  if parts.length == 2
    { schema: parts[0].to_sym, table: parts[1].to_sym }
  else
    { schema: nil, table: parts[0].to_sym } # Default to nil schema if not qualified
  end
end

# Helper to create a Sequel identifier (handles qualified names)
def create_sequel_identifier(table_name_string)
  parsed = parse_table_identifier(table_name_string)
  if parsed[:schema]
    Sequel.qualify(parsed[:schema], parsed[:table])
  else
    Sequel.identifier(parsed[:table])
  end
end

# Safely quote a column or table name (less needed with Sequel datasets)
def quote_identifier(identifier)
  # Use Sequel's identifier quoting if needed for raw SQL, otherwise datasets handle it
  $db.literal(Sequel.identifier(identifier))
end

# --- Database Configuration Loading ---

# Load database configuration from config/database.yml
def load_config_from_file(environment)
  return {} unless File.exist?(CONFIG_FILE_PATH)

  begin
    yaml_content = File.read(CONFIG_FILE_PATH)
    # Use safe_load with aliases if available
    db_config = if defined?(YAML.safe_load) && Psych::VERSION >= '3.1'
                  YAML.safe_load(yaml_content, permitted_classes: [Symbol], aliases: true) # Allow symbols for Sequel keys
                else
                  YAML.load(yaml_content) # Fallback for older versions
                end

    print_info "Loaded configuration from #{CONFIG_FILE_PATH}" if db_config

    env_config = db_config[environment.to_s] || db_config[environment.to_sym] # Check string/symbol keys
    return {} unless env_config

    # Handle Rails 6+ multi-db format (less relevant, but keep basic check)
    if env_config.is_a?(Hash) && (env_config.key?('primary') || env_config.key?(:primary))
      primary_key = env_config.key?('primary') ? 'primary' : :primary
      print_info 'Detected Rails-style primary/replica database configuration'
      print_info 'Using primary database configuration'
      env_config[primary_key]
    else
      env_config # Traditional format
    end
  rescue Psych::SyntaxError => e
    print_warning "Error parsing #{CONFIG_FILE_PATH}: #{e.message}"
    {}
  rescue => e
    print_warning "Error processing #{CONFIG_FILE_PATH}: #{e.message}"
    {}
  end
end

# Parse DATABASE_URL into a config hash suitable for Sequel
def parse_database_url(url)
  # Sequel connects directly with the URL string, but we can parse for overrides/merging
  uri = URI.parse(url)
  {
    adapter: uri.scheme == 'postgres' ? 'postgresql' : uri.scheme, # Sequel uses 'postgresql'
    host: uri.host,
    port: uri.port,
    user: uri.user, # Sequel uses 'user'
    password: uri.password,
    database: uri.path&.sub(%r{^/}, ''), # Get path, remove leading /
    url: url # Keep original URL for direct connection if preferred
  }.compact # Remove nil values
rescue URI::InvalidURIError => e
  print_warning "Invalid DATABASE_URL format: #{e.message}"
  {}
end

# Merge DATABASE_URL config with file config (keys to symbols for Sequel)
def merge_configs(file_config, url_config)
  return url_config.transform_keys(&:to_sym) if file_config.empty?
  return file_config.transform_keys(&:to_sym) if url_config.empty?

  print_info 'Merging DATABASE_URL with config/database.yml'
  # Ensure keys are symbols, URL params take precedence
  file_config.transform_keys(&:to_sym).merge(url_config.transform_keys(&:to_sym))
end

# Determine the final database configuration based on priority
def determine_database_config(options)
  file_config = options[:database_url] ? {} : load_config_from_file(options[:environment])

  db_config = if options[:database_url]
                # Prefer direct URL string for Sequel, but parse for overrides
                parsed_url_config = parse_database_url(options[:database_url])
                # If URL is the only source, return it directly
                file_config.empty? ? options[:database_url] : parsed_url_config.transform_keys(&:to_sym)
              elsif ENV['DATABASE_URL']
                url_config = parse_database_url(ENV['DATABASE_URL'])
                # If file_config is empty, return the URL string directly
                file_config.empty? ? ENV['DATABASE_URL'] : merge_configs(file_config, url_config)
              else
                file_config.transform_keys(&:to_sym) # Convert keys to symbols
              end

  # If we ended up with a hash, apply overrides and defaults
  if db_config.is_a?(Hash)
    # Override database name if provided via CLI
    if options[:database]
      print_info "Overriding database name to: #{options[:database]}"
      db_config[:database] = options[:database]
    end

    # Extract DB name from file if missing (e.g., when only URL was provided initially but file exists)
    if db_config[:database].to_s.empty? && File.exist?(CONFIG_FILE_PATH)
      db_name_from_yml = load_config_from_file(options[:environment])['database'] # Use string key for YAML load
      if db_name_from_yml
        db_config[:database] = db_name_from_yml
        print_info "Using database name '#{db_config[:database]}' from config/database.yml"
      end
    end

    # Apply pool size and timeout from options, using Sequel key names
    db_config[:max_connections] = options[:pool] if options[:pool]
    db_config[:connect_timeout] = options[:connect_timeout] if options[:connect_timeout]

    # Final check for database name
    print_warning 'Database name could not be determined.' if !db_config[:database] && db_config[:adapter] != 'sqlite'
  end

  db_config
end

# Process ERB templates in configuration values (remains the same)
def process_erb_in_config(config)
  return config unless config.is_a?(Hash)

  config.transform_values do |value|
    if value.is_a?(String) && value.include?('<%=')
      begin
        ERB.new(value).result
      rescue NameError => e
        print_warning "Could not process ERB in config value: #{e.message}"
        value # Keep original value if ERB fails
      rescue StandardError => e
        print_warning "Error processing ERB: #{e.message}"
        value
      end
    else
      value
    end
  end
end

# --- Database Connection ---

# Get the required gem name for a given adapter (same names work for Sequel)
def adapter_gem_name(adapter)
  case adapter.to_s
  when /mysql/ then 'mysql2'
  when /postgres/ then 'pg'
  when /sqlite/ then 'sqlite3'
  else adapter # For adapters like jdbc, etc.
  end
end

# Check if the required adapter gem is loaded (same logic)
def check_adapter_availability(adapter_name)
  gem_name = adapter_gem_name(adapter_name)
  return unless gem_name

  begin
    require gem_name
    print_info "Loaded #{gem_name} adapter"
  rescue LoadError => e
    # Provide specific instructions for Sequel
    abort_with_error("Couldn't load database adapter gem '#{gem_name}' for Sequel. Please add `gem '#{gem_name}'` to your Gemfile or install it.
Error: #{e.message}")
  end
end

# Build helpful error messages for connection failures (adapted for Sequel)
def build_connection_error_tips(error, config)
  # Extract info from config (hash or URL string)
  username = 'unknown'
  adapter = 'unknown'
  database = 'unknown'
  host = 'unknown'

  if config.is_a?(String) # URL
    begin
      uri = URI.parse(config)
      username = uri.user || 'unknown'
      adapter = uri.scheme
      database = uri.path&.sub(%r{^/}, '') || 'unknown'
      host = uri.host || 'unknown'
    rescue URI::InvalidURIError # rubocop:disable Lint/SuppressedException
    end
  elsif config.is_a?(Hash)
    username = config[:user] || 'unknown' # Sequel uses :user
    adapter = config[:adapter] || 'unknown'
    database = config[:database] || 'unknown'
    host = config[:host] || 'unknown'
  end

  # Basic tips based on common error patterns (may need refinement)
  tips = case error.message
         when /could not connect to server/, /Connection refused/
           "Make sure the database server is running and accessible at #{host}."
         when /database .* does not exist/
           "Database '#{database}' doesn't exist. Create it or check the name."
         when /password authentication failed/, /Access denied for user/, /authentication failed/
           "Authentication failed for user '#{username}'. Check user/password in config."
         when /role .* does not exist/, /Unknown user/ # MySQL uses "Unknown user"
           "Database user '#{username}' doesn't exist. Create it or check the username."
         when /No such file or directory/ # SQLite
           "Database file not found (for SQLite at path: #{database})."
         when /timeout|timed out/
           "Connection timed out. Check network or increase --timeout."
         else
           'Check your database configuration, network access, and ensure the server is running.'
         end

  # Config format tips
  config_tips = if config.is_a?(String)
                  'URL format: adapter://user:password@host:port/database'
                elsif config.is_a?(Hash)
                  required = [:adapter]
                  required << :database unless adapter == 'sqlite' && database == ':memory:'
                  required << :host unless adapter == 'sqlite' # Host needed unless local SQLite
                  missing = required.select { |k| config[k].to_s.empty? }
                  missing.any? ? "Config missing required keys: #{missing.join(', ')}" : nil
                end

  [tips, config_tips].compact.join("
  • ")
end

# Establish the database connection using Sequel
def establish_connection(db_config, pool_size, connect_timeout)
  print_info 'Attempting to connect to database using Sequel...'

  connection_config = db_config # Can be String (URL) or Hash

  # If it's a hash, process ERB and set defaults
  if connection_config.is_a?(Hash)
    connection_config = process_erb_in_config(connection_config)
    connection_config[:max_connections] ||= pool_size # Sequel's pool size key
    connection_config[:connect_timeout] ||= connect_timeout # Sequel's timeout key
    adapter = connection_config[:adapter]
    print_info "Using config: adapter=#{adapter || '?'}, database=#{connection_config[:database] || '?'}, host=#{connection_config[:host] || 'localhost'}, user=#{connection_config[:user] || '?'}"
    print_info "Connection pool size: #{connection_config[:max_connections]}"
    check_adapter_availability(adapter) if adapter
  elsif connection_config.is_a?(String) # URL
    masked_url = connection_config.gsub(/:[^:]*@/, ':*****@')
    print_info "Using connection string: #{masked_url}"
    # Extract adapter from URL to check gem availability
    begin
      adapter = URI.parse(connection_config).scheme
      check_adapter_availability(adapter) if adapter
    rescue URI::InvalidURIError
      print_warning 'Could not parse adapter from URL to check gem.'
    end
    # Add default timeouts to the URL connection if needed (Sequel might handle this)
    connection_config = { conn_str: connection_config, max_connections: pool_size, connect_timeout: connect_timeout }
  else
    abort_with_error 'Invalid database configuration provided.'
  end

  begin
    # Connect using Sequel.connect
    $db = Sequel.connect(connection_config)

    # Add logger if debugging
    $db.loggers << Logger.new($stdout) if $debug

    # Test the connection
    $db.test_connection
    print_info 'Successfully connected to database using Sequel'

  # Catch Sequel's specific connection error
  rescue Sequel::DatabaseConnectionError => e
    error_message = "Failed to connect to database: #{e.message}"
    tips = build_connection_error_tips(e, connection_config)
    abort_with_error "#{error_message}
  • #{tips}"
  rescue LoadError => e # Catch gem load errors here too
     abort_with_error("Failed to load database adapter gem: #{e.message}. Ensure the required gem (e.g., 'pg', 'mysql2', 'sqlite3') is installed.")
  rescue StandardError => e # Catch other potential errors
    error_message = "An unexpected error occurred during connection: #{e.message}"
    tips = build_connection_error_tips(e, connection_config) # Try to provide tips anyway
    abort_with_error "#{error_message}
  • #{tips}"
  end
end

# Display details about the current database connection using Sequel
def display_connection_info
  return unless $debug && $db # Only show if debug is enabled and connected

  begin
    opts = $db.opts
    adapter = $db.adapter_scheme.to_s
    # Try getting database name, fallback to options hash
    database = begin
                 $db.database
               rescue StandardError
                 opts[:database] || 'unknown'
               end
    # Get version using a database query
    version = begin
                $db.database_type == :sqlite ? $db.get(Sequel.lit('sqlite_version()')) : $db.get(Sequel.function(:version))
              rescue StandardError => e
                print_debug "Could not get DB version: #{e.message}"
                'unknown'
              end
    host = opts[:host] || 'localhost'
    port = opts[:port] || 'default'
    user = opts[:user] || '(default)'

    puts colored_output("
--- Database Connection Details (Sequel) ---", :yellow)
    puts "  Adapter:       #{adapter}"
    puts "  Database:      #{database}"
    puts "  Version:       #{version}"
    puts "  Host:          #{host}"
    puts "  Port:          #{port}"
    puts "  User:          #{user}"
    puts "  Pool Size:     #{opts[:max_connections]}"
    puts "------------------------------------------"
  rescue StandardError => e
    print_warning "Could not display full connection details: #{e.message}"
  end
end

# Print basic connected database info using Sequel
def print_database_info
  return unless $db
  begin
    adapter = $db.adapter_scheme.to_s
    database = begin
                 $db.database
               rescue StandardError
                 $db.opts[:database] || 'unknown'
               end
    print_info "Connected to #{adapter} database: #{database}", :cyan
  rescue StandardError => e
    print_warning "Connected to database (could not determine full details: #{e.message})"
  end
end

# List available databases (adapter-specific, using Sequel's fetch)
def list_available_databases
  return [] unless $db
  begin
    query = case $db.database_type # Use Sequel's database_type symbol
            when :postgres
              'SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname'
            when :mysql
              'SHOW DATABASES'
            when :sqlite
              print_warning "SQLite doesn't support listing databases via query."
              return []
            else
              print_warning "Unsupported adapter for listing databases: #{$db.database_type}"
              return []
            end
    # Use fetch to get array of hashes, then pluck the value
    $db.fetch(query).map(&:values).flatten
  rescue Sequel::DatabaseError => e # Catch Sequel DB errors
    print_warning "Error listing databases: #{e.message}"
    []
  rescue StandardError => e
    print_warning "Unexpected error listing databases: #{e.message}"
    []
  end
end

# --- Table Analysis ---

# Get record count for a table using Sequel dataset
def get_table_count(table_name)
  $db[Sequel.identifier(table_name)].count
rescue Sequel::DatabaseError => e
  # Keep the warning message specific about the action
  print_warning "Unable to get count for table #{table_name}: #{e.message}"
  0
rescue StandardError => e
  print_warning "Unexpected error getting count for #{table_name}: #{e.message}"
  0
end

# Build the TABLESAMPLE clause (using Sequel's dataset method)
# This function now returns a dataset modifier lambda/proc
def build_sample_modifier(sample_size, total_count)
  return nil unless sample_size && total_count > 0 && $db.database_type == :postgres

  sample_percentage = (sample_size.to_f / total_count * 100).clamp(0.01, 100)

  # Return a proc that modifies a dataset
  ->(dataset) { dataset.tablesample(:system, sample_percentage) }
rescue StandardError => e
  print_warning "Could not build sample modifier: #{e.message}"
  nil
end

# Helper to safely run aggregate queries using Sequel datasets
def execute_aggregate_query(dataset, select_expressions)
  dataset.select(*select_expressions).first # Returns a hash or nil
rescue Sequel::DatabaseError => e
  puts colored_output("  SQL Aggregate Error: #{e.message.lines.first.strip}", :red)
  print_debug "  Dataset: #{dataset.sql}"
  nil
rescue StandardError => e
  puts colored_output("  Error (Aggregate Query): #{e.message.lines.first.strip}", :red)
  nil
end

# Helper to safely run frequency queries using Sequel datasets
def execute_frequency_query(dataset, column_sym)
  # Use group_and_count for frequency
  dataset.group_and_count(column_sym).all # Returns array of hashes like { col_sym => value, :count => n }
rescue Sequel::DatabaseError => e
  puts colored_output("  SQL Frequency Error: #{e.message.lines.first.strip}", :red)
  print_debug "  Dataset: #{dataset.sql}"
  []
rescue StandardError => e
  puts colored_output("  Error (Frequency Query): #{e.message.lines.first.strip}", :red)
  []
end

# Build SELECT clause parts for aggregate query based on column type using Sequel functions/literals
def build_aggregate_select_parts(column_type, column_sym, adapter_type, is_unique)
  parts = [Sequel.function(:COUNT, column_sym).as(:non_null_count)]

  case column_type
  when :integer, :float, :decimal, :string, :text, :date, :datetime, :time, :boolean, :blob, :enum, :uuid, :inet, :json, :jsonb, :xml # Consolidate types with min/max
    parts += [
      Sequel.function(:MIN, column_sym).as(:min_val),
      Sequel.function(:MAX, column_sym).as(:max_val)
    ]

    # Add AVG for numeric types (only if not unique)
    if !is_unique && [:integer, :float, :decimal].include?(column_type)
      # Cast to float for AVG (double precision is common)
      cast_type = adapter_type == :mysql ? :double : :'double precision'
      parts << Sequel.function(:AVG, Sequel.cast(column_sym, cast_type)).as(:avg_val)
    end

    # Add AVG length for string/text/complex types (only if not unique)
    if !is_unique && [:string, :text, :json, :jsonb, :xml, :blob, :enum].include?(column_type)
      length_expr = case adapter_type
                    when :postgres
                      # Need to cast enum, json, jsonb, xml to text for length
                      cast_target = [:enum, :json, :jsonb, :xml].include?(column_type) ? :text : nil
                      col_expr = cast_target ? Sequel.cast(column_sym, cast_target) : column_sym
                      Sequel.function(:length, col_expr)
                    when :mysql
                      Sequel.function(:length, column_sym) # MySQL length works on most types
                    when :sqlite
                      Sequel.function(:length, column_sym) # SQLite length
                    else
                      Sequel.function(:length, column_sym) # Default assumption
                    end
      parts << Sequel.function(:AVG, length_expr).as(:avg_val) # Avg length
    end

  when :boolean # Handle boolean true count separately
    # SUM(CASE WHEN col THEN 1 ELSE 0 END)
    parts << Sequel.function(:SUM, Sequel.case({ column_sym => 1 }, 0)).as(:true_count)

  when :array # PostgreSQL specific array handling
    if adapter_type == :postgres
      parts += [
        Sequel.function(:MIN, Sequel.function(:array_length, column_sym, 1)).as(:min_val),
        Sequel.function(:MAX, Sequel.function(:array_length, column_sym, 1)).as(:max_val)
      ]
      # Add AVG array length (only if not unique)
      parts << Sequel.function(:AVG, Sequel.function(:array_length, column_sym, 1)).as(:avg_val) if !is_unique
    else
      print_warning "    Skipping array aggregates for non-PostgreSQL DB"
    end
  # No specific aggregates for hstore, uuid, inet beyond min/max unless needed
  else
    print_warning "    Skipping aggregates for unhandled type: #{column_type}" if $debug
  end
  parts
end

# Populate column stats from aggregate query results (keys are symbols from Sequel)
def populate_stats_from_aggregates(col_stats, agg_results, column_type, effective_count)
  return unless agg_results # agg_results is a hash

  non_null_count = agg_results[:non_null_count].to_i
  col_stats[:null_count] = effective_count - non_null_count
  col_stats[:min] = agg_results[:min_val] # nil if not present
  col_stats[:max] = agg_results[:max_val] # nil if not present

  # Handle avg/percentage
  if column_type == :boolean && non_null_count > 0
    true_count = agg_results[:true_count].to_i
    col_stats[:true_percentage] = (true_count.to_f / non_null_count) * 100
  elsif agg_results[:avg_val]
    # Ensure avg is float, handle potential BigDecimal from AVG
    col_stats[:avg] = agg_results[:avg_val].to_f rescue nil
  end

  # Format date/time nicely (Sequel often returns Time/Date objects)
  if %i[date datetime time timestamp].include?(column_type) # Use Sequel's :timestamp type
    col_stats[:min] = col_stats[:min].iso8601 rescue col_stats[:min].to_s if col_stats[:min].respond_to?(:iso8601)
    col_stats[:max] = col_stats[:max].iso8601 rescue col_stats[:max].to_s if col_stats[:max].respond_to?(:iso8601)
  end
end

# Fetch and store most and least frequent values using Sequel datasets
def analyze_frequency(col_stats, column_sym, base_dataset, column_type, unique_single_columns)
  # Skip frequency analysis for columns with a single-column unique index
  return if unique_single_columns.include?(column_sym)

  # Avoid grouping on large text/binary/complex types, allow enum
  groupable = !%i[text blob json jsonb xml array hstore].include?(column_type) # Use Sequel type symbols
  return unless groupable && col_stats[:count] > 0 && !col_stats[:is_unique] # Also skip if known unique

  # Base dataset for frequency (already includes sampling if applied)
  freq_dataset = base_dataset.where { Sequel.~(column_sym => nil) } # WHERE col IS NOT NULL

  # Most Frequent
  most_freq_results = execute_frequency_query(freq_dataset.order(Sequel.desc(:count), column_sym).limit(5), column_sym)
  # Result format: [{column_sym => value, :count => n}, ...]
  col_stats[:most_frequent] = most_freq_results.to_h { |row| [row[column_sym].to_s, row[:count].to_i] }

  # Least Frequent (only if worthwhile)
  # Check distinct count first (can be expensive)
  # Use dataset.distinct(column).count to avoid potential DISTINCT * issues with complex types like JSON
  distinct_count = freq_dataset.distinct(column_sym).count rescue 0

  if distinct_count > 5 # If more than 5 distinct values exist
    least_freq_results = execute_frequency_query(freq_dataset.order(Sequel.asc(:count), column_sym).limit(5), column_sym)
    col_stats[:least_frequent] = least_freq_results.to_h { |row| [row[column_sym].to_s, row[:count].to_i] }
  elsif distinct_count > 0 && distinct_count <= 5 && col_stats[:most_frequent].empty?
    # If few distinct values and most_frequent failed, populate with all values
    all_freq_results = execute_frequency_query(freq_dataset.order(Sequel.asc(:count), column_sym), column_sym)
    col_stats[:most_frequent] = all_freq_results.to_h { |row| [row[column_sym].to_s, row[:count].to_i] }
  end
rescue Sequel::DatabaseError => e
  print_warning "    Frequency analysis failed for #{column_sym}: #{e.message.lines.first.strip}"
rescue StandardError => e
  print_warning "    Unexpected error in frequency analysis for #{column_sym}: #{e.message}"
end

# Analyze a single table using Sequel (needs to handle qualified names)
def analyze_table(table_name_string, sample_size)
  # Use the helper to create a qualified identifier for Sequel operations
  table_identifier = create_sequel_identifier(table_name_string)

  print_info "Analyzing table: #{table_name_string}#{sample_size ? " (sample size: ~#{sample_size})" : ''}", :cyan, :bold
  adapter_type = $db.database_type
  table_stats = {}
  columns_schema = {}

  begin
    # Get schema using the potentially qualified identifier
    columns_schema = $db.schema(table_identifier).to_h { |col_info| [col_info[0], col_info[1]] } # { col_name_sym => {db_type:, type:, ...}}

    # --- Refactored Count and Sampling Logic (Use identifier) ---
    print_debug "PID #{Process.pid}: Getting count for #{table_name_string}..."
    total_count = $db[table_identifier].count # Get total count using the identifier
    print_debug "PID #{Process.pid}: Got count #{total_count} for #{table_name_string}."

    # Base dataset using the identifier
    base_dataset = $db[table_identifier]
    effective_count = total_count
    sample_modifier = nil

    if sample_size && total_count > 0
      sample_modifier = build_sample_modifier(sample_size, total_count)
      if sample_modifier
        # Apply sampling if modifier was created (PG only for now)
        base_dataset = sample_modifier.call(base_dataset)
        effective_count = [sample_size, total_count].min # Use sample size as effective count
      elsif $db.respond_to?(:rand) # Fallback: Use ORDER BY RAND() LIMIT N if adapter supports it (less efficient)
          print_warning "  TABLESAMPLE not supported/failed, attempting ORDER BY RAND() LIMIT #{sample_size} (may be slow/inaccurate)"
          base_dataset = base_dataset.order($db.random).limit(sample_size)
          effective_count = base_dataset.count # Get actual count after sampling
      else
        print_warning "  Sampling not supported for this database or configuration."
        # Proceed without sampling
      end
    elsif sample_size && total_count == 0
      print_debug "  Skipping sampling for empty table: #{table_name_string}"
      effective_count = 0
    end
    # --- End Refactored Logic ---

    # Fetch indexes using the potentially qualified identifier (as string or symbol)
    unique_single_columns = Set.new
    begin
      # Sequel's #indexes expects a symbol or string name. Let's use the original string.
      # Pass the schema explicitly if available for indexes method
      parsed_ident = parse_table_identifier(table_name_string)
      index_opts = parsed_ident[:schema] ? { schema: parsed_ident[:schema] } : {}
      indexes = $db.indexes(parsed_ident[:table], index_opts)

      unique_single_columns = indexes.select { |_name, idx| idx[:unique] && idx[:columns].length == 1 }
                                     .map { |_name, idx| idx[:columns].first } # Get the column symbol
                                     .to_set
      print_debug "  Found unique single-column indexes on: #{unique_single_columns.to_a.join(', ')}" if unique_single_columns.any?
    rescue NotImplementedError
      print_debug "  Index fetching not implemented for this adapter."
    rescue Sequel::DatabaseError => e # Catch DB errors during index fetch
      print_warning "  Could not fetch index information for #{table_name_string}: #{e.message}"
    rescue StandardError => e
      print_warning "  Unexpected error fetching indexes for #{table_name_string}: #{e.message}"
    end

    columns_schema.each do |column_sym, column_info|
      column_type = column_info[:type] # Sequel's abstracted type symbol (:string, :integer, etc.)
      db_type = column_info[:db_type] # Original database type string
      # Determine if the current column is unique based on the fetched indexes
      is_unique = unique_single_columns.include?(column_sym)
      print_info "  - Analyzing column: #{column_sym} (#{column_type}/#{db_type})#{is_unique ? ' (unique)' : ''}", :white

      col_stats = { type: column_type.to_s, db_type: db_type, count: effective_count, null_count: 0,
                    min: nil, max: nil, avg: nil, true_percentage: nil,
                    most_frequent: {}, least_frequent: {}, is_unique: is_unique }

      # Build aggregate expressions for this column, passing the is_unique flag
      select_parts = build_aggregate_select_parts(column_type, column_sym, adapter_type, is_unique)

      if select_parts.any?
        begin
          # Execute aggregate query on the (potentially sampled) base dataset
          agg_results = execute_aggregate_query(base_dataset, select_parts)
          populate_stats_from_aggregates(col_stats, agg_results, column_type, effective_count)
        rescue Sequel::DatabaseError => e # Catch errors during aggregate execution
          if sample_modifier && e.message.match?(/tablesample|syntax error/i)
             print_warning "  TABLESAMPLE query failed for #{column_sym}: #{e.message.lines.first.strip}. Retrying without sampling."
             # Reset dataset to non-sampled using the correct identifier
             base_dataset = $db[table_identifier]
             effective_count = total_count
             retry # Retry aggregates without sampling for this table
          else
            puts colored_output("  SQL Error (Aggregates) for #{column_sym}: #{e.message.lines.first.strip}", :red)
          end
        rescue StandardError => e
          puts colored_output("  Error (Aggregates) for #{column_sym}: #{e.message.lines.first.strip}", :red)
        end
      else
        print_debug("    Skipping aggregate query for #{column_sym} (no parts generated)")
        # Use correct base_dataset for null count fallback
        col_stats[:null_count] = base_dataset.where(column_sym => nil).count rescue effective_count # Estimate nulls if count fails
      end

      # Analyze frequency on the base dataset (which includes sampling if applied)
      analyze_frequency(col_stats, column_sym, base_dataset, column_type, unique_single_columns)

      table_stats[column_sym.to_s] = col_stats # Store stats using string key for JSON consistency
    end

  rescue Sequel::DatabaseError => e # Catch errors like table not found at the start
    # Improve error message slightly
    msg = "Schema/Table Error for '#{table_name_string}': #{e.message.lines.first.strip}"
    puts colored_output(msg, :red)
    return { error: msg }
  rescue StandardError => e
    msg = "Unexpected error analyzing table '#{table_name_string}': #{e.message}"
    puts colored_output(msg, :red)
    puts e.backtrace.join("
") if $debug
    return { error: msg }
  end

  table_stats
end

# --- Output Formatting ---

# Recursive helper to prepare data for JSON generation (handles non-serializable types)
# (Should work fine with Sequel results, Time/Date handling is important)
def make_json_safe(obj)
  case obj
  when Hash then obj.transform_keys(&:to_s).transform_values { |v| make_json_safe(v) }
  when Array then obj.map { |v| make_json_safe(v) }
  when Time, Date then obj.iso8601 rescue obj.to_s
  when Float then obj.nan? || obj.infinite? ? obj.to_s : obj
  when BigDecimal then obj.to_s('F') # Use standard notation
  when Sequel::SQL::Blob then '<Binary Data>' # Represent blobs safely
  else obj
  end
end

# Format stats for summary output (uses :type symbol from Sequel schema)
def format_stats_for_summary(stats)
  parts = []
  type = stats[:type].to_sym rescue nil # Type from col_stats

  # Use abstracted Sequel types
  case type
  when :string, :text, :json, :jsonb, :xml, :blob, :enum, :uuid, :inet # Group string-like/complex types
     # Use avg length if available (key is :avg)
     if stats[:min].nil? && stats[:max].nil? && stats[:avg] # Only avg length available
        parts << "avg_len:#{stats[:avg]&.round(1)}"
     elsif !stats[:min].nil? || !stats[:max].nil? # Min/Max length available
        parts << "len:[min:#{stats[:min]}, max:#{stats[:max]}, avg:#{stats[:avg]&.round(1)}]"
     end
  when :integer, :float, :decimal # Numeric types
    parts << "rng:[min:#{stats[:min]}, max:#{stats[:max]}]" unless stats[:min].nil? && stats[:max].nil?
    parts << "avg:#{stats[:avg]&.round(2)}" if stats[:avg]
  when :boolean
    parts << "true:#{stats[:true_percentage]&.round(1)}%" if stats[:true_percentage]
  when :array # PG Array
    parts << "items:[min:#{stats[:min]}, max:#{stats[:max]}, avg:#{stats[:avg]&.round(1)}]" unless stats[:min].nil? && stats[:max].nil?
  when :date, :datetime, :time, :timestamp # Date/Time types
    parts << "rng:[min:#{stats[:min]}, max:#{stats[:max]}]" unless stats[:min].nil? && stats[:max].nil?
  end
  parts.join(', ')
end

# Truncate long values for display (remains the same)
def truncate_value(value, max_length = 25)
  str = value.to_s
  str.length > max_length ? "#{str[0...(max_length - 3)]}..." : str
end

# Print the analysis summary to the console (adjusted for Sequel stats structure)
def print_summary_report(report)
  meta = report[:metadata]
  puts colored_output("
--- Database Analysis Summary (Sequel) ---", :magenta, :bold)
  # Use :database_type from metadata
  puts colored_output("DB Adapter: #{meta[:database_adapter]}, Type: #{meta[:database_type]}, Version: #{meta[:database_version]}", :magenta)
  puts colored_output("Generated at: #{meta[:generated_at]}, Duration: #{meta[:analysis_duration_seconds]}s", :magenta)
  sample_info = meta[:sample_size] ? " (Sample Size: ~#{meta[:sample_size]})" : ''
  puts colored_output("Tables Analyzed: #{meta[:analyzed_tables].length}#{sample_info}", :magenta)

  report[:tables].each do |table_name, table_data|
    puts colored_output("
Table: #{table_name}", :cyan, :bold)

    if table_data.is_a?(Hash) && table_data[:error]
      puts colored_output("  Error: #{table_data[:error]}", :red)
      next
    end

    # Get column count and row count from the first column's stats
    column_count = table_data.keys.length
    first_col_stats = table_data.values.first
    row_count = first_col_stats&.dig(:count) || 'N/A' # Count is stored per column now
    puts colored_output("  Rows: #{row_count}, Columns: #{column_count}", :white)

    table_data.each do |column_name, stats|
      next unless stats.is_a?(Hash)

      null_perc = stats[:count].to_i.positive? ? (stats[:null_count].to_f / stats[:count] * 100).round(1) : 0
      unique_marker = stats[:is_unique] ? colored_output(' (unique)', :light_blue) : ''
      # Display both abstract type and db type
      puts colored_output("  - #{column_name} (#{stats[:type]}/#{stats[:db_type]})#{unique_marker}", :yellow)
      puts "    Nulls: #{stats[:null_count]} (#{null_perc}%)"

      summary_stats = format_stats_for_summary(stats)
      puts "    Stats: #{summary_stats}" unless summary_stats.empty?

      # Use :most_frequent and :least_frequent symbols
      if stats[:most_frequent]&.any?
        freq_values = stats[:most_frequent].map { |v, c| "'#{truncate_value(v)}'(#{c})" }.join(', ')
        puts "    Top 5: #{freq_values}"
      end
      if stats[:least_frequent]&.any?
        lfreq_values = stats[:least_frequent].map { |v, c| "'#{truncate_value(v)}'(#{c})" }.join(', ')
        puts "    Bottom 5: #{lfreq_values}"
      end
    end
  end
end

# Write the JSON report to a file or stdout (use make_json_safe)
def write_json_report(report, output_file)
  report_for_json = make_json_safe(report)
  json_report = JSON.pretty_generate(report_for_json)

  if output_file
    begin
      FileUtils.mkdir_p(File.dirname(output_file))
      File.write(output_file, json_report)
      print_info "Report successfully written to #{output_file}"
    rescue StandardError => e
      print_warning "Error writing report to file #{output_file}: #{e.message}"
    end
  else
    puts json_report
  end
end

# --- Command Line Parsing ---

# Define and parse command line options (update pool/timeout descriptions)
def parse_options
  options = {
    environment: DEFAULT_ENVIRONMENT,
    output_file: nil,
    tables: [],
    sample_size: DEFAULT_SAMPLE_SIZE,
    format: DEFAULT_OUTPUT_FORMAT,
    debug: false,
    pool: DEFAULT_POOL_SIZE, # Use default pool size
    database: nil,
    database_url: nil,
    list_databases: false,
    connect_timeout: DEFAULT_CONNECT_TIMEOUT # Use default connect timeout
  }

  parser = OptionParser.new do |opts|
    opts.banner = "Usage: #{$PROGRAM_NAME} [options]"

    opts.on('-d', '--database-url URL', 'Database connection URL (Sequel format, overrides config)') do |url|
      options[:database_url] = url
      # URL overrides environment/database name from file perspective
      options[:environment] = nil # Mark as not file-based environment
      options[:database] = nil
    end
    opts.on('-e', '--environment ENV', "Environment section in config/database.yml (default: #{DEFAULT_ENVIRONMENT})") do |env|
      options[:environment] = env unless options[:database_url]
    end
    opts.on('-b', '--database NAME', 'Specific database name (overrides config/URL)') do |name|
      options[:database] = name # Can override URL database component too
    end
    opts.on('-l', '--list-databases', 'List available databases and exit') { options[:list_databases] = true }
    opts.on('-o', '--output FILE', 'Output report to file instead of stdout') { |f| options[:output_file] = f }
    opts.on('-t', '--tables TBLS', Array, 'Analyze only specific tables (comma-separated)') { |t| options[:tables] = t }
    opts.on('-s', '--sample SIZE', Integer, 'Analyze a sample of ~SIZE rows (PostgreSQL TABLESAMPLE, others may fallback)') do |s|
      options[:sample_size] = s if s.positive?
    end
    opts.on('-f', '--format FMT', OUTPUT_FORMATS, "Output format: #{OUTPUT_FORMATS.join('/')} (default: json)") do |f|
      options[:format] = f
    end
    opts.on('-p', '--pool SIZE', Integer, "Max connections pool size (default: #{DEFAULT_POOL_SIZE})") do |s|
      options[:pool] = s if s.positive?
    end
    opts.on('--timeout SECS', Integer, "Database connection timeout (default: #{DEFAULT_CONNECT_TIMEOUT})") do |t|
      options[:connect_timeout] = t if t.positive?
    end
    opts.on('--debug', 'Show detailed debug information and SQL logging') { options[:debug] = true }
    opts.on('-h', '--help', 'Show this help message') { puts opts; exit }
  end

  parser.parse!
  options[:environment] ||= DEFAULT_ENVIRONMENT # Ensure env has a value if not overridden by URL

  options
rescue OptionParser::InvalidOption, OptionParser::MissingArgument => e
  puts colored_output("Error: #{e.message}", :red)
  puts parser # Show help on error
  exit 1
end

# Print configuration details if debugging is enabled (adapted for Sequel)
def print_debug_info(options)
  return unless options[:debug]

  puts colored_output("
--- Configuration ---", :yellow)
  puts "  Ruby version: #{RUBY_VERSION}"
  puts "  Ruby platform: #{RUBY_PLATFORM}"
  puts "  Sequel version: #{Sequel::VERSION}" # Show Sequel version
  # Check for adapter gems
  %w[pg mysql2 sqlite3].each do |gem_name|
    begin
      require gem_name
      version = case gem_name
                when 'pg' then PG.library_version # Use PG specific method if possible
                when 'mysql2' then Mysql2::VERSION # Use constant
                when 'sqlite3' then SQLite3::VERSION # Use constant
                else 'unknown'
                end
      puts "  #{gem_name} version: #{version}"
    rescue LoadError # rubocop:disable Lint/SuppressedException
    end
  end
  puts "  Current directory: #{Dir.pwd}"
  if defined?(Bundler)
    puts "  Bundler version: #{Bundler::VERSION}"
    puts "  Gemfile path: #{Bundler.default_gemfile}"
    puts "  Bundle path: #{Bundler.bundle_path}"
  else
    puts "  Bundler: Not loaded"
  end
  puts "  Options: #{options.inspect}"
  puts "---------------------"
end

# --- Main Execution Logic ---

# Determine which tables to analyze based on options and available tables using Sequel
def select_tables_to_analyze(options)
  # Fetch all user tables with schemas (schema.table format)
  all_tables_with_schema_query = <<~SQL
    SELECT n.nspname, c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p') -- Regular tables and partitioned tables
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%' -- Exclude system/temp schemas
    ORDER BY n.nspname, c.relname
  SQL
  all_qualified_tables = []
  begin
    all_qualified_tables = $db.fetch(all_tables_with_schema_query).map { |row| "#{row[:nspname]}.#{row[:relname]}" }.sort
    # Ensure double quotes for interpolation
    print_debug("Found qualified tables: #{all_qualified_tables.join(', ')}")
  rescue Sequel::DatabaseError => e
    print_warning("Could not query all schemas for tables: #{e.message}")
  end

  # Fetch tables accessible via the current search_path (unqualified names)
  tables_in_search_path = []
  begin
      tables_in_search_path = $db.tables.map(&:to_s).sort
      # Ensure double quotes for interpolation
      print_debug("Found tables in search_path: #{tables_in_search_path.join(', ')}")
  rescue Sequel::DatabaseError => e
      print_warning("Could not fetch tables using default search path: #{e.message}")
  end

  # Sets for efficient lookup
  qualified_set = Set.new(all_qualified_tables)
  search_path_set = Set.new(tables_in_search_path)
  available_table_references = qualified_set + search_path_set

  if available_table_references.empty?
      abort_with_error 'No tables found in the database (checked schemas and search_path).'
  end

  user_requested_tables = options[:tables]
  tables_to_analyze = []
  invalid_tables = []

  if user_requested_tables.empty?
    # Default: Use tables from search_path, exclude internal ones
    tables_to_analyze = tables_in_search_path - SEQUEL_INTERNAL_TABLES
    if tables_to_analyze.empty?
      # Fallback: Use all non-internal qualified tables if search path yields nothing usable
      all_app_qualified_tables = all_qualified_tables.reject do |t|
        _schema_part, table_part = t.split('.', 2)
        SEQUEL_INTERNAL_TABLES.include?(t) || SEQUEL_INTERNAL_TABLES.include?(table_part)
      end

      if all_app_qualified_tables.any?
         print_warning("No application tables found in the default search path. Using all found qualified tables.")
         tables_to_analyze = all_app_qualified_tables
      else
         print_warning "No application tables found in the default search path or across schemas. Internal tables: #{SEQUEL_INTERNAL_TABLES.join(', ')}."
      end
    end
  else
    # --- Revised User Specified Tables Logic with Public Schema Priority ---
    user_requested_tables.each do |requested_table|
      resolved_table = nil

      if requested_table.include?('.')
        # User provided a qualified name
        resolved_table = requested_table if qualified_set.include?(requested_table)
      else
        # User provided an unqualified name
        # Priority 1: Does it exist directly in the search path?
        if search_path_set.include?(requested_table)
          resolved_table = requested_table # Use the unqualified name as found
          print_debug "Using table '#{requested_table}' found directly in search path."
        else
          # Priority 2: Does it exist in the public schema?
          public_qualified_name = "public.#{requested_table}"
          if qualified_set.include?(public_qualified_name)
            resolved_table = public_qualified_name
            print_debug "Resolved requested table '#{requested_table}' to qualified '#{resolved_table}' (found in public schema)."
          else
            # Priority 3: Can we uniquely resolve it to another qualified name?
            # Select matches from qualified tables NOT starting with 'public.'
            possible_matches = all_qualified_tables.select { |q| !q.start_with?('public.') && q.split('.', 2).last == requested_table }
            if possible_matches.length == 1
              resolved_table = possible_matches.first
              print_debug "Resolved requested table '#{requested_table}' to qualified '#{resolved_table}' (found in non-public schema)."
            elsif possible_matches.length > 1
              # Ambiguous among non-public schemas
              print_warning "Ambiguous requested table '#{requested_table}'. Matches found in non-public schemas: #{possible_matches.join(', ')}. Skipping."
              # Keep resolved_table nil
            end
            # If resolved_table is still nil here, it means no match was found in search path, public, or uniquely elsewhere.
          end
        end
      end

      # Add the resolved table if found and not already added
      if resolved_table
        tables_to_analyze << resolved_table unless tables_to_analyze.include?(resolved_table)
      else
        invalid_tables << requested_table # Mark as invalid if no resolution
      end
    end
    # --- End Revised Logic ---
  end

  # No need to uniq tables_to_analyze now due to the `unless include?` check

  if invalid_tables.any?
    print_warning "Requested tables not found or could not be resolved: #{invalid_tables.uniq.join(', ')}. Available references: #{available_table_references.to_a.sort.join(', ')}"
  end

  if tables_to_analyze.empty?
     msg = user_requested_tables.empty? ? "No application tables found." : "No valid tables specified or found among available references."
     available_refs_msg = available_table_references.any? ? " Available references: #{available_table_references.to_a.sort.join(', ')}" : ""
     abort_with_error "#{msg}#{available_refs_msg}"
  end

  print_info "
Analyzing #{tables_to_analyze.length} table(s): #{tables_to_analyze.join(', ')}"
  tables_to_analyze
end

# Primary execution flow using Sequel
def run_report(options)
  start_time = Time.now
  print_info "Starting database report (Sequel) at #{start_time.strftime('%Y-%m-%d %H:%M:%S')}...", :green, :bold

  $debug = options[:debug] # Set global debug flag
  print_debug_info(options)

  db_config = determine_database_config(options)
  if db_config.nil? || (db_config.is_a?(String) && db_config.empty?) || (db_config.is_a?(Hash) && db_config.empty?)
    config_source = if options[:database_url] then '--database-url'
                    elsif ENV['DATABASE_URL'] then 'DATABASE_URL env var'
                    else "config/database.yml (env: #{options[:environment]})" end
    abort_with_error "No database connection info found from #{config_source}."
  end

  establish_connection(db_config, options[:pool], options[:connect_timeout]) # Use updated function

  if options[:list_databases]
    print_info "
Available databases (from current connection's perspective):", :green, :bold
    databases = list_available_databases # Use updated function
    if databases.empty?
      print_warning 'No databases found or unable to retrieve list.'
    else
      databases.each { |db| puts colored_output("  #{db}", :cyan) }
    end
    $db.disconnect if $db # Disconnect after listing
    return # Exit after listing databases
  end

  display_connection_info # Show details if debugging
  print_database_info # Show basic connection info

  tables_to_analyze = select_tables_to_analyze(options) # Use updated function

  # Prepare report structure
  report = {
    metadata: {
      generated_at: Time.now.iso8601,
      database_adapter: $db.adapter_scheme.to_s,
      database_type: $db.database_type.to_s, # Add Sequel's db type symbol
      database_version: ($db.fetch('SELECT version()').first[:version] rescue 'unknown'), # Fetch version
      analyzed_tables: tables_to_analyze, # Already sorted strings
      sample_size: options[:sample_size],
      analysis_duration_seconds: nil
    },
    tables: {}
  }

  # Analyze each table
  tables_to_analyze.each do |table_name|
    report[:tables][table_name] = analyze_table(table_name, options[:sample_size]) # Use updated function
  end

  # Finalize and output report
  duration = (Time.now - start_time).round(2)
  report[:metadata][:analysis_duration_seconds] = duration
  print_info "
Analysis finished in #{duration} seconds.", :green, :bold

  case options[:format]
  when 'summary' then print_summary_report(report)
  when 'json' then write_json_report(report, options[:output_file])
  end
end

# --- Script Entry Point ---

def main
  options = parse_options
  run_report(options)
rescue Sequel::DatabaseConnectionError => e # Specific Sequel connection error
  abort_with_error "Database connection failed: #{e.message}. Check config and server status."
rescue Sequel::DatabaseError => e # General Sequel database errors (query issues, etc.)
  abort_with_error "Database operation failed: #{e.message}"
rescue StandardError => e # Catch-all for other unexpected errors
  puts colored_output("
An unexpected error occurred:", :red, :bold)
  puts colored_output(e.message, :red)
  if $debug || !defined?($debug) # Show backtrace if debug enabled or error before flag set
    puts colored_output("
Stack Trace:", :red)
    puts e.backtrace.join("
")
  end
  exit 1 # Exit with non-zero status
ensure
  # Ensure connection is closed if established
  $db.disconnect if $db rescue nil
end

# Run main only if script is executed directly
if __FILE__ == $PROGRAM_NAME
  main
end
