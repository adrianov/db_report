#!/usr/bin/env ruby

require 'active_record'
require 'yaml'
require 'optparse'
require 'json'
require 'fileutils'
require 'time'
require 'uri'

# Optional dependencies
HAS_COLORIZE = begin
  require 'colorize'
  true
rescue LoadError
  false
end

HAS_PROGRESS_BAR = begin
  require 'progress_bar'
  true
rescue LoadError
  false
end

# This script reads the DATABASE_URL from the environment, config/database.yml, or command line argument
# and analyzes the structure of all fields in the database.

# Method to list all available databases
def list_available_databases
  begin
    connection = ActiveRecord::Base.connection

    # Different SQL for different database adapters
    query = case connection.adapter_name.downcase
            when 'postgresql'
              "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
            when 'mysql', 'mysql2'
              "SHOW DATABASES"
            when 'sqlite'
              puts colored_output("SQLite doesn't support listing databases", :yellow)
              return []
            else
              puts colored_output("Unsupported adapter for listing databases: #{connection.adapter_name}", :yellow)
              return []
            end

    result = connection.select_values(query)
    return result
  rescue => e
    puts colored_output("Error listing databases: #{e.message}", :red)
    return []
  end
end

# Method to load database configuration
def load_database_config(environment = 'development', database_name = nil)
  config = {}

  # Load the database configuration from config/database.yml
  config_path = File.join(Dir.pwd, 'config', 'database.yml')
  if File.exist?(config_path)
    begin
      # Use aliases: true to properly parse YAML with aliases
      yaml_content = File.read(config_path)
      db_config = if Psych::VERSION >= '4.0.0'
        YAML.safe_load(yaml_content, permitted_classes: [], aliases: true)
      else
        YAML.load(yaml_content)
      end

      puts colored_output("Loaded configuration from #{config_path}", :green) if db_config

      # Handle both traditional and Rails 6+ primary/replica format
      env_config = db_config[environment]
      if env_config
        if env_config.key?('primary') # Rails 6+ multiple database format
          puts colored_output("Detected Rails 6+ primary/replica database configuration", :green)
          primary_config = env_config['primary']
          config = primary_config
          puts colored_output("Using primary database configuration", :green)
        else # Traditional single database format
          config = env_config
        end
      else
        puts colored_output("Warning: Environment '#{environment}' not found in #{config_path}", :yellow)
      end
    rescue => e
      puts colored_output("Error parsing #{config_path}: #{e.message}", :red)
      puts colored_output("Trying fallback YAML loading method...", :yellow)
      begin
        # Fallback to simple load
        db_config = YAML.load_file(config_path)

        # Handle both traditional and Rails 6+ primary/replica format
        env_config = db_config[environment]
        if env_config
          if env_config.key?('primary') # Rails 6+ multiple database format
            puts colored_output("Detected Rails 6+ primary/replica database configuration", :green)
            primary_config = env_config['primary']
            config = primary_config
            puts colored_output("Using primary database configuration", :green)
          else # Traditional single database format
            config = env_config
          end
        end
      rescue => e2
        puts colored_output("Fallback also failed: #{e2.message}", :red)
      end
    end
  else
    puts colored_output("Database configuration file not found at #{config_path}", :yellow)
  end

  # Check for DATABASE_URL in environment
  if ENV['DATABASE_URL']
    # If we have both DATABASE_URL and config/database.yml, merge them
    # with DATABASE_URL taking precedence for connection parameters
    if config.is_a?(Hash) && !config.empty?
      puts colored_output("Merging DATABASE_URL with config/database.yml", :green)
      # Convert DATABASE_URL to hash for easier merging
      uri = URI.parse(ENV['DATABASE_URL'])
      db_params = {
        'adapter'  => uri.scheme == 'postgres' ? 'postgresql' : uri.scheme,
        'host'     => uri.host,
        'port'     => uri.port,
        'username' => uri.user,
        'password' => uri.password,
        'url'      => ENV['DATABASE_URL']
      }

      # Only take the database name from config/database.yml if not in DATABASE_URL
      if uri.path && !uri.path.empty? && uri.path != '/'
        db_params['database'] = uri.path.sub(/^\//, '')
      end

      # Merge configs with DATABASE_URL taking precedence
      config = config.merge(db_params)
    else
      # Just use DATABASE_URL
      config = ENV['DATABASE_URL']
    end
  end

  # Override with specific database name if provided
  if database_name && config.is_a?(Hash)
    puts colored_output("Overriding database name to: #{database_name}", :green)
    config['database'] = database_name
  end

  # Ensure we have a database name
  if config.is_a?(Hash) && !config['database'] && ENV['DATABASE_URL']
    # Extract database name from yml if missing in URL
    begin
      yaml_content = File.read(config_path)
      db_config = if Psych::VERSION >= '4.0.0'
        YAML.safe_load(yaml_content, permitted_classes: [], aliases: true)
      else
        YAML.load(yaml_content)
      end

      env_config = db_config[environment]
      if env_config && env_config.key?('primary') && env_config['primary']['database']
        config['database'] = env_config['primary']['database']
        puts colored_output("Using database name '#{config['database']}' from config/database.yml", :green)
      end
    rescue => e
      puts colored_output("Could not extract database name from config: #{e.message}", :yellow)
    end
  end

  return config
end

# Method to establish a database connection
def establish_connection(db_config)
  puts colored_output("Attempting to connect to database...", :cyan)

  # Convert string URL to hash config if needed
  connection_config = if db_config.is_a?(String)
    # It's a DATABASE_URL
    masked_url = db_config.gsub(/:[^:]*@/, ":*****@")
    puts colored_output("Using connection string: #{masked_url}", :cyan)

    # Parse the URL to extract connection details for better error messages
    begin
      uri = URI.parse(db_config)
      user = uri.user || 'unknown'
      host = uri.host || 'unknown'
      database = uri.path.sub(/^\//, '') if uri.path && !uri.path.empty?
      puts colored_output("Connection details: host=#{host}, user=#{user}, database=#{database || 'unknown'}", :cyan) if $debug
    rescue => e
      puts colored_output("Warning: Could not parse DATABASE_URL for details: #{e.message}", :yellow)
    end

    { 'url' => db_config }
  else
    # It's a config hash
    if db_config['url']
      puts colored_output("Using URL from config: #{db_config['url'].gsub(/:[^:]*@/, ":*****@")}", :cyan)
    else
      adapter = db_config['adapter'] || 'unknown'
      database = db_config['database'] || 'unknown'
      host = db_config['host'] || 'localhost'
      user = db_config['username'] || 'unknown'
      puts colored_output("Using config: adapter=#{adapter}, database=#{database}, host=#{host}, user=#{user}", :cyan)
    end
    db_config
  end

  # Process any ERB templates in config values
  connection_config = process_erb_in_config(connection_config)

  # Ensure we have proper pool settings - important to prevent "could not obtain a connection from the pool" errors
  if connection_config.is_a?(Hash)
    # Default to a smaller pool size for this script to avoid resource exhaustion
    connection_config['pool'] ||= 5
    connection_config['checkout_timeout'] ||= 10
    puts colored_output("Connection pool size: #{connection_config['pool']}", :cyan)
  end

  # Test that required adapter is available
  if connection_config.is_a?(Hash) && connection_config['adapter']
    adapter_gem = case connection_config['adapter'].to_s
                  when /mysql/
                    'mysql2'
                  when /postgres/
                    'pg'
                  when /sqlite/
                    'sqlite3'
                  else
                    connection_config['adapter']
                  end

    begin
      require adapter_gem
      puts colored_output("Loaded #{adapter_gem} adapter", :green)
    rescue LoadError => e
      abort colored_output("ERROR: Couldn't load database adapter '#{adapter_gem}'. Please make sure it's included in your Gemfile and run 'bundle install'.\nError: #{e.message}", :red, :bold)
    end
  end

  # Connect to the database
  begin
    # Set a custom log level for ActiveRecord to avoid excessive logging
    original_logger = ActiveRecord::Base.logger
    ActiveRecord::Base.logger = nil unless $debug

    # Close any existing connections before establishing new ones
    begin
      ActiveRecord::Base.clear_active_connections! if ActiveRecord::Base.respond_to?(:clear_active_connections!)
    rescue => e
      puts colored_output("Warning: Could not clear active connections: #{e.message}", :yellow) if $debug
    end

    # Establish the connection
    ActiveRecord::Base.establish_connection(connection_config)

    # Verify the connection works
    ActiveRecord::Base.connection.execute("SELECT 1")
    puts colored_output("Successfully connected to database", :green)

    # Restore the original logger
    ActiveRecord::Base.logger = original_logger
  rescue => e
    error_message = "Failed to connect to database: #{e.message}"

    # Extract username from config for better error messages
    username = if connection_config.is_a?(Hash)
      connection_config['username'] || 'unknown'
    elsif connection_config.is_a?(Hash) && connection_config['url']
      begin
        URI.parse(connection_config['url']).user || 'unknown'
      rescue
        'unknown'
      end
    else
      'unknown'
    end

    # Add helpful tips based on error type
    tips = case e.message
           when /could not connect to server/
             "Make sure the database server is running and accessible from this host."
           when /database .* does not exist/
             "The specified database doesn't exist. Create it or check your database name."
           when /password authentication failed/, /role .* does not exist/, /authentication failed/
             "There is an issue connecting to your database with your username/password, username: #{username}.\n\nPlease check your database configuration to ensure the username/password are valid."
           when /role .* does not exist/
             "The specified database user doesn't exist. Create it or check your username."
           when /No such file or directory/
             "The database file doesn't exist (for SQLite)."
           when /could not obtain a connection from the pool/
             "Connection pool exhausted. Try setting a larger pool size with --pool option or by setting pool: value in database.yml."
           else
             "Check your database configuration and ensure the database server is running."
           end

    config_tips = case
                  when connection_config.is_a?(Hash) && connection_config['url']
                    "Your DATABASE_URL format should be: adapter://user:password@host:port/database"
                  when connection_config.is_a?(Hash)
                    required_keys = ['adapter']
                    required_keys << 'database' unless connection_config['adapter'] == 'sqlite3' && connection_config['database'] == ':memory:'
                    required_keys << 'host' unless connection_config['adapter'] == 'sqlite3'
                    missing = required_keys.select { |k| connection_config[k].nil? }
                    if missing.any?
                      "Your config is missing required keys: #{missing.join(', ')}"
                    else
                      nil
                    end
                  else
                    nil
                  end

    error_details = [error_message, tips, config_tips].compact.join("\n  • ")
    abort colored_output("ERROR: #{error_details}", :red, :bold)
  end
end

# Process ERB templates in database configuration
def process_erb_in_config(config)
  return config unless config.is_a?(Hash)

  processed_config = {}
  config.each do |key, value|
    if value.is_a?(String) && value.include?('<%')
      begin
        # Try to process ERB template
        require 'erb'
        processed_value = ERB.new(value).result
        processed_config[key] = processed_value
      rescue => e
        puts colored_output("Warning: Could not process ERB in config value for #{key}: #{e.message}", :yellow)
        # Fall back to default for pool if needed
        if key == 'pool'
          processed_config[key] = 5
        else
          processed_config[key] = value
        end
      end
    else
      processed_config[key] = value
    end
  end
  processed_config
end

# Method to safely quote a column or table name to prevent SQL injection
def quote_identifier(identifier)
  ActiveRecord::Base.connection.quote_column_name(identifier)
end

# Helper method for colored console output
def colored_output(text, color = :default, mode = nil)
  return text unless HAS_COLORIZE
  text = text.colorize(color)
  text = text.send(mode) if mode
  text
end

# Method to measure and update statistics for a value based on its column type
def update_value_stats(stats, column_name, value, column_type)
  # Convert value to appropriate type if needed
  case column_type
  when :integer
    value = value.to_i if value.is_a?(String)
  when :float, :decimal
    value = value.to_f if value.is_a?(String)
  end

  # Measure based on column type
  case column_type
  when :string, :text, :json, :jsonb, :xml, :binary, :citext
    # Character or byte length based measurements
    length = value.to_s.length
    stats[column_name][:total_length] += length
    stats[column_name][:min] = length if stats[column_name][:min].nil? || length < stats[column_name][:min]
    stats[column_name][:max] = length if stats[column_name][:max].nil? || length > stats[column_name][:max]
  when :integer, :float, :decimal, :bigint, :smallint
    # Numeric value measurements
    stats[column_name][:min] = value if stats[column_name][:min].nil? || value < stats[column_name][:min]
    stats[column_name][:max] = value if stats[column_name][:max].nil? || value > stats[column_name][:max]
  when :date, :datetime, :timestamp, :time
    # Date/time measurements - convert to Time for comparison if not already
    time_value = value.is_a?(Time) ? value : (Time.parse(value.to_s) rescue nil)
    if time_value
      stats[column_name][:min] = time_value if stats[column_name][:min].nil? || time_value < stats[column_name][:min]
      stats[column_name][:max] = time_value if stats[column_name][:max].nil? || time_value > stats[column_name][:max]
    end
  when :boolean
    # Calculate true/false percentages
    bool_value = value.to_s.downcase
    if bool_value == 'true' || bool_value == 't' || bool_value == '1'
      stats[column_name][:true_count] ||= 0
      stats[column_name][:true_count] += 1
    elsif bool_value == 'false' || bool_value == 'f' || bool_value == '0'
      stats[column_name][:false_count] ||= 0
      stats[column_name][:false_count] += 1
    end
  when :array, :hstore
    # Array measurements
    if value.is_a?(Array)
      length = value.length
      stats[column_name][:min_items] = length if stats[column_name][:min_items].nil? || length < stats[column_name][:min_items]
      stats[column_name][:max_items] = length if stats[column_name][:max_items].nil? || length > stats[column_name][:max_items]
      stats[column_name][:total_items] ||= 0
      stats[column_name][:total_items] += length
    end
  end

  # Update count
  stats[column_name][:count] += 1

  # Return the string representation for frequency count
  value.to_s
end

# Calculate final statistics based on the column type
def finalize_column_stats(stats, column_name, column_type)
  # Calculate averages based on column type
  if stats[column_name][:count] > 0
    case column_type
    when :string, :text, :json, :jsonb, :xml, :binary, :citext
      # Average length
      stats[column_name][:avg] = stats[column_name][:total_length] / stats[column_name][:count].to_f
    when :array, :hstore
      # Average items
      if stats[column_name][:total_items]
        stats[column_name][:avg_items] = stats[column_name][:total_items] / stats[column_name][:count].to_f
      end
    when :boolean
      # True percentage
      true_count = stats[column_name][:true_count] || 0
      stats[column_name][:true_percentage] = (true_count.to_f / stats[column_name][:count]) * 100
    end
  end

  # Remove temporary counters used for calculations
  [:total_length, :total_items, :true_count, :false_count].each do |key|
    stats[column_name].delete(key) if stats[column_name][key]
  end
end

# Get record count for a table
def get_table_count(table_name)
  quoted_table = quote_identifier(table_name)
  result = ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM #{quoted_table}")
  result.to_i
rescue => e
  puts colored_output("Unable to get count for table #{table_name}: #{e.message}", :yellow)
  0
end

# Method to analyze a table and gather statistics
def analyze_table(table_name, sample_size = nil)
  puts colored_output("Analyzing table: #{table_name}", :cyan, :bold)

  begin
    # Fetching the table's columns
    columns = ActiveRecord::Base.connection.columns(table_name)
    stats = {}

    # Get total row count for progress bar
    total_count = get_table_count(table_name)

    # Don't use progress bar for empty tables or when sampling
    use_progress = total_count > 0 && HAS_PROGRESS_BAR && !sample_size && ENV['DISABLE_PROGRESS_BAR'] != '1'

    columns.each do |column|
      column_name = column.name
      column_type = column.type
      quoted_column = quote_identifier(column_name)
      quoted_table = quote_identifier(table_name)

      puts "  - Column: #{column_name} (#{column_type})" if !use_progress

      # Initialize statistics based on column type
      stats[column_name] = {
        type: column_type.to_s,
        min: nil,
        max: nil,
        avg: nil,
        count: 0,
        null_count: 0,
        frequent_values: Hash.new(0),
        total_length: 0  # Initialize total_length to 0
      }

      # Add type-specific initial stats
      case column_type
      when :array, :hstore
        stats[column_name][:min_items] = nil
        stats[column_name][:max_items] = nil
        stats[column_name][:avg_items] = nil
      when :boolean
        stats[column_name][:true_percentage] = 0
      end

      # Query to fetch data from the column, with optional sampling
      query = if sample_size && ActiveRecord::Base.connection.adapter_name.downcase.include?('postgres')
        "SELECT #{quoted_column} FROM #{quoted_table} TABLESAMPLE BERNOULLI(#{sample_size})"
      elsif sample_size
        "SELECT #{quoted_column} FROM #{quoted_table} ORDER BY RANDOM() LIMIT #{sample_size}"
      else
        "SELECT #{quoted_column} FROM #{quoted_table}"
      end

      # Use connection.exec_query instead of select_all for better memory management
      result = nil
      begin
        result = ActiveRecord::Base.connection.exec_query(query)
      rescue => e
        if e.message.include?('column reference') && e.message.include?('TABLESAMPLE')
          # Fall back to regular sampling for older PostgreSQL versions
          puts colored_output("  TABLESAMPLE not supported, falling back to LIMIT", :yellow)
          query = "SELECT #{quoted_column} FROM #{quoted_table} ORDER BY RANDOM() LIMIT #{sample_size}"
          result = ActiveRecord::Base.connection.exec_query(query)
        else
          raise e
        end
      end

      # Setup progress bar
      bar = nil
      if use_progress
        bar_title = "Analyzing #{column_name}"
        begin
          bar = ProgressBar.new(result.rows.count, :bar, bar_title)
        rescue => e
          puts colored_output("Warning: Could not initialize progress bar: #{e.message}", :yellow)
          use_progress = false
        end
      end

      result.rows.each_with_index do |row, idx|
        value = row[0]  # exec_query returns an array of arrays

        # Count nulls
        if value.nil?
          stats[column_name][:null_count] += 1
        else
          # Update stats and get value as string for frequency counting
          value_key = update_value_stats(stats, column_name, value, column_type)
          stats[column_name][:frequent_values][value_key] += 1
        end

        # Update progress bar
        bar.increment! if use_progress && bar
      end

      # Clear the result to free memory
      result = nil

      # Finalize statistics calculations
      finalize_column_stats(stats, column_name, column_type)

      # Sort frequent values and extract most/least frequent
      sorted_values = stats[column_name][:frequent_values].sort_by { |_, count| -count }
      stats[column_name][:most_frequent] = sorted_values.first(5).to_h
      stats[column_name][:least_frequent] = sorted_values.last(5).to_h

      # Remove the full frequent_values hash to reduce memory usage
      stats[column_name].delete(:frequent_values)

      # Periodically clear active connections to prevent pool exhaustion
      begin
        ActiveRecord::Base.clear_active_connections! if ActiveRecord::Base.respond_to?(:clear_active_connections!)
      rescue => e
        puts colored_output("Warning: Could not clear active connections: #{e.message}", :yellow) if $debug
      end
    end
  rescue => e
    puts colored_output("Error analyzing table #{table_name}: #{e.message}", :red)
    return { error: e.message }
  end

  stats
end

# Display database connection details
def display_connection_info
  begin
    conn = ActiveRecord::Base.connection
    adapter = conn.adapter_name

    if conn.respond_to?(:current_database)
      database = conn.current_database
    elsif conn.respond_to?(:instance_variable_get) && conn.instance_variable_get(:@config)
      database = conn.instance_variable_get(:@config)[:database] || 'unknown'
    else
      database = 'unknown'
    end

    # Get server version
    server_version = case adapter.downcase
                     when 'postgresql'
                       conn.select_value("SELECT version()") rescue 'unknown'
                     when 'mysql', 'mysql2'
                       conn.select_value("SELECT version()") rescue 'unknown'
                     when 'sqlite'
                       conn.select_value("SELECT sqlite_version()") rescue 'unknown'
                     else
                       'unknown'
                     end

    # Get connection details
    connection_details = if conn.respond_to?(:instance_variable_get) && conn.instance_variable_get(:@config)
                          config = conn.instance_variable_get(:@config)
                          host = config[:host] || 'localhost'
                          port = config[:port] || 'default'
                          "#{host}:#{port}"
                        else
                          'unknown'
                        end

    puts colored_output("\nDatabase Connection Details:", :green, :bold)
    puts colored_output("  Adapter:       #{adapter}", :cyan)
    puts colored_output("  Database:      #{database}", :cyan)
    puts colored_output("  Version:       #{server_version}", :cyan)
    puts colored_output("  Connection:    #{connection_details}", :cyan)
    puts

  rescue => e
    puts colored_output("Error getting connection information: #{e.message}", :red)
  end
end

# Print database info
def print_database_info
  adapter_name = "unknown"
  database_name = "unknown"

  begin
    # Get connection and adapter name
    conn = ActiveRecord::Base.connection
    adapter_name = conn.adapter_name rescue "unknown"

    # Get database name using the appropriate method
    database_name = if conn.respond_to?(:current_database)
      conn.current_database
    elsif conn.respond_to?(:instance_variable_get) && conn.instance_variable_get(:@config)
      config = conn.instance_variable_get(:@config)
      config[:database] || config['database'] || "unknown"
    else
      "unknown"
    end

    puts colored_output("Connected to #{adapter_name} database: #{database_name}", :green, :bold)
  rescue => e
    puts colored_output("Connected to database (could not determine full details: #{e.message})", :yellow)
  end
end

# Main execution
def main
  options = {
    environment: 'development',
    output_file: nil,
    tables: [],
    sample_size: nil,
    format: 'json',
    debug: false,
    pool: 5,  # Default connection pool size
    database: nil,
    list_databases: false
  }

  OptionParser.new do |opts|
    opts.banner = "Usage: db_report.rb [options]"

    opts.on("-d", "--database-url URL", "Database URL") do |url|
      options[:database_url] = url
    end

    opts.on("-e", "--environment ENV", "Rails environment (default: development)") do |env|
      options[:environment] = env
    end

    opts.on("-b", "--database NAME", "Specific database name to connect to") do |name|
      options[:database] = name
    end

    opts.on("-l", "--list-databases", "List all available databases and exit") do
      options[:list_databases] = true
    end

    opts.on("-o", "--output FILE", "Output to file instead of stdout") do |file|
      options[:output_file] = file
    end

    opts.on("-t", "--tables TABLE1,TABLE2", Array, "Analyze only specific tables (comma-separated)") do |tables|
      options[:tables] = tables
    end

    opts.on("-s", "--sample SIZE", Integer, "Sample size (rows or percentage for PostgreSQL)") do |size|
      options[:sample_size] = size
    end

    opts.on("-f", "--format FORMAT", "Output format: json or summary (default: json)") do |format|
      options[:format] = format if ['json', 'summary'].include?(format)
    end

    opts.on("-p", "--pool SIZE", Integer, "Connection pool size (default: 5)") do |size|
      options[:pool] = size.to_i
    end

    opts.on("--debug", "Show detailed debug information") do
      options[:debug] = true
    end

    opts.on("-h", "--help", "Show this help message") do
      puts opts
      exit
    end
  end.parse!

  # Make debug flag globally accessible
  $debug = options[:debug]

  begin
    puts colored_output("Starting database report...", :green, :bold)

    if options[:debug]
      puts colored_output("\nEnvironment:", :yellow)
      puts "  Ruby version: #{RUBY_VERSION}"
      puts "  Ruby platform: #{RUBY_PLATFORM}"
      puts "  ActiveRecord version: #{ActiveRecord::VERSION::STRING}"
      puts "  Current directory: #{Dir.pwd}"
      if defined?(Bundler)
        puts "  Bundler version: #{Bundler::VERSION}"
      else
        puts "  Bundler: Not loaded"
      end
    end

    # Determine the database configuration
    # Priority: command-line option, database.yml + DATABASE_URL
    db_config = options[:database_url] || load_database_config(options[:environment], options[:database])

    if db_config.nil?
      abort colored_output("ERROR: No database connection information found. Please provide a DATABASE_URL environment variable, --database-url option, or ensure config/database.yml exists with a #{options[:environment]} environment.", :red, :bold)
    end

    # Establish connection to the database
    establish_connection(db_config)

    # List databases if requested
    if options[:list_databases]
      puts colored_output("\nAvailable databases:", :green, :bold)
      databases = list_available_databases
      if databases.empty?
        puts colored_output("  No databases found or unable to retrieve list", :yellow)
      else
        databases.each do |db|
          puts colored_output("  #{db}", :cyan)
        end
      end
      exit
    end

    # Display additional connection info if requested
    display_connection_info if options[:debug]

    # Print database info - show which database we're connected to
    print_database_info

    # Get tables to analyze
    all_tables = ActiveRecord::Base.connection.tables
    if all_tables.empty?
      abort colored_output("ERROR: No tables found in the database. Please ensure the database has tables.", :red, :bold)
    end

    tables_to_analyze = options[:tables].empty? ? all_tables : options[:tables]

    # Validate table names
    invalid_tables = tables_to_analyze - all_tables
    if invalid_tables.any?
      puts colored_output("Warning: These tables don't exist: #{invalid_tables.join(', ')}", :yellow)
      tables_to_analyze = tables_to_analyze - invalid_tables
    end

    if tables_to_analyze.empty?
      abort colored_output("ERROR: No valid tables to analyze.", :red, :bold)
    end

    report = {
      metadata: {
        generated_at: Time.now.iso8601,
        database_adapter: ActiveRecord::Base.connection.adapter_name,
        tables_analyzed: tables_to_analyze.length,
        sample_size: options[:sample_size]
      }
    }

    # Analyze each table
    tables_to_analyze.each do |table_name|
      report[table_name] = analyze_table(table_name, options[:sample_size])
    end

    # Generate output
    case options[:format]
    when 'summary'
      # Print a summary to the console
      puts colored_output("\nDatabase Analysis Summary:", :green, :bold)
      report.each do |table_name, table_data|
        next if table_name == :metadata

        puts colored_output("\nTable: #{table_name}", :cyan, :bold)

        if table_data.is_a?(Hash) && table_data[:error]
          puts colored_output("  Error: #{table_data[:error]}", :red)
          next
        end

        puts colored_output("  Columns: #{table_data.keys.length}", :white)

        table_data.each do |column_name, stats|
          next unless stats.is_a?(Hash)

          puts colored_output("  - #{column_name} (#{stats[:type]})", :yellow)
          puts "    Count: #{stats[:count]}, Null count: #{stats[:null_count]}"

          # Handle type-specific stats
          column_type = begin
                          stats[:type].to_sym
                        rescue
                          nil
                        end

          case column_type
          when :string, :text, :json, :jsonb, :binary
            puts "    Length: min=#{stats[:min]}, max=#{stats[:max]}, avg=#{stats[:avg].round(2) if stats[:avg]}"
          when :integer, :decimal, :float, :bigint
            puts "    Range: min=#{stats[:min]}, max=#{stats[:max]}"
          when :boolean
            puts "    True: #{stats[:true_percentage].round(2) if stats[:true_percentage]}%"
          when :array
            puts "    Items: min=#{stats[:min_items]}, max=#{stats[:max_items]}, avg=#{stats[:avg_items].round(2) if stats[:avg_items]}"
          when :date, :datetime, :timestamp
            puts "    Range: min=#{stats[:min]}, max=#{stats[:max]}"
          end

          if stats[:most_frequent] && !stats[:most_frequent].empty?
            freq_values = stats[:most_frequent].map { |v, c| "#{v}(#{c})" }.join(", ")
            puts "    Top values: #{freq_values}"
          end
        end
      end
    else # json format
      json_report = JSON.pretty_generate(report)

      if options[:output_file]
        # Create directory if it doesn't exist
        FileUtils.mkdir_p(File.dirname(options[:output_file]))
        File.write(options[:output_file], json_report)
        puts colored_output("Report written to #{options[:output_file]}", :green)
      else
        puts json_report
      end
    end

  rescue => e
    puts colored_output("Stack trace:", :red) if options[:debug]
    puts e.backtrace.join("\n") if options[:debug]
    abort colored_output("Error: #{e.message}", :red, :bold)
  end
end

# Run the main method
if __FILE__ == $0
  main
end
