#!/usr/bin/env ruby
# frozen_string_literal: true

# --- Setup Load Path ---
# Ensure the lib directory next to the script is in the load path
SCRIPT_DIR = __dir__
LIB_DIR = File.expand_path('lib', SCRIPT_DIR)
$LOAD_PATH.unshift(LIB_DIR) unless $LOAD_PATH.include?(LIB_DIR)

# --- Core Requirements ---
require 'sequel'
require 'optparse'
require 'time'
require 'set'
require 'fileutils'

# --- Local Application Code ---
# Use relative paths from the script's directory
# Ensure lib directory is in the load path or use full relative paths
begin
  # Use standard require, relying on the modified $LOAD_PATH
  require 'utils'
  require 'config'
  require 'connector'
  require 'analyzer'
  require 'reporter'
rescue LoadError => e
  # If require fails, abort
  abort "Error: Could not load necessary library files from '#{LIB_DIR}'.\nMake sure the necessary files exist directly within it.\nOriginal error: #{e.message}"
end

# --- Global State ---
# Use a global variable for debug state, accessible by Utils' print_debug
# TODO: Consider passing debug state explicitly instead of using a global.
$debug = false

# --- Application Class ---

class DbReportApp
  # Mixin Utilities for logging and constants
  # Include Utils for instance methods (print_info, etc.)
  # Extend self with Utils for class-level access to constants needed during option parsing
  include DbReport::Utils
  extend DbReport::Utils

  attr_reader :options, :connector, :analyzer, :reporter

  def initialize
    @options = parse_options
    # Set global debug state AFTER options are parsed, as :debug is set there
    $debug = @options[:debug]
    print_debug_info(@options) # Now call debug info printing
  end

  # --- Command Line Parsing --- #
  def parse_options
    # Access constants via the Utils module directly (since it's extended)
    options = {
      environment: DbReport::Utils::DEFAULT_ENVIRONMENT,
      output_file: nil,
      tables: [],
      format: DbReport::Utils::DEFAULT_OUTPUT_FORMAT,
      debug: false,
      pool: DbReport::Utils::DEFAULT_POOL_SIZE,
      database: nil,
      database_url: nil,
      list_databases: false,
      connect_timeout: DbReport::Utils::DEFAULT_CONNECT_TIMEOUT
    }

    parser = OptionParser.new do |opts|
      opts.banner = "Usage: #{$PROGRAM_NAME} [options]"

      opts.on('-d', '--database-url URL', 'Database connection URL (Sequel format, overrides config)') do |url|
        options[:database_url] = url
        options[:environment] = nil # Mark as not file-based environment
        options[:database] = nil # URL takes precedence over specific DB name from file
      end
      opts.on('-e', '--environment ENV', "Environment section in config/database.yml (default: #{DbReport::Utils::DEFAULT_ENVIRONMENT})") do |env|
        options[:environment] = env unless options[:database_url] # Only set if URL not provided
      end
      opts.on('-b', '--database NAME', 'Specific database name (overrides config/URL database component)') do |name|
        options[:database] = name
      end
      opts.on('-l', '--list-databases', 'List available databases and exit') { options[:list_databases] = true }
      opts.on('-o', '--output FILE', 'Output report to file instead of stdout') { |f| options[:output_file] = f }
      opts.on('-t', '--tables TBLS', Array, 'Analyze only specific tables (comma-separated)') { |t| options[:tables] = t }
      opts.on('-f', '--format FMT', DbReport::Utils::OUTPUT_FORMATS, "Output format: #{DbReport::Utils::OUTPUT_FORMATS.join('/')} (default: #{DbReport::Utils::DEFAULT_OUTPUT_FORMAT})") do |f|
        options[:format] = f
      end
      opts.on('-p', '--pool SIZE', Integer, "Max connections pool size (default: #{DbReport::Utils::DEFAULT_POOL_SIZE})") do |s|
        options[:pool] = s if s.positive?
      end
      opts.on('--timeout SECS', Integer, "Database connection timeout (default: #{DbReport::Utils::DEFAULT_CONNECT_TIMEOUT})") do |t|
        options[:connect_timeout] = t if t.positive?
      end
      opts.on('--debug', 'Show detailed debug information and SQL logging') { options[:debug] = true }
      opts.on('-h', '--help', 'Show this help message') { puts opts; exit }
    end

    parser.parse!
    # Ensure environment has a value if not overridden by URL
    options[:environment] ||= DbReport::Utils::DEFAULT_ENVIRONMENT

    options
  rescue OptionParser::InvalidOption, OptionParser::MissingArgument, OptionParser::AmbiguousOption => e
    # Use instance method colored_output available via include
    puts colored_output("Error: #{e.message}", :red)
    puts parser # Show help on error
    exit 1
  end

  # Print configuration details if debugging is enabled
  def print_debug_info(options)
    return unless options[:debug]

    # Use instance methods from included Utils module
    puts colored_output("
--- Configuration ---", :yellow)
    puts "  Ruby version: #{RUBY_VERSION}"
    puts "  Ruby platform: #{RUBY_PLATFORM}"
    puts "  Sequel version: #{Sequel::VERSION}"
    # Check for adapter gems dynamically
    %w[pg mysql2 sqlite3].each do |gem_name|
      begin
        require gem_name
        # Get version safely
        version = case gem_name
                  when 'pg' then (PG.respond_to?(:library_version) ? PG.library_version : 'N/A') rescue 'error'
                  when 'mysql2' then Mysql2::VERSION rescue 'error'
                  when 'sqlite3' then SQLite3::VERSION rescue 'error'
                  else 'unknown'
                  end
        puts "  #{gem_name} version: #{version}"
      rescue LoadError # rubocop:disable Lint/SuppressedException
        # Gem not found, do nothing
      end
    end
    puts "  Current directory: #{Dir.pwd}"
    if defined?(Bundler)
      puts "  Bundler version: #{Bundler::VERSION rescue 'N/A'}"
      puts "  Gemfile path: #{Bundler.default_gemfile rescue 'N/A'}"
      puts "  Bundle path: #{Bundler.bundle_path rescue 'N/A'}"
    else
      puts "  Bundler: Not loaded"
    end
    puts "  Options: #{options.inspect}"
    puts "---------------------"
  end

  # --- Main Execution Logic --- #
  def run
    start_time = Time.now
    print_info "Starting database report (Sequel) at #{start_time.strftime('%Y-%m-%d %H:%M:%S')}...", :green, :bold

    # 1. Determine Configuration
    config_handler = DbReport::Config.new(options)
    db_config = config_handler.determine

    if db_config.nil? || (db_config.is_a?(String) && db_config.empty?) || (db_config.is_a?(Hash) && db_config.empty?)
      config_source = if options[:database_url] then '--database-url'
                      elsif ENV['DATABASE_URL'] then 'DATABASE_URL env var'
                      # Use constant via Utils module
                      else "#{DbReport::Utils::CONFIG_FILE_PATH} (env: #{options[:environment]})" end
      abort_with_error "No database connection info found from #{config_source}."
    end

    # 2. Establish Connection
    @connector = DbReport::Connector.new(db_config, options[:pool], options[:connect_timeout], options[:debug])

    # Determine if we need to list databases *before* attempting the final connection
    should_list_databases = options[:list_databases] || !DbReport::Utils.database_name_present?(db_config)

    # Connect: Pass list_mode=true if we need to list databases
    db_connection = connector.connect(list_mode: should_list_databases)

    # If connection failed (handled within connect method), exit.
    exit 1 unless db_connection

    # If we established a connection *only* for listing databases, list and exit.
    if should_list_databases
      handle_list_databases
      # Explicitly disconnect the base connection used for listing
      connector.disconnect
      return # Exit after listing
    end

    # If we reached here, we have a connection to the target database.
    # Proceed with analysis.
    # Display connection info if debugging
    connector.display_connection_info
    connector.print_database_info # Show basic connection info regardless of debug

    # 3. Analyze Tables
    @analyzer = DbReport::Analyzer.new(db_connection, options)
    tables_to_analyze = analyzer.select_tables_to_analyze

    # Prepare the main report structure
    report_data = generate_report_structure(db_connection, tables_to_analyze)

    # Analyze each table and store results
    tables_to_analyze.each do |table_name|
      report_data[:tables][table_name] = analyzer.analyze_table(table_name)
    end

    # 4. Generate and Output Report
    finalize_and_output_report(report_data, start_time)

  rescue Sequel::DatabaseConnectionError => e
    # Specific connection errors handled by Connector, but catch here as fallback
    abort_with_error "Database connection failed: #{e.message}. Check config and server status."
  rescue Sequel::DatabaseError => e
    # General database errors during analysis/querying
    abort_with_error "Database operation failed: #{e.message}"
  rescue StandardError => e
    # Catch-all for other unexpected errors (like file loading, option parsing issues not caught earlier)
    handle_unexpected_error(e)
  ensure
    # Always attempt to disconnect if the connector was initialized
    connector&.disconnect
  end

  private

  # Helper method to list databases
  def handle_list_databases
    print_info "
Available databases (from current connection's perspective):", :green, :bold
    databases = connector.list_available_databases
    if databases.empty?
      print_warning 'No databases found or unable to retrieve list.'
    else
      databases.each { |db| puts colored_output("  #{db}", :cyan) }
    end
  end

  # Helper method to create the basic report structure
  def generate_report_structure(db_connection, tables_to_analyze)
    {
      metadata: {
        generated_at: Time.now.iso8601,
        database_adapter: db_connection.adapter_scheme.to_s,
        database_type: db_connection.database_type.to_s,
        # Safely fetch version
        database_version: begin
                            db_connection.fetch('SELECT version()').first[:version]
                          rescue StandardError
                            'unknown'
                          end,
        analyzed_tables: tables_to_analyze, # List of tables included in the report
        analysis_duration_seconds: nil # Placeholder
      },
      tables: {} # Placeholder for table-specific analysis results
    }
  end

  # Helper method to finalize and output the report
  def finalize_and_output_report(report_data, start_time)
    duration = (Time.now - start_time).round(2)
    report_data[:metadata][:analysis_duration_seconds] = duration
    print_info "
Analysis finished in #{duration} seconds.", :green, :bold

    # Create reporter and output based on format
    @reporter = DbReport::Reporter.new(report_data)
    case options[:format]
    when 'summary', 'gpt'
      output_target = options[:output_file]
      if output_target
        begin
          # Ensure the directory exists before writing
          FileUtils.mkdir_p(File.dirname(output_target))
          File.open(output_target, 'w') do |file|
            original_stdout = $stdout
            $stdout = file # Temporarily redirect stdout
            options[:format] == 'summary' ? reporter.print_summary : reporter.print_gpt_summary
          ensure
            $stdout = original_stdout # Ensure stdout is restored
          end
          # Use existing helper, assumes print_info goes to original stdout/stderr managed elsewhere or is acceptable here
          print_info "Report successfully written to #{output_target}"
        rescue StandardError => e
          # Use existing helper for warnings
          print_warning "Error writing report to file #{output_target}: #{e.message}"
        end
      else
        # Original behavior: print directly to current stdout
        options[:format] == 'summary' ? reporter.print_summary : reporter.print_gpt_summary
      end
    when 'json' then reporter.write_json(options[:output_file])
    # Add other formats here if needed
    end
  end

  # Helper method to handle unexpected errors gracefully
  def handle_unexpected_error(error)
    puts colored_output("
An unexpected error occurred:", :red, :bold)
    puts colored_output(error.message, :red)
    # Show backtrace only if debug mode is enabled
    if $debug
      puts colored_output("
Stack Trace:", :red)
      puts error.backtrace.join("
")
    end
    exit 1 # Exit with a non-zero status code
  end

end

# --- Script Entry Point --- #

# This ensures the code runs only when the script is executed directly
if __FILE__ == $PROGRAM_NAME
  app = DbReportApp.new
  app.run
end
