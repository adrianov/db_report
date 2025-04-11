# frozen_string_literal: true

require 'sequel'
require_relative '../utils'
require_relative 'schema_helper'
require_relative 'aggregate_functions'
require_relative 'frequency_analyzer'

module DbReport
  module DbAnalyzer
    # Base analyzer class that uses the extracted modules
    class Base
      include Utils
      include SchemaHelper
      include AggregateFunctions
      include FrequencyAnalyzer

      attr_reader :db, :options, :debug

      def initialize(db_connection, options)
        @db = db_connection
        @options = options
        @debug = options[:debug] # Ensure debug flag is accessible
        # Make $debug accessible globally for Utils module print_debug
        # This is a compromise; ideally, debug state would be passed explicitly.
        $debug = @debug
      end

      # --- Table Selection Logic ---

      # Determine which tables to analyze based on options and available tables
      def select_tables_to_analyze
        select_tables_for_analysis(db, options)
      end

      # --- Table Analysis --- #

      # Analyze a single table
      def analyze_table(table_name_string)
        table_identifier = create_sequel_identifier(table_name_string)
        print_info "Analyzing table: #{table_name_string}", :cyan, :bold
        adapter_type = db.database_type
        table_stats = {}

        begin
          columns_schema = fetch_and_enhance_schema(db, table_identifier)
          return { error: "Could not fetch schema for #{table_name_string}" } if columns_schema.empty?

          base_dataset = db[table_identifier]
          unique_single_columns = fetch_unique_single_columns(db, table_name_string)

          # --- Aggregation --- #
          all_select_parts = [Sequel.function(:COUNT, Sequel.lit('*')).as(:_total_count)]
          initial_col_stats = initialize_column_stats(columns_schema, unique_single_columns)
          columns_schema.each do |col_sym, col_info|
            col_select_parts = build_aggregate_select_parts(col_info[:type], col_sym, adapter_type, initial_col_stats[col_sym][:is_unique])
            all_select_parts.concat(col_select_parts)
          end

          all_agg_results = execute_aggregate_query(base_dataset, all_select_parts)
          total_count = all_agg_results ? all_agg_results[:_total_count].to_i : 0

          # --- Populate Stats & Frequency --- #
          columns_schema.each do |column_sym, column_info|
            col_stats = initial_col_stats[column_sym]
            print_info "  - Analyzing column: #{column_sym} (#{col_stats[:type]}/#{col_stats[:db_type]})#{col_stats[:is_unique] ? ' (unique)' : ''}", :white

            col_stats[:count] = total_count # Set total count for this column
            populate_stats_from_aggregates(col_stats, all_agg_results, column_info[:type], column_sym, total_count) if all_agg_results
          end

          # Batch analyze frequencies for all columns in one go
          print_debug "  Batch analyzing column frequencies..." if debug
          batch_analyze_frequencies(initial_col_stats, base_dataset, columns_schema, unique_single_columns)

          # Search for specific value if provided in options
          if options[:search_value]
            print_debug "  Searching for value: #{options[:search_value]}..." if debug
            search_for_value_in_table(table_name_string, columns_schema, options[:search_value], initial_col_stats)
          end

          # Clean up and store final stats
          columns_schema.each do |column_sym, _column_info|
            col_stats = initial_col_stats[column_sym]
            col_stats.delete_if { |_key, value| value.nil? || value == {} } # Clean up empty stats
            table_stats[column_sym.to_s] = col_stats # Store final stats with string key
          end

        rescue Sequel::DatabaseError => e # Catch errors like table not found
          msg = "Schema/Table Error for '#{table_name_string}': #{e.message.lines.first.strip}"
          puts colored_output(msg, :red)
          return { error: msg }
        rescue StandardError => e
          msg = "Unexpected error analyzing table '#{table_name_string}': #{e.message}"
          puts colored_output(msg, :red)
          puts e.backtrace.join("\n") if debug
          return { error: msg }
        end

        table_stats
      end

      # Search for a specific value in a given table and its columns
      # @param table_name [String] Name of the table to search in
      # @param columns_schema [Hash] Schema information about columns
      # @param search_value [String] Value to search for
      # @param col_stats [Hash] Column statistics to update with search results
      def search_for_value_in_table(table_name, columns_schema, search_value, col_stats)
        table_identifier = create_sequel_identifier(table_name)

        columns_schema.each do |column_sym, column_info|
          begin
            # Create a safe query to search for the value
            dataset = db[table_identifier]

            # Different search strategies based on column type
            case column_info[:type]
            when :integer, :bigint
              # Try to convert value to integer if the column type is integer
              begin
                int_value = Integer(search_value)
                result = dataset.where(column_sym => int_value).count > 0
              rescue ArgumentError
                # If search_value can't be converted to integer, no match
                result = false
              end
            when :float, :decimal, :numeric
              # Try to convert value to float if the column type is float/decimal
              begin
                float_value = Float(search_value)
                result = dataset.where(column_sym => float_value).count > 0
              rescue ArgumentError
                result = false
              end
            when :boolean
              # Handle boolean values
              bool_value = case search_value.downcase
                          when 'true', 't', 'yes', 'y', '1'
                            true
                          when 'false', 'f', 'no', 'n', '0'
                            false
                          else
                            nil
                          end
              result = !bool_value.nil? && dataset.where(column_sym => bool_value).count > 0
            when :date, :datetime, :timestamp
              # Skip date/time types for simple string searches
              result = false
            when :json, :jsonb
              # For JSON types, use database-specific containment operators if available
              if db.database_type == :postgres
                # Postgres JSON containment check (works with both text and non-text values)
                result = dataset.where(Sequel.lit("#{db.literal(column_sym)}::text ILIKE ?", "%#{search_value}%")).count > 0
              else
                # Fallback to string representation for other databases
                result = dataset.where(Sequel.like(Sequel.function(:cast, column_sym, :text), "%#{search_value}%")).count > 0
              end
            else
              # Default string-based search for other types
              result = dataset.where(Sequel.like(column_sym.to_sym, "%#{search_value}%")).count > 0
            end

            # Add search result to column stats
            if result
              col_stats[column_sym][:found] = true
              col_stats[column_sym][:search_value] = search_value
              print_info "    Found '#{search_value}' in column: #{column_sym}", :green
            end
          rescue Sequel::DatabaseError => e
            print_debug "    Error searching for value in #{table_name}.#{column_sym}: #{e.message}" if debug
          end
        end
      end

      # Analyze tables in parallel using multiple processes (default method)
      # @param tables_to_analyze [Array<String>] List of table names to analyze
      # @param parallel_processes [Integer] Number of parallel processes to use (defaults to processor count)
      # @return [Hash] Combined results of table analysis
      def analyze_tables_in_parallel(tables_to_analyze = nil, parallel_processes = nil)
        begin
          require 'parallel'
        rescue LoadError
          print_warning "The 'parallel' gem is required for parallel analysis but was not found."
          print_warning "Run 'gem install parallel' to install it, or add it to your Gemfile."
          print_warning "Falling back to sequential analysis."
          return analyze_tables_sequentially(tables_to_analyze)
        end

        tables_to_analyze ||= select_tables_to_analyze
        # Determine number of workers - use specified value or auto-detect, but never more than table count
        default_processes = [Parallel.processor_count, tables_to_analyze.size].min
        parallel_processes ||= options[:parallel_processes] || default_processes

        # Further limit processes to table count if needed
        parallel_processes = [parallel_processes, tables_to_analyze.size].min

        print_info "Using #{parallel_processes} parallel processes to analyze #{tables_to_analyze.size} tables", :cyan, :bold

        # Initialize database config for connection recreation in child processes
        db_opts = @db.opts.dup

        # We need to use processes, not threads, for true parallelism
        # Each process will create its own database connection
        results = Parallel.map(tables_to_analyze, in_processes: parallel_processes) do |table_name|
          # Create a new database connection for each process
          process_db = Sequel.connect(db_opts)
          process_db.extension :connection_validator if db.database_type == :postgres

          # Create a new analyzer instance with the new connection
          analyzer = DbReport::DbAnalyzer::Base.new(process_db, options)

          # Analyze the table and return the result
          table_result = analyzer.analyze_table(table_name)
          process_db.disconnect

          [table_name, table_result]
        end

        # Convert results array to hash
        Hash[results]
      end

      # Analyze tables sequentially (fallback method when parallel gem is unavailable)
      # @param tables_to_analyze [Array<String>] List of table names to analyze
      # @return [Hash] Combined results of table analysis
      def analyze_tables_sequentially(tables_to_analyze = nil)
        tables_to_analyze ||= select_tables_to_analyze
        results = {}

        tables_to_analyze.each do |table_name|
          results[table_name] = analyze_table(table_name)
        end

        results
      end
    end
  end
end
