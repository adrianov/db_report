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
        @debug = options[:debug]
        # Make $debug accessible globally for Utils module print_debug
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

          # Build all aggregate queries in one pass
          columns_schema.each do |col_sym, col_info|
            is_unique = initial_col_stats[col_sym][:is_unique]
            all_select_parts.concat(build_aggregate_select_parts(col_info[:type], col_sym, adapter_type, is_unique))
          end

          # Execute a single aggregate query for all columns
          all_agg_results = execute_aggregate_query(base_dataset, all_select_parts)
          total_count = all_agg_results ? all_agg_results[:_total_count].to_i : 0

          # --- Populate Stats & Frequency --- #
          columns_schema.each do |column_sym, column_info|
            col_stats = initial_col_stats[column_sym]
            print_info "  - Analyzing column: #{column_sym} (#{col_stats[:type]}/#{col_stats[:db_type]})#{col_stats[:is_unique] ? ' (unique)' : ''}", :white

            col_stats[:count] = total_count
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
          columns_schema.each do |column_sym, _|
            col_stats = initial_col_stats[column_sym]
            col_stats.delete_if { |_key, value| value.nil? || value == {} }
            table_stats[column_sym.to_s] = col_stats
          end

        rescue Sequel::DatabaseError => e
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
      # Using optimized search with UNIONs for compatible column types
      def search_for_value_in_table(table_name, columns_schema, search_value, col_stats)
        table_identifier = create_sequel_identifier(table_name)
        is_postgres = db.database_type == :postgres

        # Separate columns that need very specialized handling from those that can use UNION
        union_compatible_columns = []
        special_columns = []

        # Try parsing numeric values upfront for later use
        numeric_search_value = nil
        float_search_value = nil
        begin
          numeric_search_value = Integer(search_value)
          float_search_value = Float(search_value)
        rescue ArgumentError
          # Not a valid number, leave as nil
        end

        # Check if it's a boolean
        bool_search_value = case search_value.downcase
                          when 'true', 't', 'yes', 'y', '1' then true
                          when 'false', 'f', 'no', 'n', '0' then false
                          end

        # Categorize columns by search compatibility
        columns_schema.each do |column_sym, column_info|
          case column_info[:type]
          when :date, :datetime, :timestamp
            # These types need specialized handling
            special_columns << { column_sym: column_sym, column_info: column_info }
          when :boolean
            # Only include booleans if the search value is a boolean
            if bool_search_value.nil?
              special_columns << { column_sym: column_sym, column_info: column_info }
            else
              union_compatible_columns << { column_sym: column_sym, column_info: column_info }
            end
          else
            # All other types can use a combined query
            union_compatible_columns << { column_sym: column_sym, column_info: column_info }
          end
        end

        # Process all union-compatible columns in a single batch
        search_all_compatible_columns(table_identifier, union_compatible_columns, search_value, col_stats,
                                      numeric_search_value, float_search_value, bool_search_value) if union_compatible_columns.any?

        # Process special columns individually
        special_columns.each do |col_data|
          search_individual_column(table_identifier, col_data[:column_sym], col_data[:column_info], search_value, col_stats)
        end
      end

      # Search all compatible columns with a single UNION query
      def search_all_compatible_columns(table_identifier, columns, search_value, col_stats,
                                        numeric_value = nil, float_value = nil, bool_value = nil)
        return if columns.empty?

        begin
          query_parts = []
          is_postgres = db.database_type == :postgres

          columns.each do |col_data|
            column_sym = col_data[:column_sym]
            column_info = col_data[:column_info]

            # Build appropriate query part based on column type
            query_part = case column_info[:type]
            when :integer, :bigint
              if numeric_value.nil?
                # If search value is not a number, include a cast-based search for flexibility
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              else
                # If search value is a number, include both exact and cast-based search
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)} = #{numeric_value} OR #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              end
            when :float, :decimal, :numeric
              if float_value.nil?
                # If search value is not a number, include a cast-based search for flexibility
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              else
                # If search value is a number, include both exact and cast-based search
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)} = #{float_value} OR #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              end
            when :boolean
              if bool_value.nil?
                # If search value is not a boolean, only do a text search
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              else
                # If search value is a boolean, do an exact match
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)} = #{bool_value}"
              end
            when :array
              if is_postgres
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE EXISTS (SELECT 1 FROM unnest(#{db.literal(column_sym)}) AS elem WHERE elem::text ILIKE #{db.literal("%#{search_value}%")})"
              else
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              end
            when :string, :text, :char, :varchar, :inet, :uuid, :json, :jsonb, :enum, nil
              build_text_search_query(table_identifier, column_sym, column_info, search_value)
            else
              # For any other types, use a text cast in Postgres or LIKE for other DBs
              if is_postgres
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              else
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)} LIKE #{db.literal("%#{search_value}%")}"
              end
            end

            print_debug "    Search for '#{column_sym}' (#{column_info[:type]}): #{query_part}" if debug
            query_parts << query_part
          end

          # Execute the combined query
          unless query_parts.empty?
            combined_query = query_parts.join(" UNION ALL ")
            print_debug "    Executing combined query for all compatible columns (#{query_parts.size} columns)" if debug
            print_debug "    Query: #{combined_query}" if debug
            results = db.fetch(combined_query).all

            # Process results
            results.each do |row|
              if row[:found]
                column_name = row[:col_name].to_sym
                col_stats[column_name][:found] = true
                col_stats[column_name][:search_value] = search_value
                column_type = columns.find { |c| c[:column_sym] == column_name }&.dig(:column_info, :type)
                type_display = column_type && column_type != :string && column_type != :text ? " #{column_type}" : ""
                print_info "    Found '#{search_value}' in#{type_display} column: #{column_name}", :green
              end
            end
          end
        rescue Sequel::DatabaseError => e
          print_debug "    Error in combined column search: #{e.message}" if debug
          # Fall back to type-specific searches
          search_by_column_types(table_identifier, columns, search_value, col_stats)
        end
      end

      # Fallback method that searches by column types when combined query fails
      def search_by_column_types(table_identifier, columns, search_value, col_stats)
        print_debug "    Falling back to type-specific searches" if debug

        # Group columns by type
        column_groups = {}
        columns.each do |col_data|
          type = col_data[:column_info][:type] || :text
          column_groups[type] ||= []
          column_groups[type] << col_data
        end

        # Search each type group
        column_groups.each do |type, cols|
          search_columns_by_type(table_identifier, cols, type, search_value, col_stats)
        end
      end

      # Generic method to search columns of a specific type using UNION queries
      # Used as a fallback when the combined query fails
      def search_columns_by_type(table_identifier, columns, type, search_value, col_stats)
        return if columns.empty?

        begin
          query_parts = []
          is_postgres = db.database_type == :postgres

          columns.each do |col_data|
            column_sym = col_data[:column_sym]
            column_info = col_data[:column_info]

            # Build query part based on column type
            query_part = case type
            when :text, :string, :char, :varchar, :inet, nil
              build_text_search_query(table_identifier, column_sym, column_info, search_value)
            when :uuid
              "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
            when :array
              if is_postgres
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE EXISTS (SELECT 1 FROM unnest(#{db.literal(column_sym)}) AS elem WHERE elem::text ILIKE #{db.literal("%#{search_value}%")})"
              else
                "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
              end
            when :json, :jsonb, :enum
              "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
            end

            print_debug "    #{type.to_s.upcase} search for '#{column_sym}': #{query_part}" if debug
            query_parts << query_part
          end

          # Execute the combined query
          unless query_parts.empty?
            combined_query = query_parts.join(" UNION ALL ")
            print_debug "    Executing combined query for #{type} columns: #{combined_query}" if debug
            results = db.fetch(combined_query).all

            # Process results
            process_search_results(results, col_stats, search_value, type)
          end
        rescue Sequel::DatabaseError => e
          print_debug "    Error in batch #{type} search: #{e.message}" if debug
          # Fall back to individual searches
          columns.each do |col_data|
            search_individual_column(table_identifier, col_data[:column_sym], col_data[:column_info], search_value, col_stats)
          end
        end
      end

      # Build specialized text column search query
      def build_text_search_query(table_identifier, column_sym, column_info, search_value)
        db_type = column_info[:db_type].to_s.downcase
        column_name = column_sym.to_s.downcase
        needs_cast = false

        # Determine if we need to cast for PostgreSQL
        if db.database_type == :postgres
          needs_cast = column_name == 'event' ||
                      column_name == 'audit_tg_op' ||
                      (db_type != 'character varying' &&
                       db_type != 'varchar' &&
                       db_type != 'text' &&
                       db_type != 'char' &&
                       db_type != 'character' &&
                       !db_type.start_with?('varchar(') &&
                       !db_type.start_with?('character varying(') &&
                       !db_type.start_with?('char(') &&
                       !db_type.start_with?('character('))
        end

        if needs_cast
          "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)}::text ILIKE #{db.literal("%#{search_value}%")}"
        else
          "SELECT '#{column_sym}' AS col_name, COUNT(*) > 0 AS found FROM #{db.literal(table_identifier)} WHERE #{db.literal(column_sym)} ILIKE #{db.literal("%#{search_value}%")}"
        end
      end

      # Process search results and update column stats
      def process_search_results(results, col_stats, search_value, type)
        results.each do |row|
          if row[:found]
            column_name = row[:col_name].to_sym
            col_stats[column_name][:found] = true
            col_stats[column_name][:search_value] = search_value

            type_display = type == :text ? "" : " #{type}"
            print_info "    Found '#{search_value}' in#{type_display} column: #{column_name}", :green
          end
        end
      end

      # Search a single column individually
      def search_individual_column(table_identifier, column_sym, column_info, search_value, col_stats)
        begin
          # Create a safe query to search for the value
          dataset = db[table_identifier]
          result = false
          query_used = nil

          # Different search strategies based on column type
          case column_info[:type]
          when :integer, :bigint
            begin
              int_value = Integer(search_value)
              query_used = "#{db.literal(column_sym)} = #{int_value}"
              print_debug "    NUMERIC search in '#{column_sym}': WHERE #{query_used}" if debug
              result = dataset.where(column_sym => int_value).count > 0
            rescue ArgumentError
              result = false
            end
          when :float, :decimal, :numeric
            begin
              float_value = Float(search_value)
              query_used = "#{db.literal(column_sym)} = #{float_value}"
              print_debug "    NUMERIC search in '#{column_sym}': WHERE #{query_used}" if debug
              result = dataset.where(column_sym => float_value).count > 0
            rescue ArgumentError
              result = false
            end
          when :boolean
            bool_value = case search_value.downcase
                        when 'true', 't', 'yes', 'y', '1' then true
                        when 'false', 'f', 'no', 'n', '0' then false
                        end
            if !bool_value.nil?
              query_used = "#{db.literal(column_sym)} = #{bool_value}"
              print_debug "    BOOLEAN search in '#{column_sym}': WHERE #{query_used}" if debug
              result = dataset.where(column_sym => bool_value).count > 0
            end
          when :date, :datetime, :timestamp
            # Skip date/time types for simple string searches
            print_debug "    Skipping DATE/TIME search in '#{column_sym}'" if debug
            result = false
          else
            # Handle special cases for string searching
            is_postgres = db.database_type == :postgres
            db_type = column_info[:db_type].to_s.downcase
            column_name = column_sym.to_s.downcase

            # Handle special PostgreSQL types
            if is_postgres && (column_name == 'event' || column_name == 'audit_tg_op' ||
               !['character varying', 'varchar', 'text', 'char', 'character'].include?(db_type))
              begin
                query_used = "#{db.literal(column_sym)}::text ILIKE '%#{search_value}%'"
                print_debug "    TEXT search with cast in '#{column_sym}': WHERE #{query_used}" if debug
                result = dataset.where(Sequel.lit("#{db.literal(column_sym)}::text ILIKE ?", "%#{search_value}%")).count > 0
              rescue Sequel::DatabaseError => e
                print_debug "    Error searching custom type column with cast: #{e.message}" if debug
                query_used = "#{db.literal(column_sym)} LIKE '%#{search_value}%'"
                print_debug "    Fallback TEXT search in '#{column_sym}': WHERE #{query_used}" if debug
                result = dataset.where(Sequel.like(column_sym, "%#{search_value}%")).count > 0
              end
            else
              # Default string-based search for other types
              query_used = "#{db.literal(column_sym)} LIKE '%#{search_value}%'"
              print_debug "    TEXT search in '#{column_sym}': WHERE #{query_used}" if debug
              result = dataset.where(Sequel.like(column_sym, "%#{search_value}%")).count > 0
            end
          end

          # Add search result to column stats
          if result
            col_stats[column_sym][:found] = true
            col_stats[column_sym][:search_value] = search_value
            print_info "    Found '#{search_value}' in column: #{column_sym}", :green
          end
        rescue Sequel::DatabaseError => e
          print_debug "    Error searching for value in #{table_identifier.to_s}.#{column_sym}: #{e.message}" if debug
        end
      end

      # Analyze tables in parallel using multiple processes (default method)
      def analyze_tables_in_parallel(tables_to_analyze = nil, parallel_processes = nil)
        begin
          require 'parallel'
        rescue LoadError
          print_warning "The 'parallel' gem is required for parallel analysis but was not found."
          print_warning "Falling back to sequential analysis."
          return analyze_tables_sequentially(tables_to_analyze)
        end

        tables_to_analyze ||= select_tables_to_analyze
        # Determine optimal number of workers
        default_processes = [Parallel.processor_count, tables_to_analyze.size].min
        parallel_processes ||= options[:parallel_processes] || default_processes
        parallel_processes = [parallel_processes, tables_to_analyze.size].min

        print_info "Using #{parallel_processes} parallel processes to analyze #{tables_to_analyze.size} tables", :cyan, :bold

        # Initialize database config for connection recreation in child processes
        db_opts = @db.opts.dup

        # Run analysis in parallel using processes
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

      # Analyze tables sequentially
      def analyze_tables_sequentially(tables_to_analyze = nil)
        tables_to_analyze ||= select_tables_to_analyze
        tables_to_analyze.each_with_object({}) do |table_name, results|
          results[table_name] = analyze_table(table_name)
        end
      end
    end
  end
end
