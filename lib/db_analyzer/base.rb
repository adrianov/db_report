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
    end
  end
end
