# frozen_string_literal: true

require 'sequel'
require_relative '../utils'

module DbReport
  module DbAnalyzer
    # Frequency Analysis module extracts and analyzes frequency distribution of values in database columns
    module FrequencyAnalyzer
      include Utils # Include Utils for logging, constants, identifiers
      extend Utils  # Extend Utils to make its methods available as module methods too

      # Batch analyze frequencies for multiple columns at once
      def batch_analyze_frequencies(col_stats_map, base_dataset, columns_schema, unique_single_columns)
        return if columns_schema.empty?

        # Get total row count from first column's stats
        row_count = col_stats_map.values.first[:count].to_i
        return if row_count <= 0

        # Identify columns needing frequency analysis and group by type
        analyzable_columns = {}

        columns_schema.each do |column_sym, column_info|
          col_stats = col_stats_map[column_sym]
          column_type = column_info[:type]

          if needs_frequency_analysis?(col_stats, column_sym, column_type, unique_single_columns, row_count)
            db_type = column_info[:db_type].to_s.downcase

            # Group by actual database type for union compatibility
            if column_type == :json || column_type == :jsonb
              # Process JSON columns individually
              begin
                analyze_json_frequency(col_stats_map[column_sym], column_sym, base_dataset)
              rescue => e
                puts colored_output("  Error analyzing JSON frequency for #{column_sym}: #{e.message}", :red)
              end
            else
              # Group by database type for UNION compatibility
              type_key = db_type.split('(').first.strip # Extract base type without size/precision
              analyzable_columns[type_key] ||= []
              analyzable_columns[type_key] << {sym: column_sym, type: column_type, col_stats: col_stats}
            end
          end
        end

        # Process each type group separately for UNION compatibility
        analyzable_columns.each do |type_key, columns|
          print_debug("  Processing batch for type: #{type_key} with #{columns.size} columns") if $debug

          # Most frequent values
          batch_analyze_most_frequent_by_type(columns, base_dataset)

          # Least frequent values where needed
          least_freq_columns = columns.select do |col_info|
            distinct_count = col_info[:col_stats][:distinct_count] || 0
            distinct_count > 5
          end

          batch_analyze_least_frequent_by_type(least_freq_columns, base_dataset) if least_freq_columns.any?

          # Handle special case for columns with few distinct values
          few_distinct_columns = columns.select do |col_info|
            col_stats = col_info[:col_stats]
            distinct_count = col_stats[:distinct_count] || 0
            most_freq_empty = col_stats[:most_frequent].nil? || col_stats[:most_frequent].empty?
            distinct_count > 0 && distinct_count <= 5 && most_freq_empty
          end

          batch_analyze_all_values_by_type(few_distinct_columns, base_dataset) if few_distinct_columns.any?
        end

        # Clean up least_frequent that don't need it
        columns_schema.each do |column_sym, _|
          col_stats = col_stats_map[column_sym]
          distinct_count = col_stats[:distinct_count] || 0
          if distinct_count <= 5 || (col_stats[:least_frequent] && col_stats[:least_frequent].empty?)
            col_stats.delete(:least_frequent)
          end
        end
      end

      # Determine if a column needs frequency analysis
      def needs_frequency_analysis?(col_stats, column_sym, column_type, unique_single_columns, row_count)
        return false if row_count <= 0 # Skip if table is empty

        is_groupable = !%i[text blob xml array hstore json jsonb].include?(column_type)
        is_likely_key = col_stats[:is_unique] || unique_single_columns.include?(column_sym) ||
                         column_sym == :id || column_sym.to_s.end_with?('_id')
        is_json_type = column_type == :json || column_type == :jsonb

        # Skip if not groupable (unless JSON) or is a likely key (unless JSON)
        return false unless is_groupable || is_json_type
        return false if is_likely_key && !is_json_type

        true
      end

      # Analyze JSON field frequencies by casting to text
      def analyze_json_frequency(col_stats, column_sym, base_dataset)
        print_info "    Analyzing JSON column frequency..." if $debug
        casted_column = Sequel.cast(column_sym, :text)
        json_dataset = base_dataset.select(casted_column.as(column_sym), Sequel.function(:COUNT, Sequel.lit('*')).as(:count))
                              .group(casted_column)
                              .order(Sequel.desc(:count))
                              .limit(5)

        print_debug("    JSON frequency SQL: #{json_dataset.sql}") if $debug
        most_freq_results = json_dataset.all

        col_stats[:most_frequent] = most_freq_results.to_h { |row| [row[column_sym].to_s, row[:count].to_i] }
        print_debug "    Found #{col_stats[:most_frequent].size} frequent JSON patterns" if $debug
        # Skip least frequent for JSON for simplicity
        col_stats.delete(:least_frequent) # Remove placeholder if it exists
      end

      # Analyze standard column frequencies (non-JSON)
      def analyze_standard_frequency(col_stats, column_sym, base_dataset)
        # Most Frequent
        most_freq_order = [Sequel.desc(:count), column_sym]
        most_freq_results = execute_frequency_query(base_dataset, column_sym, most_freq_order, 5)
        col_stats[:most_frequent] = format_frequency_results(most_freq_results, column_sym)

        # Least Frequent (only if worthwhile)
        distinct_count = col_stats[:distinct_count] || 0
        if distinct_count > 5
          least_freq_order = [Sequel.asc(:count), column_sym]
          least_freq_results = execute_frequency_query(base_dataset, column_sym, least_freq_order, 5)
          col_stats[:least_frequent] = format_frequency_results(least_freq_results, column_sym)
        elsif distinct_count > 0 && distinct_count <= 5 && col_stats[:most_frequent].empty?
          # If few distinct values and most_frequent wasn't useful, get all values
          least_freq_order = [Sequel.asc(:count), column_sym]
          all_freq_results = execute_frequency_query(base_dataset, column_sym, least_freq_order, nil) # No limit
          col_stats[:most_frequent] = format_frequency_results(all_freq_results, column_sym)
          col_stats.delete(:least_frequent) # Remove placeholder
        else
          col_stats.delete(:least_frequent) # Remove placeholder if not calculated
        end
      end

      # Format frequency query results into a hash
      def format_frequency_results(results, column_sym)
        results.to_h do |row|
          key = row[column_sym].nil? ? "NULL" : row[column_sym].to_s
          [key, row[:count].to_i]
        end
      end

      # Helper to safely run frequency queries
      def execute_frequency_query(base_dataset, column_sym, order_expressions, limit_count)
        print_debug("    [Debug Frequency] Column: #{column_sym}, Order: #{order_expressions.inspect}, Limit: #{limit_count.inspect}") if $debug

        freq_dataset = base_dataset.group_and_count(column_sym).order(*order_expressions)
        freq_dataset = freq_dataset.limit(limit_count) if limit_count

        sql_query = freq_dataset.sql
        print_debug("  Executing Frequency: #{sql_query}") if $debug

        start_time = Time.now
        result = []
        begin
          result = freq_dataset.all
        rescue Sequel::DatabaseError => e
          puts colored_output("  SQL Frequency Error: #{e.message.lines.first.strip}", :red)
          print_debug "  Failed SQL: #{sql_query}" if $debug
          return [] # Return empty array on SQL error
        rescue StandardError => e
          puts colored_output("  Error (Frequency Query): #{e.message.lines.first.strip}", :red)
          return []
        ensure
          duration = (Time.now - start_time).round(4)
          print_debug("  Frequency Duration: #{duration}s") if $debug
        end
        result
      end

      # Batch analyze most frequent values for multiple columns of the same type
      def batch_analyze_most_frequent_by_type(columns, base_dataset)
        return if columns.empty?

        column_symbols = columns.map { |c| c[:sym] }
        print_debug("  Batch analyzing most frequent values for columns: #{column_symbols.join(', ')}") if $debug

        # For each column, create a separate query and union them
        union_datasets = []

        columns.each do |col_info|
          column_sym = col_info[:sym]
          # Create a dataset that selects column, count, and a constant 'column_name'
          column_dataset = base_dataset
            .select(column_sym,
                    Sequel.function(:COUNT, Sequel.lit('*')).as(:count),
                    Sequel.lit("'#{column_sym}'").as(:column_name))
            .group(column_sym)
            .order(Sequel.desc(:count), column_sym)
            .limit(5)

          union_datasets << column_dataset
        end

        # Combine all queries with UNION ALL
        combined_dataset = union_datasets.reduce { |combined, dataset| combined.union(dataset, all: true) }

        # Execute the combined query
        start_time = Time.now
        begin
          results = combined_dataset.all

          # Process results
          results_by_column = {}
          results.each do |row|
            column_name = row[:column_name].to_sym
            results_by_column[column_name] ||= []
            results_by_column[column_name] << row
          end

          # Update column stats
          results_by_column.each do |column_sym, column_results|
            columns.each do |col_info|
              if col_info[:sym] == column_sym
                col_info[:col_stats][:most_frequent] = format_frequency_results(column_results, column_sym)
                break
              end
            end
          end

        rescue => e
          puts colored_output("  Error in batch frequency analysis: #{e.message}", :red)
          print_debug("  Failed SQL: #{combined_dataset.sql}") if $debug

          # Fall back to individual queries
          print_debug("  Falling back to individual frequency queries") if $debug
          columns.each do |col_info|
            analyze_standard_frequency(col_info[:col_stats], col_info[:sym], base_dataset)
          end
        ensure
          duration = (Time.now - start_time).round(4)
          print_debug("  Batch frequency analysis duration: #{duration}s") if $debug
        end
      end

      # Batch analyze least frequent values for multiple columns of the same type
      def batch_analyze_least_frequent_by_type(columns, base_dataset)
        return if columns.empty?

        column_symbols = columns.map { |c| c[:sym] }
        print_debug("  Batch analyzing least frequent values for columns: #{column_symbols.join(', ')}") if $debug

        # Similar to batch_analyze_most_frequent but with ascending count order
        union_datasets = []

        columns.each do |col_info|
          column_sym = col_info[:sym]
          column_dataset = base_dataset
            .select(column_sym,
                    Sequel.function(:COUNT, Sequel.lit('*')).as(:count),
                    Sequel.lit("'#{column_sym}'").as(:column_name))
            .group(column_sym)
            .order(Sequel.asc(:count), column_sym)
            .limit(5)

          union_datasets << column_dataset
        end

        combined_dataset = union_datasets.reduce { |combined, dataset| combined.union(dataset, all: true) }

        start_time = Time.now
        begin
          results = combined_dataset.all

          results_by_column = {}
          results.each do |row|
            column_name = row[:column_name].to_sym
            results_by_column[column_name] ||= []
            results_by_column[column_name] << row
          end

          results_by_column.each do |column_sym, column_results|
            columns.each do |col_info|
              if col_info[:sym] == column_sym
                col_info[:col_stats][:least_frequent] = format_frequency_results(column_results, column_sym)
                break
              end
            end
          end

        rescue => e
          puts colored_output("  Error in batch least frequency analysis: #{e.message}", :red)
          # Individual fallback handled in outer method
        ensure
          duration = (Time.now - start_time).round(4)
          print_debug("  Batch least frequency analysis duration: #{duration}s") if $debug
        end
      end

      # Analyze all values for columns with few distinct values
      def batch_analyze_all_values_by_type(columns, base_dataset)
        return if columns.empty?

        column_symbols = columns.map { |c| c[:sym] }
        print_debug("  Analyzing all values for columns with few distinct values: #{column_symbols.join(', ')}") if $debug

        union_datasets = []

        columns.each do |col_info|
          column_sym = col_info[:sym]
          column_dataset = base_dataset
            .select(column_sym,
                    Sequel.function(:COUNT, Sequel.lit('*')).as(:count),
                    Sequel.lit("'#{column_sym}'").as(:column_name))
            .group(column_sym)
            .order(Sequel.asc(:count), column_sym)

          union_datasets << column_dataset
        end

        combined_dataset = union_datasets.reduce { |combined, dataset| combined.union(dataset, all: true) }

        start_time = Time.now
        begin
          results = combined_dataset.all

          results_by_column = {}
          results.each do |row|
            column_name = row[:column_name].to_sym
            results_by_column[column_name] ||= []
            results_by_column[column_name] << row
          end

          results_by_column.each do |column_sym, column_results|
            columns.each do |col_info|
              if col_info[:sym] == column_sym
                col_info[:col_stats][:most_frequent] = format_frequency_results(column_results, column_sym)
                col_info[:col_stats].delete(:least_frequent) # Remove placeholder
                break
              end
            end
          end

        rescue => e
          puts colored_output("  Error in batch all values analysis: #{e.message}", :red)
        ensure
          duration = (Time.now - start_time).round(4)
          print_debug("  Batch all values analysis duration: #{duration}s") if $debug
        end
      end

      # Analyze frequency of column values
      def analyze_frequency(col_stats, column_sym, base_dataset, column_type, unique_single_columns)
        is_groupable = !%i[text blob xml array hstore json jsonb].include?(column_type)
        is_likely_key = col_stats[:is_unique] || unique_single_columns.include?(column_sym) || column_sym == :id || column_sym.to_s.end_with?('_id')
        is_json_type = column_type == :json || column_type == :jsonb

        # Skip if not groupable (unless JSON) or is a likely key (unless JSON)
        unless is_groupable || is_json_type
          print_debug("    Skipping frequency analysis for non-groupable type: #{column_sym} (#{column_type})") if $debug
          return
        end
        if is_likely_key && !is_json_type
          print_debug("    Skipping frequency analysis for PK/FK or unique column: #{column_sym}") if $debug
          return
        end
        return unless col_stats[:count] > 0 # Skip if table is empty

        begin
          if is_json_type
            # Analyze JSON frequency by casting to text
            analyze_json_frequency(col_stats, column_sym, base_dataset)
          else
            # Analyze frequency for standard groupable types
            analyze_standard_frequency(col_stats, column_sym, base_dataset)
          end
        rescue Sequel::DatabaseError => e
          puts colored_output("  SQL Frequency Error for column #{column_sym}: #{e.message.lines.first.strip}", :red)
        rescue StandardError => e
          puts colored_output("  Error during frequency analysis for column #{column_sym}: #{e.message}", :red)
          puts e.backtrace.join("\n") if $debug
        end
      end

      # Make module methods available as instance methods
      module_function :batch_analyze_frequencies, :needs_frequency_analysis?, :analyze_json_frequency,
                      :analyze_standard_frequency, :format_frequency_results, :execute_frequency_query,
                      :batch_analyze_most_frequent_by_type, :batch_analyze_least_frequent_by_type,
                      :batch_analyze_all_values_by_type, :analyze_frequency
    end
  end
end
