# frozen_string_literal: true

require 'sequel'
require_relative '../utils'

module DbReport
  module DbAnalyzer
    # AggregateFunction module contains functions for building and executing aggregate queries
    module AggregateFunctions
      include Utils # Include Utils for logging, constants, identifiers
      extend Utils  # Extend Utils to make its methods available as module methods too

      # Build SELECT clause parts for aggregate query based on column type
      def build_aggregate_select_parts(column_type, column_sym, adapter_type, is_unique)
        non_null_alias = :"non_null_count_#{column_sym}"
        min_alias = :"min_val_#{column_sym}"
        max_alias = :"max_val_#{column_sym}"
        avg_alias = :"avg_val_#{column_sym}"
        true_count_alias = :"true_count_#{column_sym}"
        distinct_count_alias = :"distinct_count_#{column_sym}"

        parts = [Sequel.function(:COUNT, column_sym).as(non_null_alias)]
        # Simple heuristic for PK/FK identification
        is_likely_key = is_unique || column_sym == :id || column_sym.to_s.end_with?('_id')
        # Add :tsvector to the list of types that don't support standard MIN/MAX
        groupable = !%i[text blob xml array hstore tsvector].include?(column_type)

        # MIN/MAX (handle type specifics)
        min_max_added = false
        case column_type
        when :json, :jsonb
          cast_expr = Sequel.cast(column_sym, :text)
          parts += [Sequel.function(:MIN, cast_expr).as(min_alias), Sequel.function(:MAX, cast_expr).as(max_alias)]
          min_max_added = true
        when :uuid
          if adapter_type == :postgres
            cast_expr = Sequel.cast(column_sym, :text)
            parts += [Sequel.function(:MIN, cast_expr).as(min_alias), Sequel.function(:MAX, cast_expr).as(max_alias)]
            min_max_added = true
          else
            print_warning "MIN/MAX on UUID might not be supported for #{adapter_type}" if $debug
          end
        when :boolean
          # Cast to int for MIN/MAX compatibility across DBs
          int_cast_expr = Sequel.cast(column_sym, :integer)
          parts += [Sequel.function(:MIN, int_cast_expr).as(min_alias), Sequel.function(:MAX, int_cast_expr).as(max_alias)]
          min_max_added = true
        when :array
          if adapter_type == :postgres
            parts += [Sequel.function(:MIN, Sequel.function(:array_length, column_sym, 1)).as(min_alias),
                      Sequel.function(:MAX, Sequel.function(:array_length, column_sym, 1)).as(max_alias)]
            min_max_added = true
          end
        end
        # Default MIN/MAX for other types if not handled above
        unless min_max_added || !groupable # Don't add MIN/MAX for non-groupable types like text/blob unless handled above
          parts += [Sequel.function(:MIN, column_sym).as(min_alias), Sequel.function(:MAX, column_sym).as(max_alias)]
        end

        # AVG, Distinct Count, True Count
        case column_type
        when :integer, :float, :decimal # Numeric
          unless is_likely_key
            cast_type = adapter_type == :mysql ? :double : :"double precision"
            parts << Sequel.function(:AVG, Sequel.cast(column_sym, cast_type)).as(avg_alias)
            parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, column_sym)).as(distinct_count_alias)
          end
        when :string, :text, :blob, :enum, :inet, :uuid, :json, :jsonb # String-like/complex
          # Average Length (cast non-strings to text)
          unless is_unique || [:text, :blob].include?(column_type) # Skip avg length for text/blob/unique
            length_expr = case column_type
                          when :json, :jsonb, :uuid, :enum, :inet then Sequel.function(:length, Sequel.cast(column_sym, :text))
                          else Sequel.function(:length, column_sym)
                          end
            parts << Sequel.function(:AVG, length_expr).as(avg_alias)
          end
          # Distinct Count (if groupable and not likely key)
          if groupable && !is_likely_key
             distinct_expr = [:json, :jsonb].include?(column_type) ? Sequel.cast(column_sym, :text) : column_sym
             parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, distinct_expr)).as(distinct_count_alias)
          end

        when :boolean
          parts << Sequel.function(:SUM, Sequel.case({ column_sym => 1 }, 0)).as(true_count_alias)
          unless is_likely_key
            parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, column_sym)).as(distinct_count_alias)
          end
        when :array # PG Array
          if !is_unique && adapter_type == :postgres
            parts << Sequel.function(:AVG, Sequel.function(:array_length, column_sym, 1)).as(avg_alias)
          end
          # Distinct count for arrays is tricky, skip for now
        when :date, :datetime, :time, :timestamp
          unless is_likely_key
            parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, column_sym)).as(distinct_count_alias)
          end
        end

        parts
      end

      # Helper to safely run aggregate queries
      def execute_aggregate_query(dataset, select_expressions)
        sql_query = dataset.select(*select_expressions).sql
        print_debug("  Executing Aggregates: #{sql_query}") if $debug
        start_time = Time.now
        result = nil
        begin
          result = dataset.select(*select_expressions).first
        rescue Sequel::DatabaseError => e
          puts colored_output("  SQL Aggregate Error: #{e.message.lines.first.strip}", :red)
          print_debug "  Failed SQL: #{sql_query}" if $debug
          return nil # Return nil on SQL error
        rescue StandardError => e
          puts colored_output("  Error (Aggregate Query): #{e.message.lines.first.strip}", :red)
          return nil
        ensure
          duration = (Time.now - start_time).round(4)
          print_debug("  Aggregates Duration: #{duration}s") if $debug
        end
        result
      end

      # Populate column stats from aggregate query results
      def populate_stats_from_aggregates(col_stats, agg_results, column_type, column_sym, total_count)
        return unless agg_results # Guard against nil results

        non_null_key = :"non_null_count_#{column_sym}"
        min_key = :"min_val_#{column_sym}"
        max_key = :"max_val_#{column_sym}"
        avg_key = :"avg_val_#{column_sym}"
        true_count_key = :"true_count_#{column_sym}"
        distinct_count_key = :"distinct_count_#{column_sym}"

        non_null_count = agg_results[non_null_key].to_i
        col_stats[:null_count] = total_count - non_null_count
        col_stats[:min] = agg_results[min_key]
        col_stats[:max] = agg_results[max_key]
        col_stats[:distinct_count] = agg_results[distinct_count_key].to_i if agg_results.key?(distinct_count_key)

        if column_type == :boolean && non_null_count > 0
          true_count = agg_results[true_count_key].to_i
          col_stats[:true_percentage] = (true_count.to_f / non_null_count) * 100
        elsif agg_results[avg_key]
          col_stats[:avg] = agg_results[avg_key].to_f rescue nil
        end

        # Format date/time (Sequel often returns objects)
        if %i[date datetime time timestamp].include?(column_type)
          col_stats[:min] = col_stats[:min].iso8601 rescue col_stats[:min].to_s if col_stats[:min].respond_to?(:iso8601)
          col_stats[:max] = col_stats[:max].iso8601 rescue col_stats[:max].to_s if col_stats[:max].respond_to?(:iso8601)
        end
      end

      # Make module methods available as instance methods
      module_function :build_aggregate_select_parts, :execute_aggregate_query, :populate_stats_from_aggregates
    end
  end
end
