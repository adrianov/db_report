# frozen_string_literal: true

require_relative '../utils'

module DbReport
  module Reporter
    # Formats the report data into a detailed summary for console output.
    module SummaryFormatter
      extend DbReport::Utils

      def self.format(report_data)
        meta = report_data[:metadata]
        meta = report_data[:metadata]
        puts colored_output("
--- Database Analysis Summary (Sequel) ---", :magenta, :bold)
        puts colored_output("Adapter: #{meta[:database_adapter]}, Type: #{meta[:database_type]}, Version: #{meta[:database_version]}", :magenta)
        puts colored_output("Generated: #{meta[:generated_at]}, Duration: #{meta[:analysis_duration_seconds]}s", :magenta)
        puts colored_output("Tables Analyzed: #{meta[:analyzed_tables].length}", :magenta)
        # Display search value if it was used
        if meta[:search_value]
          puts colored_output("Search Value: #{meta[:search_value]}", :magenta, :bold)
        end

        report_data[:tables].each do |table_name, table_data|
          # Determine title based on relation type
          title = case table_data[:relation_type]
                  when :table then "Table"
                  when :view then "View"
                  when :materialized_view then "Materialized View"
                  else "Relation" # Default for unknown types
                  end
          puts colored_output("\n#{title}: #{table_name}", :cyan, :bold)

          if table_data.is_a?(Hash) && table_data[:error]
          if table_data.is_a?(Hash) && table_data.key?(:error) && !table_data[:error].to_s.empty?
            puts colored_output("#{title}: #{table_name} - Error: #{table_data[:error].to_s}", :red) # Ensure error is string
            next
          end

          # Check if we are in schema-only mode (no aggregate data like :count)
          is_schema_only = !table_data.values.any? { |v| v.is_a?(Hash) && v.key?(:count) }

          if is_schema_only
            # --- Schema-Only Output Logic ---
            puts colored_output("  (Schema Only)", :light_black)
            # Print View Metadata if present
            rel_type = table_data[:relation_type]
            if [:view, :materialized_view].include?(rel_type)
              puts "    Type:          #{rel_type.to_s.capitalize}"
              puts "    Definition:    #{DbReport::Utils.truncate_value(table_data[:view_definition], 100)}" if table_data[:view_definition]
              puts "    Dependencies:  #{table_data[:dependencies].join(', ')}" if table_data[:dependencies]&.any?
              if table_data[:is_materialized] && table_data[:last_refresh]
                refresh_time = table_data[:last_refresh].is_a?(Time) ? table_data[:last_refresh].iso8601 : table_data[:last_refresh]
                puts "    Last Refresh:  #{refresh_time}"
              end
            end
            # Print Column List
            puts "    Columns:"
            column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }
            column_data.each do |col_sym, col_info|
              type_str = if col_info[:type] && col_info[:type] != col_info[:db_type]
                           "#{col_info[:type]} / #{col_info[:db_type]}"
                         else
                           col_info[:db_type]
                         end
              puts "      - #{col_sym} (#{type_str})"
            end
            next # Skip the rest of the loop for this relation
          end

          # --- Existing Full Analysis Output Logic Continues Below ---
          # --- Existing Full Analysis Output Logic Continues Below ---
          column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }
          # Safely access count from the first *actual* column's stats hash
          first_col_stats = column_data.values.find { |v| v.is_a?(Hash) && v.key?(:count) } || {} # Find first actual column stats
          row_count = first_col_stats[:count] || 'N/A' # Access :count symbol
          puts colored_output("  Rows: #{row_count}, Columns: #{column_count}", :white)

          # Iterate only over actual column data
          column_data.each do |column_name, stats|

            unique_marker = stats[:is_unique] ? colored_output(' (unique)', :light_blue) : ''
            found_marker = stats[:found] ? colored_output(' [FOUND]', :green, :bold) : ''
            puts colored_output("  - #{column_name}#{unique_marker}#{found_marker}", :yellow)

            type_part = stats[:type].to_s
            db_type_part = stats[:db_type].to_s
            type_str = if type_part.empty? || type_part == db_type_part
                         db_type_part
                       else
                         "#{type_part} / #{db_type_part}"
                       end
            puts "    Type:          #{type_str}"

            null_count = stats[:null_count].to_i
            total_count = stats[:count].to_i
            if null_count > 0
               null_perc = total_count.positive? ? (null_count.to_f / total_count * 100).round(1) : 0
               puts "    Nulls:         #{null_count} (#{null_perc}%)"
            end

            formatted = DbReport::Utils.format_stats_for_summary(stats) # Use Utils method

            # Print individual stats
            puts "    Min:           #{truncate_value(formatted[:min])}" if formatted.key?(:min)
            puts "    Max:           #{truncate_value(formatted[:max])}" if formatted.key?(:max)
            puts "    Average:       #{formatted[:average]}" if formatted.key?(:average)
            puts "    Avg Length:    #{formatted[:avg_length]}" if formatted.key?(:avg_length)
            puts "    Avg Items:     #{formatted[:avg_items]}" if formatted.key?(:avg_items)
            puts "    True %:        #{formatted[:true_percentage]}%" if formatted.key?(:true_percentage)

            # Print search value match if found
            if stats[:found] && stats[:search_value]
              puts colored_output("    Found Match:    '#{stats[:search_value]}'", :green, :bold)
            end

            # Print most frequent value if present
            if formatted[:most_frequent]&.any?
              top_val, top_count = formatted[:most_frequent].first
              puts "    Most Frequent: #{truncate_value(top_val)} (#{top_count} times)"
            end
          end # Closes column_data.each
        end # Closes report_data[:tables].each

        # Add search summary at the end of the report
        search_summary_data = DbReport::Utils.generate_search_summary(report_data)
        if search_summary_data
          if search_summary_data[:total_found] > 0
            puts colored_output("\nSearch Summary", :green, :bold)
            puts colored_output("Value '#{search_summary_data[:search_value]}' found in #{search_summary_data[:total_found]} column(s):", :green)
            search_summary_data[:found_locations].each do |location|
              puts colored_output("  - #{location}", :green)
            end
          else
            puts colored_output("\nValue '#{search_summary_data[:search_value]}' not found in any column", :yellow)
          end
        end
      end
    end
end

