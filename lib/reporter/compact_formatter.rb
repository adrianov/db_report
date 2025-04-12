# frozen_string_literal: true

require 'terminal-table'
require_relative '../utils'

module DbReport
  module Reporter
    # Formats the report data into a compact table suitable for console output.
    module CompactFormatter
      extend DbReport::Utils

      def self.format(report_data)
        meta = report_data[:metadata]

        # Print title manually before the table
        puts colored_output("Database Analysis Summary", :magenta, :bold)

        # Print Metadata Table - no borders and left-aligned
        metadata_table = Terminal::Table.new do |t|
          t.headings = ['Parameter', 'Value']
          rows = [
            ['Adapter', meta[:database_adapter]],
            ['Type', meta[:database_type]],
            ['Version', meta[:database_version]],
            ['Generated', meta[:generated_at]],
            ['Duration', "#{meta[:analysis_duration_seconds]}s"],
            ['Tables Analyzed', meta[:analyzed_tables].length]
          ]

          # Add search value to metadata table if present
          rows << ['Search Value', meta[:search_value]] if meta[:search_value]

          t.rows = rows
          t.style = {
            border_x: "",
            border_y: " ",
            border_i: " ",
            border_top: false,
            border_bottom: false,
            border_left: false,
            border_right: false
          }
        end

        # Modify the rendering to remove heading separator line and extra empty lines
        rendered_table = metadata_table.to_s.split("\n")
        cleaned_lines = []

        # Process each line
        rendered_table.each_with_index do |line, i|
          # Skip separator line (usually the second line in the rendered table)
          next if i == 1 && (line.include?('─') || line.include?('-') || line.strip.empty?)
          # Skip empty or space-only lines
          next if line.strip.empty?
          # Remove leading space that forms the left border
          line = line.sub(/^ /, '')
          cleaned_lines << line
        end

        puts cleaned_lines.join("\n")
        puts # Add space after metadata table

        # Print Table Summaries
        report_data[:tables].each do |table_name, table_data|
          puts # Add space before each table

          if table_data.is_a?(Hash) && table_data.key?(:error) && !table_data[:error].to_s.empty?
            puts colored_output("Table: #{table_name} - Error: #{table_data[:error].to_s}", :red) # Ensure error is string
            next
          end
          # Schema-only check is now done in lib/reporter.rb
          # Determine title and row_count/indicator based on full data
          # This block will only be entered if it's NOT schema-only mode.
          # is_schema_only = !table_data.values.any? { |v| v.is_a?(Hash) && v.key?(:count) }
          has_count_data = true # Assume full data here

          # Determine title and row_count/indicator
          title = case table_data[:relation_type]
                  when :materialized_view then "Materialized View"
                  else "Relation"
                  end
          row_count_str = has_count_data ? (table_data.values.find { |v| v.is_a?(Hash) && v.key?(:count) }&.dig(:count) || 'N/A') : '(Schema Only)'
          relation_header_title = "#{title}: #{table_name} (Rows: #{row_count_str})"

          # Filter out metadata keys to get column data
          # Filter out metadata keys to get column data
          column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }

          # --- Full Analysis Output Logic (Schema-only handled in reporter.rb) ---
          # Removed DEBUG message
          all_rows_data = []
          initial_headers = ['Column', 'Type', 'Nulls (%)', 'Distinct', 'Stats', 'Found']
          # Use the row_count_str determined earlier
          # Ensure row_count_str is fetched correctly even if this module is somehow called directly
          # Note: This module is intended to be used via Reporter#print_compact_summary
          row_count_from_first_col = table_data.values.find { |v| v.is_a?(Hash) && v.key?(:count) }&.dig(:count) || 0

          column_data.each do |col_sym, col_info|
            stats = table_data[col_sym] # Get the full stats hash, col_info might be just basic schema
            next unless stats.is_a?(Hash) # Ensure we have stats data

            # Calculate display strings
            type_str = if stats[:type] && stats[:type] != stats[:db_type]
                         "#{stats[:type]} / #{stats[:db_type]}"
                       else
                         stats[:db_type]
                       end
            null_count = stats[:null_count].to_i
            null_perc_str = row_count_from_first_col.positive? ? "#{(null_count * 100.0 / row_count_from_first_col).round(1)}%" : "N/A"
            distinct_str = if stats[:distinct_count]
                             "#{stats[:distinct_count]}#{stats[:is_unique] ? ' (U)' : ''}"
                           else
                             ''
                           end

            # Format aggregate stats
            # Use DbReport::Utils.format_stats_for_summary directly if module used standalone
            formatted = DbReport::Utils.format_stats_for_summary(stats)
            stats_parts = []
            stats_parts << "Min: #{truncate_value(formatted[:min], 15)}" if formatted[:min]
            stats_parts << "Max: #{truncate_value(formatted[:max], 15)}" if formatted[:max]
            stats_parts << "Avg: #{formatted[:average]}" if formatted[:average]
            stats_parts << "AvgLen: #{formatted[:avg_length]}" if formatted[:avg_length]
            stats_parts << "True%: #{formatted[:true_percentage]}" if formatted[:true_percentage]
            stats_parts << "AvgItems: #{formatted[:avg_items]}" if formatted[:avg_items]
            if formatted[:most_frequent]&.any?
              top_val, top_count = formatted[:most_frequent].first
              stats_parts << "MostFreq: #{truncate_value(top_val, 15)} (#{top_count})"
            end
            stats_str = stats_parts.join(", ")

            # Found string
            found_str = stats[:found] ? colored_output("YES", :green, :bold) : ''

            # Add row data
            all_rows_data << [
              colored_output(col_sym.to_s, :yellow),
              type_str,
              null_perc_str,
              distinct_str,
              stats_str,
              found_str
            ]
          end # Closes column_data.each

          next if all_rows_data.empty? # Skip if no data for the table

          # Determine which columns are empty
          empty_column_indices = Set.new
          (1...initial_headers.length).each do |col_index|
            # Check if all values in this column index are empty strings
            is_empty = all_rows_data.all? { |row| row[col_index].to_s.strip.empty? }
            empty_column_indices << col_index if is_empty
          end

          # Filter headers and rows based on empty columns
          filtered_headers = initial_headers.reject.with_index { |_, index| empty_column_indices.include?(index) }
          filtered_rows = all_rows_data.map do |row|
            row.reject.with_index { |_, index| empty_column_indices.include?(index) }
          end

          # Build table with filtered data
          table_summary = Terminal::Table.new do |t|
            # Add header row spanning all columns
            t.add_row [{ value: relation_header_title, colspan: filtered_headers.length, alignment: :center }]
            t.add_separator # Add separator after header
            t.headings = filtered_headers
            t.rows = filtered_rows
            t.style = {
              border_x: "",
              border_y: " ",
              border_i: " ",
              border_top: false,
              border_bottom: false,
              border_left: false,
              border_right: false
            }
          end

          # Modify the rendering to remove heading separator line and empty lines
          rendered_table = table_summary.to_s.split("\n")
          cleaned_lines = []

          # Process each line
          rendered_table.each_with_index do |line, i|
            # Skip separator line (usually the second line in the rendered table)
            next if i == 1 && (line.include?('─') || line.include?('-') || line.strip.empty?)
            # Skip empty or space-only lines
            next if line.strip.empty?
            # Remove leading space that forms the left border
            line = line.sub(/^ /, '')
            cleaned_lines << line
          end

          puts cleaned_lines.join("\n")
          puts # Add extra line after each table

          column_data.each do |col_sym, col_info|
            stats = table_data[col_sym] # Get the full stats hash, col_info might be just basic schema
            next unless stats.is_a?(Hash) # Ensure we have stats data

            # Calculate display strings
            type_str = if stats[:type] && stats[:type] != stats[:db_type]
                         "#{stats[:type]} / #{stats[:db_type]}"
                       else
                         stats[:db_type]
                       end
            null_count = stats[:null_count].to_i
            null_perc_str = row_count.positive? ? "#{(null_count * 100.0 / row_count).round(1)}%" : "N/A"
            distinct_str = if stats[:distinct_count]
                             "#{stats[:distinct_count]}#{stats[:is_unique] ? ' (U)' : ''}"
                           else
                             ''
                           end

            # Format aggregate stats
            formatted = DbReport::Utils.format_stats_for_summary(stats)
            stats_parts = []
            stats_parts << "Min: #{truncate_value(formatted[:min], 15)}" if formatted[:min]
            stats_parts << "Max: #{truncate_value(formatted[:max], 15)}" if formatted[:max]
            stats_parts << "Avg: #{formatted[:average]}" if formatted[:average]
            stats_parts << "AvgLen: #{formatted[:avg_length]}" if formatted[:avg_length]
            stats_parts << "True%: #{formatted[:true_percentage]}" if formatted[:true_percentage]
            stats_parts << "AvgItems: #{formatted[:avg_items]}" if formatted[:avg_items]
            if formatted[:most_frequent]&.any?
              top_val, top_count = formatted[:most_frequent].first
              stats_parts << "MostFreq: #{truncate_value(top_val, 15)} (#{top_count})"
            end
            stats_str = stats_parts.join(", ")

            # Found string
            found_str = stats[:found] ? colored_output("YES", :green, :bold) : ''

            # Add row data
            all_rows_data << [
              colored_output(col_sym.to_s, :yellow),
              type_str,
              null_perc_str,
              distinct_str,
              stats_str,
              found_str
            ]
          end # Closes column_data.each

          next if all_rows_data.empty? # Skip if no data for the table

          # Determine which columns are empty
          empty_column_indices = Set.new
          (1...initial_headers.length).each do |col_index|
            # Check if all values in this column index are empty strings
            is_empty = all_rows_data.all? { |row| row[col_index].to_s.empty? }
            empty_column_indices << col_index if is_empty
          end

          # Filter headers and rows based on empty columns
          filtered_headers = initial_headers.reject.with_index { |_, index| empty_column_indices.include?(index) }
          filtered_rows = all_rows_data.map do |row|
            row.reject.with_index { |_, index| empty_column_indices.include?(index) }
          end

          # Build table with filtered data
          table_summary = Terminal::Table.new do |t|
            # Add header row spanning all columns
            t.add_row [{ value: relation_header_title, colspan: filtered_headers.length, alignment: :center }]
            t.add_separator # Add separator after header
            t.headings = filtered_headers
            t.rows = filtered_rows
            t.style = {
              border_x: "",
              border_y: " ",
              border_i: " ",
              border_top: false,
              border_bottom: false,
              border_left: false,
              border_right: false
            }
          end

          # Modify the rendering to remove heading separator line and empty lines
          rendered_table = table_summary.to_s.split("\n")
          cleaned_lines = []

          # Process each line
          rendered_table.each_with_index do |line, i|
            # Skip separator line (usually the second line in the rendered table)
            next if i == 1 && (line.include?('─') || line.include?('-') || line.strip.empty?)
            # Skip empty or space-only lines
            next if line.strip.empty?
            # Remove leading space that forms the left border
            line = line.sub(/^ /, '')
            cleaned_lines << line
          end

          puts cleaned_lines.join("\n")
          puts # Add extra line after each table
        end

        # Add footer with any search findings summary
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
        end # Closes if search_summary_data
      end # Closes self.format
    end # Closes module CompactFormatter
  end # Closes module Reporter
end # Closes module DbReport
