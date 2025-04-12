# frozen_string_literal: true

require_relative '../utils'

module DbReport
  module Reporter
    # Formats the report data into a Markdown format suitable for GPT consumption.
    module GptFormatter
      extend DbReport::Utils

      def self.format(report_data)
        meta = report_data[:metadata]
        meta = report_data[:metadata]
        puts "# Database Analysis Report (GPT Format)"
        puts
        puts "- **Adapter:** #{meta[:database_adapter]}"
        puts "- **Type:** #{meta[:database_type]}"
        puts "- **Version:** #{meta[:database_version]}"
        puts "- **Generated:** #{meta[:generated_at]}"
        puts "- **Duration:** #{meta[:analysis_duration_seconds]}s"
        puts "- **Tables Analyzed:** #{meta[:analyzed_tables].length}"

        # Display search value if it was used
        if meta[:search_value]
          puts "- **Search Value:** #{meta[:search_value]}"
        end

        report_data[:tables].each do |table_name, table_data|
          title = case table_data[:relation_type]
                  when :table then "Table"
                  when :view then "View"
                  when :materialized_view then "Materialized View"
                  else "Relation" # Default for unknown types
                  end
          puts "\n## #{title}: #{table_name}"
          # Display relation type and view-specific info
          rel_type = table_data[:relation_type]&.to_s&.capitalize
          puts "- **Type:** #{rel_type}" if rel_type

          if [:view, :materialized_view].include?(table_data[:relation_type])
            if table_data[:view_definition]
              puts "- **Definition:**"
              # Use the utility function for truncation
              puts "```sql\n#{DbReport::Utils.truncate_value(table_data[:view_definition], 200)}\n```"
            end
            if table_data[:dependencies]&.any?
              puts "- **Dependencies:** #{table_data[:dependencies].join(', ')}"
            end
            if table_data[:is_materialized] && table_data[:last_refresh]
              # Ensure time is formatted nicely if it's a Time object
          if table_data.is_a?(Hash) && table_data.key?(:error) && !table_data[:error].to_s.empty?
            puts "  - Error: #{table_data[:error].to_s}" # Ensure error is string
            # If there's an error, don't print schema details below
            next
          end # Closes error check
          # Check if we are in schema-only mode (no aggregate data like :count)
          is_schema_only = !table_data.values.any? { |v| v.is_a?(Hash) && v.key?(:count) }

          if is_schema_only
            # --- Schema-Only Output Logic ---
            puts "- **Mode:** Schema Only"
            # View Metadata is already printed above the error/schema check
            # Print Columns
            puts "- **Columns:**"
            column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }
            column_data.each do |col_sym, col_info|
              type_str = if col_info[:type] && col_info[:type] != col_info[:db_type]
                           "#{col_info[:type]} / #{col_info[:db_type]}"
                         else
                           col_info[:db_type]
                         end
              puts "  - `#{col_sym}` (#{type_str})"
            end
            next # Skip the rest of the loop for this relation
          end

          # --- Existing Full Analysis Output Logic Continues Below ---
          # --- Existing Full Analysis Output Logic Continues Below ---

          # Filter out metadata keys before counting columns and getting row count
          column_count = column_data.keys.length
          first_col_stats = column_data.values.first || {}
          row_count = first_col_stats[:count] || 'N/A'
          puts "\n- **Rows:** #{row_count}"
          puts "- **Columns:** #{column_count}"
          puts "### Columns Details:"

          # Iterate only over actual column data
          column_data.each do |column_name, stats|
            next unless stats.is_a?(Hash)

            type_part = stats[:type].to_s
            db_type_part = stats[:db_type].to_s
            type_str = if type_part.empty? || type_part == db_type_part
                         db_type_part
                       else
                         "#{type_part} / #{db_type_part}"
                       end

            found_marker = stats[:found] ? " [FOUND]" : ""
            puts "- **`#{column_name}`**#{found_marker}"
            puts "    - Type: #{type_str}"

            null_count = stats[:null_count].to_i
            total_count = stats[:count].to_i
            null_perc_str = if total_count.positive?
                              "(#{ (null_count.to_f / total_count * 100).round(1) }%)"
                            else
                              ''
                            end
            puts "    - Nulls: #{null_count} #{null_perc_str}"

            if stats[:distinct_count] && stats[:distinct_count].to_i > 0
              puts "    - Distinct Values: #{stats[:distinct_count]} #{stats[:is_unique] ? '(Unique)' : ''}"
            elsif stats[:is_unique]
               puts "    - Distinct Values: (Unique - count matches rows)"
            end

            # Include search value finding in the report
            if stats[:found] && stats[:search_value]
              puts "    - **Search Value Found:** '#{stats[:search_value]}'"
            end

            # Add simplified stats based on type (more concise for GPT)
            formatted = DbReport::Utils.format_stats_for_summary(stats)
            stat_parts = [] # Collect key stats
            stat_parts << "Min: #{truncate_value(formatted[:min], 40)}" if formatted.key?(:min)
            stat_parts << "Max: #{truncate_value(formatted[:max], 40)}" if formatted.key?(:max)
            stat_parts << "Avg: #{formatted[:average]}" if formatted.key?(:average)
            stat_parts << "AvgLen: #{formatted[:avg_length]}" if formatted.key?(:avg_length)
            stat_parts << "AvgItems: #{formatted[:avg_items]}" if formatted.key?(:avg_items)
            stat_parts << "True%: #{formatted[:true_percentage]}" if formatted.key?(:true_percentage)

            puts "    - Stats: #{stat_parts.join(', ')}" unless stat_parts.empty?

            if formatted[:most_frequent]&.any?
               top_val, top_count = formatted[:most_frequent].first
               puts "    - Most Frequent: #{truncate_value(top_val, 40)} (#{top_count})"
            end
          end
        end

        # Add search summary at the end of the report
        search_summary_data = DbReport::Utils.generate_search_summary(report_data)
        if search_summary_data
          puts "\n## Search Summary"
          if search_summary_data[:total_found] > 0
            puts "- **Value:** '#{search_summary_data[:search_value]}'"
            puts "- **Found In:** #{search_summary_data[:total_found]} column(s)"
            puts "- **Locations:**"
            search_summary_data[:found_locations].each do |location|
              puts "  - #{location}"
            end
          else
            puts "- Value '#{search_summary_data[:search_value]}' not found in any column"
          end
        end
      end
    end
  end
end

