<search/>
# frozen_string_literal: true

require_relative &#39;../utils&#39;

module DbReport
  module Reporter
    # Formats the report data into a Markdown format suitable for GPT consumption.
    module GptFormatter
      extend DbReport::Utils

      def self.format(report_data)
        meta = report_data[:metadata]
        puts &quot;# Database Analysis Report (GPT Format)&quot;
        puts
        puts &quot;- **Adapter:** #{meta[:database_adapter]}&quot;
        puts &quot;- **Type:** #{meta[:database_type]}&quot;
        puts &quot;- **Version:** #{meta[:database_version]}&quot;
        puts &quot;- **Generated:** #{meta[:generated_at]}&quot;
        puts &quot;- **Duration:** #{meta[:analysis_duration_seconds]}s&quot;
        puts &quot;- **Tables Analyzed:** #{meta[:analyzed_tables].length}&quot;

        # Display search value if it was used
        if meta[:search_value]
          puts &quot;- **Search Value:** #{meta[:search_value]}&quot;
        end

        report_data[:tables].each do |table_name, table_data|
          # Determine title based on relation type
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
              refresh_time = table_data[:last_refresh].is_a?(Time) ? table_data[:last_refresh].iso8601 : table_data[:last_refresh]
              puts "- **Last Refresh:** #{refresh_time}"
            end
          end

          if table_data.is_a?(Hash) && table_data[:error]
            puts "  - Error: #{table_data[:error]}"
            next
          end

          # Check if there&#39;s any valid column data, ignoring metadata keys
          # Note: The check below implicitly handles the error case above as well
          unless table_data.is_a?(Hash) && table_data.values.any? { |v| v.is_a?(Hash) && v.key?(:count) }
            # If there's no error key but still no valid columns, then skip
            unless table_data.is_a?(Hash) && table_data[:error]
              puts "  - Skipping malformed data for relation (no valid column stats found): #{table_name}"
            end
            next
          end

          # Filter out metadata keys before counting columns and getting row count
          column_data = table_data.reject { |k, _| METADATA_KEYS.include?(k) }
          column_count = column_data.keys.length
          first_col_stats = column_data.values.first || {}
          row_count = first_col_stats[:count] || &#39;N/A&#39;
          puts &quot;\n- **Rows:** #{row_count}&quot;
          puts &quot;- **Columns:** #{column_count}&quot;
          puts &quot;### Columns Details:&quot;

          # Iterate only over actual column data
          column_data.each do |column_name, stats|
            next unless stats.is_a?(Hash)

            type_part = stats[:type].to_s
            db_type_part = stats[:db_type].to_s
            type_str = if type_part.empty? || type_part == db_type_part
                         db_type_part
                       else
                         &quot;#{type_part} / #{db_type_part}&quot;
                       end

            found_marker = stats[:found] ? &quot; [FOUND]&quot; : &quot;&quot;
            puts &quot;- **`#{column_name}`**#{found_marker}&quot;
            puts &quot;    - Type: #{type_str}&quot;

            null_count = stats[:null_count].to_i
            total_count = stats[:count].to_i
            null_perc_str = if total_count.positive?
                              &quot;(#{ (null_count.to_f / total_count * 100).round(1) }%)&quot;
                            else
                              &#39;&#39;
                            end
            puts &quot;    - Nulls: #{null_count} #{null_perc_str}&quot;

            if stats[:distinct_count] &amp;&amp; stats[:distinct_count].to_i &gt; 0
              puts &quot;    - Distinct Values: #{stats[:distinct_count]} #{stats[:is_unique] ? &#39;(Unique)&#39; : &#39;&#39;}&quot;
            elsif stats[:is_unique]
               puts &quot;    - Distinct Values: (Unique - count matches rows)&quot;
            end

            # Include search value finding in the report
            if stats[:found] &amp;&amp; stats[:search_value]
              puts &quot;    - **Search Value Found:** &#39;#{stats[:search_value]}&#39;&quot;
            end

            # Add simplified stats based on type (more concise for GPT)
            formatted = DbReport::Utils.format_stats_for_summary(stats)
            stat_parts = [] # Collect key stats
            stat_parts << "Min: #{truncate_value(formatted[:min], 40)}" if formatted.key?(:min)
            stat_parts &lt;&lt; &quot;Max: #{truncate_value(formatted[:max], 40)}&quot; if formatted.key?(:max)
            stat_parts &lt;&lt; &quot;Avg: #{formatted[:average]}&quot; if formatted.key?(:average)
            stat_parts &lt;&lt; &quot;AvgLen: #{formatted[:avg_length]}&quot; if formatted.key?(:avg_length)
            stat_parts &lt;&lt; &quot;AvgItems: #{formatted[:avg_items]}&quot; if formatted.key?(:avg_items)
            stat_parts &lt;&lt; &quot;True%: #{formatted[:true_percentage]}&quot; if formatted.key?(:true_percentage)

            puts &quot;    - Stats: #{stat_parts.join(&#39;, &#39;)}&quot; unless stat_parts.empty?

            if formatted[:most_frequent]&amp;.any?
               top_val, top_count = formatted[:most_frequent].first
               puts &quot;    - Most Frequent: #{truncate_value(top_val, 40)} (#{top_count})&quot;
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

