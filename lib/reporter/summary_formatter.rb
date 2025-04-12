<search/>
# frozen_string_literal: true

require_relative &#39;../utils&#39;

module DbReport
  module Reporter
    # Formats the report data into a detailed summary for console output.
    module SummaryFormatter
      extend DbReport::Utils

      def self.format(report_data)
        meta = report_data[:metadata]
        puts colored_output(&quot;
--- Database Analysis Summary (Sequel) ---&quot;, :magenta, :bold)
        puts colored_output(&quot;Adapter: #{meta[:database_adapter]}, Type: #{meta[:database_type]}, Version: #{meta[:database_version]}&quot;, :magenta)
        puts colored_output(&quot;Generated: #{meta[:generated_at]}, Duration: #{meta[:analysis_duration_seconds]}s&quot;, :magenta)
        puts colored_output(&quot;Tables Analyzed: #{meta[:analyzed_tables].length}&quot;, :magenta)

        # Display search value if it was used
        if meta[:search_value]
          puts colored_output(&quot;Search Value: #{meta[:search_value]}&quot;, :magenta, :bold)
        end

        report_data[:tables].each do |table_name, table_data|
          puts colored_output(&quot;
Table: #{table_name}&quot;, :cyan, :bold)

          if table_data.is_a?(Hash) &amp;&amp; table_data[:error]
            puts colored_output(&quot;Table: #{table_name} - Error: #{table_data[:error]}&quot;, :red)
            next
          end

          # Check if there&#39;s any valid column data, ignoring metadata keys
          unless table_data.is_a?(Hash) &amp;&amp; table_data.values.any? { |v| v.is_a?(Hash) &amp;&amp; v.key?(:count) }
            puts colored_output(&quot;Table: #{table_name} - Skipping malformed data (no valid column stats found)&quot;, :yellow)
            next
          end

          # Filter out metadata keys before counting columns and getting row count
          column_data = table_data.reject { |k, _| METADATA_KEYS.include?(k) }
          column_count = column_data.keys.length
          # Safely access count from the first *actual* column&#39;s stats hash
          first_col_stats = column_data.values.first || {}
          row_count = first_col_stats[:count] || &#39;N/A&#39; # Access :count symbol
          puts colored_output(&quot;  Rows: #{row_count}, Columns: #{column_count}&quot;, :white)

          # Iterate only over actual column data
          column_data.each do |column_name, stats|

            unique_marker = stats[:is_unique] ? colored_output(&#39; (unique)&#39;, :light_blue) : &#39;&#39;
            found_marker = stats[:found] ? colored_output(&#39; [FOUND]&#39;, :green, :bold) : &#39;&#39;
            puts colored_output(&quot;  - #{column_name}#{unique_marker}#{found_marker}&quot;, :yellow)

            type_part = stats[:type].to_s
            db_type_part = stats[:db_type].to_s
            type_str = if type_part.empty? || type_part == db_type_part
                         db_type_part
                       else
                         &quot;#{type_part} / #{db_type_part}&quot;
                       end
            puts &quot;    Type:          #{type_str}&quot;

            null_count = stats[:null_count].to_i
            total_count = stats[:count].to_i
            if null_count &gt; 0
               null_perc = total_count.positive? ? (null_count.to_f / total_count * 100).round(1) : 0
               puts &quot;    Nulls:         #{null_count} (#{null_perc}%)&quot;
            end

            formatted = DbReport::Utils.format_stats_for_summary(stats) # Use Utils method

            # Print individual stats
            puts &quot;    Min:           #{truncate_value(formatted[:min])}&quot; if formatted.key?(:min)
            puts &quot;    Max:           #{truncate_value(formatted[:max])}&quot; if formatted.key?(:max)
            puts &quot;    Average:       #{formatted[:average]}&quot; if formatted.key?(:average)
            puts &quot;    Avg Length:    #{formatted[:avg_length]}&quot; if formatted.key?(:avg_length)
            puts &quot;    Avg Items:     #{formatted[:avg_items]}&quot; if formatted.key?(:avg_items)
            puts &quot;    True %:        #{formatted[:true_percentage]}%&quot; if formatted.key?(:true_percentage)

            # Print search value match if found
            if stats[:found] &amp;&amp; stats[:search_value]
              puts colored_output(&quot;    Found Match:    &#39;#{stats[:search_value]}&#39;&quot;, :green, :bold)
            end

            # Print most frequent value if present
            if formatted[:most_frequent]&amp;.any?
              top_val, top_count = formatted[:most_frequent].first
              puts &quot;    Most Frequent: #{truncate_value(top_val)} (#{top_count} times)&quot;
            end
          end
        end


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

