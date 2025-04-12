<search/>
# frozen_string_literal: true

require &#39;terminal-table&#39;
require_relative &#39;../utils&#39;

module DbReport
  module Reporter
    # Formats the report data into a compact table suitable for console output.
    module CompactFormatter
      extend DbReport::Utils

      def self.format(report_data)
        meta = report_data[:metadata]

        # Print title manually before the table
        puts colored_output(&quot;Database Analysis Summary&quot;, :magenta, :bold)

        # Print Metadata Table - no borders and left-aligned
        metadata_table = Terminal::Table.new do |t|
          t.headings = [&#39;Parameter&#39;, &#39;Value&#39;]
          rows = [
            [&#39;Adapter&#39;, meta[:database_adapter]],
            [&#39;Type&#39;, meta[:database_type]],
            [&#39;Version&#39;, meta[:database_version]],
            [&#39;Generated&#39;, meta[:generated_at]],
            [&#39;Duration&#39;, &quot;#{meta[:analysis_duration_seconds]}s&quot;],
            [&#39;Tables Analyzed&#39;, meta[:analyzed_tables].length]
          ]

          # Add search value to metadata table if present
          rows &lt;&lt; [&#39;Search Value&#39;, meta[:search_value]] if meta[:search_value]

          t.rows = rows
          t.style = {
            border_x: &quot;&quot;,
            border_y: &quot; &quot;,
            border_i: &quot; &quot;,
            border_top: false,
            border_bottom: false,
            border_left: false,
            border_right: false
          }
        end

        # Modify the rendering to remove heading separator line and extra empty lines
        rendered_table = metadata_table.to_s.split(&quot;\n&quot;)
        cleaned_lines = []

        # Process each line
        rendered_table.each_with_index do |line, i|
          # Skip separator line (usually the second line in the rendered table)
          next if i == 1 &amp;&amp; (line.include?(&#39;─&#39;) || line.include?(&#39;-&#39;) || line.strip.empty?)
          # Skip empty or space-only lines
          next if line.strip.empty?
          # Remove leading space that forms the left border
          line = line.sub(/^ /, &#39;&#39;)
          cleaned_lines &lt;&lt; line
        end

        puts cleaned_lines.join(&quot;\n&quot;)
        puts # Add space after metadata table

        # Print Table Summaries
        report_data[:tables].each do |table_name, table_data|
          puts # Add space before each table

          if table_data.is_a?(Hash) &amp;&amp; table_data[:error]
            puts colored_output(&quot;Table: #{table_name} - Error: #{table_data[:error]}&quot;, :red)
            next
          end

          # Check if there&#39;s any valid column data, ignoring metadata keys
          unless table_data.is_a?(Hash) &amp;&amp; table_data.values.any? { |v| v.is_a?(Hash) &amp;&amp; v.key?(:count) }
            puts colored_output(&quot;Table: #{table_name} - Skipping malformed data (no valid column stats found)&quot;, :yellow)
            next
          end

          # Filter out metadata keys before getting row count
          column_data = table_data.reject { |k, _| METADATA_KEYS.include?(k) }
          first_col_stats = column_data.values.first || {}
          row_count = first_col_stats[:count] || &#39;N/A&#39;

          # Prepare data before creating the table
          all_rows_data = []
          initial_headers = [&#39;Column&#39;, &#39;Type&#39;, &#39;Nulls (%)&#39;, &#39;Distinct&#39;, &#39;Stats&#39;, &#39;Found&#39;]

          # Iterate only over actual column data
          column_data.each do |column_name, stats|
            next unless stats.is_a?(Hash)

            type_str = stats[:db_type].to_s
            null_count = stats[:null_count].to_i
            total_count = stats[:count].to_i
            null_perc_str = total_count.positive? ? &quot;#{null_count} (#{(null_count.to_f / total_count * 100).round(1)}%)&quot; : &quot;#{null_count}&quot;
            distinct_str = stats[:distinct_count] ? "#{stats[:distinct_count]}#{stats[:is_unique] ? ' (U)' : ''}" : ''

            formatted = DbReport::Utils.format_stats_for_summary(stats)

            # Create combined stats string
            stats_parts = []
            stats_parts &lt;&lt; &quot;Min: #{truncate_value(formatted[:min], 15)}&quot; if formatted[:min]
            stats_parts &lt;&lt; &quot;Max: #{truncate_value(formatted[:max], 15)}&quot; if formatted[:max]
            stats_parts &lt;&lt; &quot;Avg: #{formatted[:average]}&quot; if formatted[:average]
            stats_parts &lt;&lt; &quot;AvgLen: #{formatted[:avg_length]}&quot; if formatted[:avg_length]
            stats_parts &lt;&lt; &quot;True%: #{formatted[:true_percentage]}&quot; if formatted[:true_percentage]
            stats_parts &lt;&lt; &quot;AvgItems: #{formatted[:avg_items]}&quot; if formatted[:avg_items]

            # Add most frequent to stats if available
            if formatted[:most_frequent]&amp;.any?
              top_val, top_count = formatted[:most_frequent].first
              stats_parts &lt;&lt; &quot;MostFreq: #{truncate_value(top_val, 15)} (#{top_count})&quot;
            end

            stats_str = stats_parts.join(&quot;, &quot;)

            # Add found value column
            found_str = formatted[:found] ? colored_output(&quot;YES&quot;, :green, :bold) : &#39;&#39;

            # Collect row data (raw values before colorization where possible for checks)
            all_rows_data &lt;&lt; [
              colored_output(column_name.to_s, :yellow), # Color applied here is ok
              type_str,
              null_perc_str,
              distinct_str,
              stats_str,
              found_str
            ]
          end

          next if all_rows_data.empty? # Skip if no data for the table

          # Determine which columns are empty
          empty_column_indices = Set.new
          (1...initial_headers.length).each do |col_index|
            # Check if all values in this column index are empty strings
            is_empty = all_rows_data.all? { |row| row[col_index].to_s.empty? }
            empty_column_indices &lt;&lt; col_index if is_empty
          end

          # Filter headers and rows based on empty columns
          filtered_headers = initial_headers.reject.with_index { |_, index| empty_column_indices.include?(index) }
          filtered_rows = all_rows_data.map do |row|
            row.reject.with_index { |_, index| empty_column_indices.include?(index) }
          end

          # Print table title manually
          puts colored_output(&quot;Table: #{table_name} (Rows: #{row_count})&quot;, :cyan, :bold)

          # Build table with filtered data
          table_summary = Terminal::Table.new do |t|
            t.headings = filtered_headers
            t.rows = filtered_rows
            t.style = {
              border_x: &quot;&quot;,
              border_y: &quot; &quot;,
              border_i: &quot; &quot;,
              border_top: false,
              border_bottom: false,
              border_left: false,
              border_right: false
            }
          end

          # Modify the rendering to remove heading separator line and empty lines
          rendered_table = table_summary.to_s.split(&quot;\n&quot;)
          cleaned_lines = []

          # Process each line
          rendered_table.each_with_index do |line, i|
            # Skip separator line (usually the second line in the rendered table)
            next if i == 1 &amp;&amp; (line.include?(&#39;─&#39;) || line.include?(&#39;-&#39;) || line.strip.empty?)
            # Skip empty or space-only lines
            next if line.strip.empty?
            # Remove leading space that forms the left border
            line = line.sub(/^ /, &#39;&#39;)
            cleaned_lines &lt;&lt; line
          end

          puts cleaned_lines.join(&quot;\n&quot;)
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
        end
      end
    end
  end
end

