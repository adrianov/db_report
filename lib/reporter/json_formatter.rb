<search/>
# frozen_string_literal: true

require &#39;json&#39;
require &#39;fileutils&#39;
require_relative &#39;../utils&#39;

module DbReport
  module Reporter
    # Formats the report data into JSON format and writes it to a file.
    module JsonFormatter
      extend DbReport::Utils

      def self.format(report_data, output_file = nil)
        # Add search summary directly to report_data if needed
        meta = report_data[:metadata]
        if meta[:search_value]
          search_summary = {
            search_value: meta[:search_value],
            total_found: 0,
            found_locations: []
          }
          report_data[:tables].each do |table_name, table_data|
            # Ensure table_data is a hash and contains some column stats
            next unless table_data.is_a?(Hash) &amp;&amp; table_data.values.any? { |v| v.is_a?(Hash) &amp;&amp; v.key?(:count) }
            column_data = table_data.reject { |k, _| METADATA_KEYS.include?(k) }
            column_data.each do |column_name, stats|
              # Only add to search summary if the value was actually found in this column
              if stats.is_a?(Hash) &amp;&amp; stats[:found]
                search_summary[:total_found] += 1
                search_summary[:found_locations] &lt;&lt; &quot;#{table_name}.#{column_name}&quot;
              end
            end
          end # Closes report_data[:tables].each loop
          # Add search_summary directly to the original report data
          report_data[:search_summary] = search_summary
        end # Closes if meta[:search_value]

        # Now create the JSON-safe version *after* potential modification
        report_for_json = make_json_safe(report_data)

        # Use a default output file if none provided
        output_file ||= File.join(Dir.pwd, &quot;db_report_#{Time.now.strftime(&#39;%Y%m%d_%H%M%S&#39;)}.json&quot;)

        # Ensure the directory exists
        dir = File.dirname(output_file)
        FileUtils.mkdir_p(dir) unless File.directory?(dir)

        # Write the JSON file
        File.open(output_file, &#39;w&#39;) do |file|
          file.write(JSON.pretty_generate(report_for_json))
        end

        puts colored_output(&quot;Report written to #{output_file}&quot;, :green)
      end
    end
  end
end

