# frozen_string_literal: true

require "json"
require "fileutils"
require "set"
require "terminal-table" # Moved from print_compact_summary
module DbReport
  class Reporter
    include Utils # Include Utils for formatting, logging, etc.

    attr_reader :report_data

    def initialize(report_data)
      @report_data = report_data
    end

    # Format stats for the summary output view
    def format_stats_for_summary(stats)
      formatted_stats = {}
      type = (stats[:type]&.to_sym rescue nil) # Type from col_stats
      is_json_type = type.to_s.include?("json") || (type.to_s.empty? && stats[:db_type].to_s.include?("json"))

      # Common stats
      formatted_stats[:min] = stats[:min] unless stats[:min].nil?
      formatted_stats[:max] = stats[:max] unless stats[:max].nil?

      # Type-specific stats
      case type
      when :string, :text, :xml, :blob, :enum, :inet
        formatted_stats[:avg_length] = stats[:avg]&.round(1) if stats[:avg]
      when :integer, :float, :decimal
        formatted_stats[:average] = stats[:avg]&.round(2) if stats[:avg]
      when :boolean
        formatted_stats[:true_percentage] = stats[:true_percentage]&.round(1) if stats[:true_percentage]
      when :array
        formatted_stats[:avg_items] = stats[:avg]&.round(1) if stats[:avg] # Avg array items
      when :json, :jsonb
        formatted_stats[:avg_length] = stats[:avg]&.round(1) if stats[:avg] # Avg length of text representation
      when :uuid
        formatted_stats[:avg_length] = stats[:avg]&.round(1) if stats[:avg] # Avg length of text representation
      end

      # Fallback for JSON if type wasn't inferred correctly but db_type indicates it
      if is_json_type && stats[:avg] && !formatted_stats.key?(:avg_length)
        formatted_stats[:avg_length] = stats[:avg]&.round(1)
      end

      # Add frequency data (use raw data, format during printing)
      formatted_stats[:most_frequent] = stats[:most_frequent] if stats[:most_frequent]&.any?
      formatted_stats[:least_frequent] = stats[:least_frequent] if stats[:least_frequent]&.any?

      # Add search value findings
      if stats[:found] && stats[:search_value]
        formatted_stats[:found] = true
        formatted_stats[:search_value] = stats[:search_value]
      end

      formatted_stats
    end

    # Print the analysis summary to the console
    def print_summary
      meta = report_data[:metadata]
      puts colored_output("\n--- Database Analysis Summary (Sequel) ---", :magenta, :bold)
      puts colored_output("Adapter: #{meta[:database_adapter]}, Type: #{meta[:database_type]}, Version: #{meta[:database_version]}", :magenta)
      puts colored_output("Generated: #{meta[:generated_at]}, Duration: #{meta[:analysis_duration_seconds]}s", :magenta)
      puts colored_output("Tables Analyzed: #{meta[:analyzed_tables].length}", :magenta)

      # Display search value if it was used
      if meta[:search_value]
        puts colored_output("Search Value: #{meta[:search_value]}", :magenta, :bold)
      end

      report_data[:tables].each do |table_name, table_data|
        # Guard clause for schema-only mode
        if table_data[:schema_only]
          print_schema_only_summary_for_relation(table_name, table_data)
          next
        end
        # --- Full Summary Logic (now runs if not schema_only) ---
        puts colored_output("\nTable: #{table_name}", :cyan, :bold)

        if table_data.is_a?(Hash) && table_data[:error]
          puts colored_output("Table: #{table_name} - Error: #{table_data[:error]}", :red)
          next
        end

        # Check if data is malformed (not hash or no columns with :count key)
        is_malformed = !table_data.is_a?(Hash) || !table_data.values.any? do |v|
                         v.is_a?(Hash) && v.key?(:count)
                       end
        if is_malformed
          puts colored_output("Table: #{table_name} - Skipping malformed data (no valid column stats found)", :yellow)
          next
        end

        # Filter out metadata keys before counting columns and getting row count
        column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }
        column_count = column_data.keys.length
        # Safely access count from the first *actual* column's stats hash
        first_col_stats = column_data.values.first || {}
        row_count = first_col_stats[:count] || "N/A" # Access :count symbol
        puts colored_output("  Rows: #{row_count}, Columns: #{column_count}", :white)

        # Iterate only over actual column data
        column_data.each do |column_name, stats|
          unique_marker = stats[:is_unique] ? colored_output(" (unique)", :light_blue) : ""
          found_marker = stats[:found] ? colored_output(" [FOUND]", :green, :bold) : ""
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

          formatted = format_stats_for_summary(stats) # Use instance method

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
        end # Closes column iteration
      end # Closes tables iteration

      # Add search summary at the end of the report, unless in schema-only mode
      if meta[:search_value] && !meta[:schema_only]
        total_found = 0
        found_locations = []

        report_data[:tables].each do |table_name, table_data|
          # Skip if schema only or no valid column data
          next if table_data[:schema_only]
          next unless table_data.is_a?(Hash) && table_data.values.any? do |v|
                        v.is_a?(Hash) && v.key?(:count)
                      end

          # Filter out metadata keys
          column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }

          column_data.each do |column_name, stats|
            if stats[:found] && stats[:search_value]
              total_found += 1
              found_locations << "#{table_name}.#{column_name}"
            end
          end
        end

        if total_found > 0
          puts colored_output("\nSearch Summary", :green, :bold)
          puts colored_output("Value '#{meta[:search_value]}' found in #{total_found} column(s):", :green)
          found_locations.each do |location|
            puts colored_output("  - #{location}", :green)
          end
        else
          puts colored_output("\nValue '#{meta[:search_value]}' not found in any column", :yellow)
        end
      end
    end

    # Print a summary formatted for GPT consumption
    def print_gpt_summary
      meta = report_data[:metadata]
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
        if table_data[:schema_only]
          print_schema_only_gpt_for_relation(table_name, table_data)
        else
          # --- Full GPT Summary Logic ---
          puts "\n## Table: #{table_name}"

          if table_data.is_a?(Hash) && table_data[:error]
            puts "  - Error: #{table_data[:error]}"
            next
          end

          # Check if there's any valid column data, ignoring metadata keys
          unless table_data.is_a?(Hash) && table_data.values.any? do |v|
                   v.is_a?(Hash) && v.key?(:count)
                 end
            puts "  - Skipping malformed data for table (no valid column stats found): #{table_name}"
            next
          end

          # Filter out metadata keys before counting columns and getting row count
          column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }
          column_count = column_data.keys.length
          first_col_stats = column_data.values.first || {}
          row_count = first_col_stats[:count] || "N/A"
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
            if total_count.positive?
              null_perc_str = "(#{(null_count.to_f / total_count * 100).round(1)}%)"
            else
              null_perc_str = ""
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
            formatted = format_stats_for_summary(stats)
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
          end # Closes column iteration
        end # Closes if/else for schema_only
      end # Closes tables iteration

      # Add search summary at the end of the report, unless in schema-only mode
      if meta[:search_value] && !meta[:schema_only]
        total_found = 0
        found_locations = []

        report_data[:tables].each do |table_name, table_data|
          # Skip if schema only or no valid column data
          next if table_data[:schema_only]
          next unless table_data.is_a?(Hash) && table_data.values.any? do |v|
                        v.is_a?(Hash) && v.key?(:count)
                      end

          # Filter out metadata keys
          column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }

          column_data.each do |column_name, stats|
            if stats[:found] && stats[:search_value]
              total_found += 1
              found_locations << "#{table_name}.#{column_name}"
            end
          end
        end

        puts "\n## Search Summary"
        if total_found > 0
          puts "- **Value:** '#{meta[:search_value]}'"
          puts "- **Found In:** #{total_found} column(s)"
          puts "- **Locations:**"
          found_locations.each do |location|
            puts "  - #{location}"
          end
        else
          puts "- Value '#{meta[:search_value]}' not found in any column"
        end
      end # Closes if meta[:search_value] && !meta[:schema_only]
    end # Closes print_gpt_summary method

    # Print a compact summary using tables
    def print_compact_summary
      meta = report_data[:metadata]

      # Print title manually before the table
      puts colored_output("Database Analysis Summary", :magenta, :bold)

      # Print Metadata Table - no borders and left-aligned
      metadata_table = Terminal::Table.new do |t|
        t.headings = ["Parameter", "Value"]
        rows = [
          ["Adapter", meta[:database_adapter]],
          ["Type", meta[:database_type]],
          ["Version", meta[:database_version]],
          ["Generated", meta[:generated_at]],
          ["Duration", "#{meta[:analysis_duration_seconds]}s"],
          ["Tables Analyzed", meta[:analyzed_tables].length]
        ]

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

      # Clean up the table output
      rendered_table = metadata_table.to_s.split("\n")
      cleaned_lines = rendered_table.select { |line| !line.strip.empty? && !line.include?("─") && !line.include?("-") }
      puts cleaned_lines.join("\n")
      puts

      # Process each table
      report_data[:tables].each do |table_name, table_data|
        if table_data[:schema_only]
          print_schema_only_compact_for_relation(table_name, table_data)
        else
          # Skip full analysis for debugging
          puts "Table: #{table_name} - Full analysis skipped, please fix syntax"
          puts "Table: #{table_name} - Full analysis skipped, please fix syntax"
        end
      end
    end # Closes print_compact_summary method

    def write_json(output_file)
      # Create a deep copy to modify for JSON output without affecting original data
      report_for_json = Marshal.load(Marshal.dump(report_data))

      # Remove aggregation data from tables if schema-only
      if report_data[:metadata][:schema_only]
        report_for_json[:tables].transform_values! do |table_data|
          next table_data unless table_data.is_a?(Hash) # Skip errors or malformed data
          # Keep metadata keys and schema_only flag
          schema_keys_to_keep = DbReport::Utils::METADATA_KEYS + [:schema_only]
          # Filter columns: Keep only the column name and its {type:, db_type:} hash
          filtered_columns = table_data.select do |k, _|
                               !schema_keys_to_keep.include?(k)
                             end.transform_values do |col_stats|
            next col_stats unless col_stats.is_a?(Hash) # Should always be hash
            { type: col_stats[:type], db_type: col_stats[:db_type] }
          end
          # Keep existing metadata
          metadata_part = table_data.select { |k, _| schema_keys_to_keep.include?(k) }
          # Combine metadata and filtered columns
          metadata_part.merge(filtered_columns)
        end
      else
        # Add search summary if NOT schema-only and search was performed
        if report_data[:metadata][:search_value]
          search_summary = {
            search_value: report_data[:metadata][:search_value],
            total_found: 0,
            found_locations: []
          }
          report_data[:tables].each do |table_name, table_data|
            next if table_data[:schema_only] # Should not happen here, but safe check
            next unless table_data.is_a?(Hash) && table_data.values.any? do |v|
                          v.is_a?(Hash) && v.key?(:count)
                        end
            column_data = table_data.reject { |k, _| DbReport::Utils::METADATA_KEYS.include?(k) }
            column_data.each do |column_name, stats|
              if stats[:found]
                search_summary[:total_found] += 1
                search_summary[:found_locations] << "#{table_name}.#{column_name}"
              end
            end
          end
          # Add search_summary to the JSON version
          report_for_json[:search_summary] = search_summary
        end
      end

      # Convert all data to be JSON-safe (handles symbols, etc.)
      final_json_data = make_json_safe(report_for_json)

      # Use a default output file if none provided
      output_file ||= File.join(
        Dir.pwd, "db_report_#{Time.now.strftime('%Y%m%d_%H%M%S')}.json"
      )

      # Ensure the directory exists
      dir = File.dirname(output_file)
      FileUtils.mkdir_p(dir) unless File.directory?(dir)

      # Write the JSON file
      File.open(output_file, "w") do |file|
        file.write(JSON.pretty_generate(final_json_data))
      end

      puts colored_output("Report written to #{output_file}", :green)
    end

    private

    # --- Schema-Only Helper Methods ---

    def print_schema_only_summary_for_relation(table_name, table_data)
      puts colored_output(
        "\n#{get_relation_title(table_data[:relation_type])}: #{table_name} (Schema Only)", :cyan, :bold
      )
      print_view_metadata_summary(table_data)

      column_data = table_data.reject do |k, _|
        DbReport::Utils::METADATA_KEYS.include?(k) || k == :schema_only
      end
      column_data.each do |column_name, col_info|
        type_str = format_type_string(col_info[:type], col_info[:db_type])
        puts colored_output("  - #{column_name}", :yellow)
        puts "    Type:          #{type_str}"
      end
    end

    def print_schema_only_gpt_for_relation(table_name, table_data)
      puts "\n## #{get_relation_title(table_data[:relation_type])}: #{table_name} (Schema Only)"
      print_view_metadata_gpt(table_data)

      column_data = table_data.reject do |k, _|
        DbReport::Utils::METADATA_KEYS.include?(k) || k == :schema_only
      end
      puts "### Columns Details:"
      column_data.each do |column_name, col_info|
        type_str = format_type_string(col_info[:type], col_info[:db_type])
        puts "- **`#{column_name}`**"
        puts "    - Type: #{type_str}"
      end
    end

    def print_schema_only_compact_for_relation(table_name, table_data)
      puts # Add space before table
      relation_title = get_relation_title(table_data[:relation_type])
      # Print title separately, styled like the full report
      puts colored_output("#{relation_title}: #{table_name}", :cyan, :bold)
      # Print view metadata separately if applicable
      print_view_metadata_compact(table_data)

      column_data = table_data.reject do |k, _|
        DbReport::Utils::METADATA_KEYS.include?(k) || k == :schema_only
      end
      schema_rows = column_data.map do |col_sym, col_info|
        type_str = format_type_string(col_info[:type], col_info[:db_type])
        [colored_output(col_sym.to_s, :yellow), type_str]
      end

      # Create the table with borderless style
      schema_table = Terminal::Table.new do |t|
        t.headings = ["Column", "Type"]
        t.rows = schema_rows
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

      # Render and clean up the table output like the full report
      rendered_table = schema_table.to_s.split("\n")
      cleaned_lines = []
      rendered_table.each_with_index { |line, i|
        # Skip separator line (usually the second line)
        next if i == 1 && (line.include?("─") || line.include?("-") || line.strip.empty?)
        next if line.strip.empty?
        # Remove leading space
        line = line.sub(/^ /, "")
        cleaned_lines << line
      }

      puts cleaned_lines.join("\n")
      puts # Add space after table
    end

    # --- Helper methods for schema-only view metadata ---

    def print_view_metadata_summary(table_data)
      if [:view, :materialized_view].include?(table_data[:relation_type])
        puts "  View Information:"
        puts "    Definition:    #{truncate_value(table_data[:view_definition])}" if table_data[:view_definition]
        puts "    Dependencies:  #{table_data[:dependencies].join(', ')}" if table_data[:dependencies]&.any?
        puts "    Last Refresh:  #{table_data[:last_refresh]}" if table_data[:last_refresh] # For MVs
      end
    end

    def print_view_metadata_gpt(table_data)
      if [:view, :materialized_view].include?(table_data[:relation_type])
        puts "\n### View Information:"
        puts "    - **Definition:** `#{truncate_value(table_data[:view_definition])}`" if table_data[:view_definition]
        puts "    - **Dependencies:** #{table_data[:dependencies].map { |d| "`#{d}`" }.join(', ')}" if table_data[:dependencies]&.any?
        puts "    - **Last Refresh:** #{table_data[:last_refresh]}" if table_data[:last_refresh] # For MVs
      end
    end

    # Print View/MV metadata for compact format
    def print_view_metadata_compact(table_data)
      if [:view, :materialized_view].include?(table_data[:relation_type])
        # Simple key-value output, left-aligned
        puts "  Definition:    #{truncate_value(table_data[:view_definition])}" if table_data[:view_definition]
        puts "  Dependencies:  #{table_data[:dependencies].join(', ')}" if table_data[:dependencies]&.any?
        puts "  Last Refresh:  #{table_data[:last_refresh]}" if table_data[:last_refresh] # For MVs
      end
    end

    # --- General Helpers ---

    def get_relation_title(relation_type)
      case relation_type
      when :table then "Table"
      when :view then "View"
      when :materialized_view then "Materialized View"
      else "Relation"
      end
    end

    def format_type_string(type, db_type)
      type_part = type.to_s
      db_type_part = db_type.to_s
      if type_part.empty? || type_part == db_type_part
        db_type_part
      else
        "#{type_part} / #{db_type_part}"
      end
    end

    # Convert data to be JSON-safe
    def make_json_safe(data)
      case data
      when Hash
        data.transform_keys(&:to_s).transform_values { |v| make_json_safe(v) }
      when Array
        data.map { |item| make_json_safe(item) }
      when Symbol
        data.to_s
      else
        data
      end
    end
  end # Closes class Reporter
end # Closes module DbReport
