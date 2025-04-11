# frozen_string_literal: true

require 'json'
require 'fileutils'
require 'terminal-table' # Added for compact format
require 'set'

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
      type = stats[:type]&.to_sym rescue nil # Type from col_stats
      is_json_type = type.to_s.include?('json') || (type.to_s.empty? && stats[:db_type].to_s.include?('json'))

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

      formatted_stats
    end

    # Print the analysis summary to the console
    def print_summary
      meta = report_data[:metadata]
      puts colored_output("
--- Database Analysis Summary (Sequel) ---", :magenta, :bold)
      puts colored_output("Adapter: #{meta[:database_adapter]}, Type: #{meta[:database_type]}, Version: #{meta[:database_version]}", :magenta)
      puts colored_output("Generated: #{meta[:generated_at]}, Duration: #{meta[:analysis_duration_seconds]}s", :magenta)
      puts colored_output("Tables Analyzed: #{meta[:analyzed_tables].length}", :magenta)

      report_data[:tables].each do |table_name, table_data|
        puts colored_output("
Table: #{table_name}", :cyan, :bold)

        if table_data.is_a?(Hash) && table_data[:error]
          puts colored_output("  Error: #{table_data[:error]}", :red)
          next
        end

        unless table_data.is_a?(Hash) && table_data.values.first.is_a?(Hash)
           puts colored_output("  Skipping malformed data for table: #{table_name}", :yellow)
           next
        end

        column_count = table_data.keys.length
        # Safely access count from the first column's stats hash
        first_col_stats = table_data.values.first || {}
        row_count = first_col_stats[:count] || 'N/A' # Access :count symbol
        puts colored_output("  Rows: #{row_count}, Columns: #{column_count}", :white)

        table_data.each do |column_name, stats|
          next unless stats.is_a?(Hash) # Ensure stats is a hash

          unique_marker = stats[:is_unique] ? colored_output(' (unique)', :light_blue) : ''
          puts colored_output("  - #{column_name}#{unique_marker}", :yellow)

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
          puts "    Distinct:      #{stats[:distinct_count]}" if stats[:distinct_count] && stats[:distinct_count].to_i > 0

          # Print frequency info
          if formatted[:most_frequent]
             puts "    Most Frequent:"
             formatted[:most_frequent].each_with_index do |(v, c), i|
                prefix = i == 0 ? "      - " : "        "
                # Use truncate_value for display
                puts "#{prefix}#{truncate_value(v)} (#{c})"
             end
          end
          if formatted[:least_frequent]
             puts "    Least Frequent:"
             formatted[:least_frequent].each_with_index do |(v, c), i|
                prefix = i == 0 ? "      - " : "        "
                puts "#{prefix}#{truncate_value(v)} (#{c})"
             end
          end
        end
      end
    end

    # Print a summary formatted for GPT consumption
    def print_gpt_summary
      meta = report_data[:metadata]
      puts "# Database Analysis Report (GPT Format)"
      puts
      puts "- **Adapter:** #{meta[:database_adapter]}"
      puts "- **Type:** #{meta[:database_type]}"
      puts "- **Version:** #{meta[:database_version]}"
      puts "- **Generated:** #{meta[:generated_at]}"
      puts "- **Duration:** #{meta[:analysis_duration_seconds]}s"
      puts "- **Tables Analyzed:** #{meta[:analyzed_tables].length}"

      report_data[:tables].each do |table_name, table_data|
        puts "\n## Table: #{table_name}"

        if table_data.is_a?(Hash) && table_data[:error]
          puts "  - Error: #{table_data[:error]}"
          next
        end

        unless table_data.is_a?(Hash) && table_data.values.first.is_a?(Hash)
          puts "  - Skipping malformed data for table: #{table_name}"
          next
        end

        column_count = table_data.keys.length
        first_col_stats = table_data.values.first || {}
        row_count = first_col_stats[:count] || 'N/A'
        puts "\n- **Rows:** #{row_count}"
        puts "- **Columns:** #{column_count}"
        puts
        puts "### Columns Details:"

        table_data.each do |column_name, stats|
          next unless stats.is_a?(Hash)

          type_part = stats[:type].to_s
          db_type_part = stats[:db_type].to_s
          type_str = if type_part.empty? || type_part == db_type_part
                       db_type_part
                     else
                       "#{type_part} / #{db_type_part}"
                     end

          puts "- **`#{column_name}`**"
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
        end
      end
    end

    # Print a compact summary using tables
    def print_compact_summary
      require 'terminal-table' # Ensure it's loaded
      meta = report_data[:metadata]

      # Print title manually before the table
      puts colored_output(" Database Analysis Summary", :magenta, :bold)

      # Print Metadata Table - no borders and left-aligned
      metadata_table = Terminal::Table.new do |t|
        t.headings = ['Parameter', 'Value']
        t.rows = [
          ['Adapter', meta[:database_adapter]],
          ['Type', meta[:database_type]],
          ['Version', meta[:database_version]],
          ['Generated', meta[:generated_at]],
          ['Duration', "#{meta[:analysis_duration_seconds]}s"],
          ['Tables Analyzed', meta[:analyzed_tables].length]
        ]
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
        cleaned_lines << line
      end

      puts cleaned_lines.join("\n")
      puts # Add space after metadata table

      # Print Table Summaries
      report_data[:tables].each do |table_name, table_data|
        puts # Add space before each table

        if table_data.is_a?(Hash) && table_data[:error]
          puts colored_output(" Table: #{table_name} - Error: #{table_data[:error]}", :red)
          next
        end

        unless table_data.is_a?(Hash) && table_data.values.first.is_a?(Hash)
           puts colored_output(" Table: #{table_name} - Skipping malformed data", :yellow)
           next
        end

        first_col_stats = table_data.values.first || {}
        row_count = first_col_stats[:count] || 'N/A'

        # Prepare data before creating the table
        all_rows_data = []
        initial_headers = ['Column', 'Type', 'Nulls (%)', 'Distinct', 'Min', 'Max', 'Average/AvgLen', 'Most Frequent']

        table_data.each do |column_name, stats|
          next unless stats.is_a?(Hash)

          type_str = stats[:db_type].to_s
          null_count = stats[:null_count].to_i
          total_count = stats[:count].to_i
          null_perc_str = total_count.positive? ? "#{null_count} (#{(null_count.to_f / total_count * 100).round(1)}%)" : "#{null_count}"
          distinct_str = stats[:distinct_count] ? "#{stats[:distinct_count]}#{stats[:is_unique] ? ' (U)' : ''}" : ''

          formatted = format_stats_for_summary(stats)
          min_val = formatted[:min].nil? ? '' : truncate_value(formatted[:min], 15)
          max_val = formatted[:max].nil? ? '' : truncate_value(formatted[:max], 15)
          avg_val = if formatted.key?(:average)
                      formatted[:average].to_s # Ensure string for emptiness check
                    elsif formatted.key?(:avg_length)
                      formatted[:avg_length].to_s
                    elsif formatted.key?(:true_percentage)
                      "#{formatted[:true_percentage]}% True"
                    elsif formatted.key?(:avg_items)
                      formatted[:avg_items].to_s
                    else
                      ''
                    end

          most_freq_str = ''
          if formatted[:most_frequent]&.any?
            top_val, top_count = formatted[:most_frequent].first
            most_freq_str = "#{truncate_value(top_val, 15)} (#{top_count})"
          end

          # Collect row data (raw values before colorization where possible for checks)
          all_rows_data << [
            colored_output(column_name.to_s, :yellow), # Color applied here is ok
            type_str,
            null_perc_str,
            distinct_str,
            min_val,
            max_val,
            avg_val,
            most_freq_str
          ]
        end

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

        # Print table title manually
        puts " " + colored_output("Table: #{table_name} (Rows: #{row_count})", :cyan, :bold)

        # Build table with filtered data
        table_summary = Terminal::Table.new do |t|
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
          cleaned_lines << line
        end

        puts cleaned_lines.join("\n")
        puts # Add extra line after each table
      end
    end

    # Write the JSON report to a file or stdout
    def write_json(output_file)
      # Use make_json_safe from Utils module
      report_for_json = make_json_safe(report_data)
      json_report = JSON.pretty_generate(report_for_json)

      if output_file
        begin
          FileUtils.mkdir_p(File.dirname(output_file))
          File.write(output_file, json_report)
          print_info "Report successfully written to #{output_file}"
        rescue StandardError => e
          print_warning "Error writing report to file #{output_file}: #{e.message}"
        end
      else
        puts json_report
      end
    end
  end
end
