module DbReport
  module Utils
    require 'colorize'
    require 'set'
    require 'uri' # For database_name_present? helper

    # Helper to print colored output
    def colored_output(text, color, style = nil)
      text.colorize(color: color, mode: style)
    end

    # Helper to print warning messages
    def print_warning(message)
      puts colored_output("Warning: #{message}", :yellow)
    end

    # Helper to print info messages
    def print_info(message, color = :white, style = nil)
      puts colored_output(message, color, style)
    end

    # Helper to print debug messages
    def print_debug(message)
      puts colored_output("DEBUG: #{message}", :light_black) if $debug
    end

    # Helper to abort with an error message
    def abort_with_error(message)
      puts colored_output("Error: #{message}", :red, :bold)
      exit(1)
    end

    # Parse table identifier string (schema.table or just table)
    def parse_table_identifier(name_str)
      parts = name_str.split('.', 2)
      if parts.length == 2
        { schema: parts[0].to_sym, table: parts[1].to_sym }
      else
        { table: parts[0].to_sym } # Assume public schema or search path
      end
    end

    # Create a Sequel identifier for a table name
    def create_sequel_identifier(name_str)
      parsed = parse_table_identifier(name_str)
      if parsed[:schema]
        Sequel[parsed[:schema]][parsed[:table]]
      else
        Sequel[parsed[:table]]
      end
    end

    # Quote identifier for raw SQL
    def quote_identifier(db, identifier)
      db.literal(identifier.to_sym)
    end

    # Helper to make hash values JSON serializable (convert sets, symbols)
    def make_json_safe(value)
      case value
      when Hash
        value.transform_values { |v| make_json_safe(v) }
      when Array
        value.map { |v| make_json_safe(v) }
      when Set
        value.map { |v| make_json_safe(v) }
      when Symbol
        value.to_s
      when Time # Convert Time objects to ISO 8601 string
        value.iso8601
      else
        value
      end
    end

    # Helper to truncate long values for display
    def truncate_value(value, max_length = 50)
      str_value = value.to_s
      if str_value.length > max_length
        "#{str_value[0...max_length]}..."
      else
        str_value
      end
    end

    # Helper to check if a config specifies a database name
    def database_name_present?(config)
      case config
      when String # URL string
        begin
          uri = URI.parse(config)
          # Path exists and is longer than just "/"
          !uri.path.nil? && uri.path.length > 1
        rescue URI::InvalidURIError
          false # Invalid URL likely doesn't specify a DB
        end
      when Hash # Hash
        # Check if :database key exists and is not empty
        !config[:database].to_s.empty?
      else
        false # Not a String or Hash
      end
    end

    # Define constants used across modules/classes
    CONFIG_FILE_PATH = File.join(Dir.pwd, 'config', 'database.yml').freeze
    OUTPUT_FORMATS = ['json', 'summary', 'gpt', 'compact'].freeze
    DEFAULT_OUTPUT_FORMAT = 'compact'
    DEFAULT_ENVIRONMENT = 'development'.freeze
    DEFAULT_POOL_SIZE = 5.freeze
    DEFAULT_CONNECT_TIMEOUT = 10.freeze
    SEQUEL_INTERNAL_TABLES = Set['schema_migrations', 'ar_internal_metadata', 'sequel_migrations'].freeze
    METADATA_KEYS = Set[:relation_type, :view_definition, :dependencies, :last_refresh, :is_materialized, :error].freeze

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

      # Add search value findings
      if stats[:found] && stats[:search_value]
        formatted_stats[:found] = true
        formatted_stats[:search_value] = stats[:search_value]
      end

      formatted_stats
    end

    # Generate search summary data
    def generate_search_summary(report_data)
      meta = report_data[:metadata]
      return nil unless meta[:search_value]

      total_found = 0
      found_locations = []

      report_data[:tables].each do |table_name, table_data|
        # Ensure table_data is a hash and contains some column stats
        next unless table_data.is_a?(Hash) && table_data.values.any? { |v| v.is_a?(Hash) && v.key?(:count) }

        # Iterate only over actual column stats, excluding metadata keys
        column_data = table_data.reject { |k, _| METADATA_KEYS.include?(k) }
        column_data.each do |column_name, stats|
          if stats.is_a?(Hash) && stats[:found]
            total_found += 1
            found_locations << "#{table_name}.#{column_name}"
          end
        end
      end

      {
        search_value: meta[:search_value],
        total_found: total_found,
        found_locations: found_locations
      }
    end

    # Make methods available as class methods too
    module_function :database_name_present?, :format_stats_for_summary, :generate_search_summary,
                    :colored_output, :abort_with_error, :print_warning, :print_info, :print_debug,
                    :parse_table_identifier, :create_sequel_identifier, :quote_identifier,
                    :make_json_safe, :truncate_value
  end # Closes module Utils
end # Closes module DbReport
