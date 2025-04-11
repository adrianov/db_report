# frozen_string_literal: true

require 'time'
require 'uri'
require 'bigdecimal'
require 'sequel' # For Sequel::SQL::Blob

# Optional dependencies
begin
  require 'colorize'
  HAS_COLORIZE = true
rescue LoadError
  HAS_COLORIZE = false
end

module DbReport
  module Utils
    # Helper method for colored console output
    def colored_output(text, color = :default, mode = nil)
      # Only colorize if the gem is available AND output is a TTY
      return text unless HAS_COLORIZE && $stdout.tty?

      text = text.colorize(color)
      text = text.send(mode) if mode
      text
    end

    # Abort script with a colored error message
    def abort_with_error(message, error_code = 1)
      abort colored_output("ERROR: #{message}", :red, :bold)
    end

    # Print a colored warning message
    def print_warning(message)
      puts colored_output("Warning: #{message}", :yellow)
    end

    # Print a colored info message
    def print_info(message, color = :green, mode = nil)
      puts colored_output(message, color, mode)
    end

    # Print debug information if enabled
    def print_debug(message)
      # Assume $debug is available in the including context or passed
      puts colored_output(message, :cyan) if $debug
    end

    # Helper to parse table name into schema and table parts
    def parse_table_identifier(table_name_string)
      parts = table_name_string.to_s.split('.', 2)
      if parts.length == 2
        { schema: parts[0].to_sym, table: parts[1].to_sym }
      else
        # Default to nil schema if not qualified, table name as symbol
        { schema: nil, table: parts[0].to_sym }
      end
    end

    # Helper to create a Sequel identifier (handles qualified names)
    def create_sequel_identifier(table_name_string)
      parsed = parse_table_identifier(table_name_string)
      if parsed[:schema]
        Sequel.qualify(parsed[:schema], parsed[:table])
      else
        Sequel.identifier(parsed[:table])
      end
    end

    # Safely quote an identifier (less needed with Sequel datasets, but useful for raw SQL)
    def quote_identifier(identifier, db_connection)
      # Use Sequel's identifier quoting via the connection
      db_connection.literal(Sequel.identifier(identifier))
    end

    # Recursive helper to prepare data for JSON generation (handles non-serializable types)
    def make_json_safe(obj)
      case obj
      when Hash then obj.transform_keys(&:to_s).transform_values { |v| make_json_safe(v) }
      when Array then obj.map { |v| make_json_safe(v) }
      when Time, Date then obj.iso8601 rescue obj.to_s # Use ISO 8601 for consistency
      when Float then obj.nan? || obj.infinite? ? obj.to_s : obj # Handle NaN/Infinity
      when BigDecimal then obj.to_s('F') # Standard notation for BigDecimal
      when Sequel::SQL::Blob then '<Binary Data>' # Represent blobs safely
      else obj
      end
    end

    # Truncate long values for display
    def truncate_value(value, max_length = 80)
      str = value.to_s
      # Handle multi-line strings by taking the first line
      first_line = str.split("
").first || ''
      first_line.length > max_length ? "#{first_line[0...(max_length - 3)]}..." : first_line
    end

    # Define constants used across modules/classes
    DEFAULT_ENVIRONMENT = ENV.fetch('RAILS_ENV', 'development')
    DEFAULT_POOL_SIZE = ENV.fetch('DB_POOL', 5).to_i # Sequel uses :max_connections
    DEFAULT_CONNECT_TIMEOUT = 10 # Sequel uses :connect_timeout
    DEFAULT_OUTPUT_FORMAT = 'json'
    CONFIG_FILE_PATH = File.join(Dir.pwd, 'config', 'database.yml').freeze
    OUTPUT_FORMATS = %w[json summary gpt compact].freeze
    SEQUEL_INTERNAL_TABLES = [
      'schema_info',                 # Default table name for Sequel migrations (optional)
      'sequel_migrations'            # Common alternative name
    ].freeze

    # Helper to check if a config specifies a database name
    # @param config [String, Hash, nil] The configuration object
    # @return [Boolean] True if a database name is present, false otherwise
    def database_name_present?(config)
      case config
      when String # URL
        begin
          uri = URI.parse(config)
          # Check if path is present and not just "/"
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

    # Make it available as a class method too if needed elsewhere statically
    module_function :database_name_present?
  end
end
