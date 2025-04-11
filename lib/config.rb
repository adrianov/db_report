# frozen_string_literal: true

require 'yaml'
require 'uri'

# Load Utils relative to this file's directory
require_relative 'utils'

module DbReport
  # Handles determining the database configuration based on options,
  # environment variables, and a configuration file.
  class Config
    include DbReport::Utils # Include Utils for constants and logging

    # @param options [Hash] Parsed command-line options
    def initialize(options)
      @options = options
    end

    # Determines the final database configuration.
    # Priority:
    # 1. --database-url option
    # 2. config/database.yml file (if --environment is explicitly used)
    # 3. DATABASE_URL environment variable
    # 4. config/database.yml file (using default environment if --environment not used)
    #
    # The --database option overrides the database name component
    # of the configuration found via URL or file.
    #
    # @return [String, Hash, nil] Sequel connection string, config hash, or nil if none found.
    def determine
      config = find_config_source

      # If a specific database name is provided via options, override the one from the URL or config file
      # Apply the override and update the config variable
      config = apply_database_override(config)

      config
    end

    private

    # Finds the primary configuration source
    def find_config_source
      if @options[:database_url]
        print_debug "Using database URL from --database-url option."
        return @options[:database_url]
      end

      # If no --database-url, try the config file first using the effective environment
      # (@options[:environment] will hold either the value from -e or the default)
      print_debug "Attempting to use configuration file: #{CONFIG_FILE_PATH} (Environment: #{@options[:environment]})."
      config_from_file = load_from_config_file

      return config_from_file if config_from_file

      # If config file loading failed or returned nil, fall back to ENV['DATABASE_URL']
      if ENV['DATABASE_URL']
        print_debug "Using database URL from DATABASE_URL environment variable."
        return ENV['DATABASE_URL']
      end

      # If all sources failed
      print_warning "No valid configuration found from --database-url, config file (#{CONFIG_FILE_PATH}), or DATABASE_URL environment variable."
      nil
    end

    # Loads configuration from the YAML file.
    def load_from_config_file
      unless File.exist?(CONFIG_FILE_PATH)
        print_warning "Configuration file not found: #{CONFIG_FILE_PATH}"
        return nil
      end

      begin
        # Enable aliases for YAML anchors/references
        full_config = YAML.load_file(CONFIG_FILE_PATH, aliases: true)
        env_block = full_config[@options[:environment]]

        unless env_block
          print_warning "Environment '#{@options[:environment]}' not found in #{CONFIG_FILE_PATH}"
          return nil
        end

        # Expecting a 'primary' key within the environment block
        primary_config = env_block['primary']
        unless primary_config && primary_config.is_a?(Hash)
          print_warning "No 'primary' database configuration found for environment '#{@options[:environment]}' in #{CONFIG_FILE_PATH}"
          return nil
        end

        # If DATABASE_URL is set and the loaded config has both 'url' (from alias) and 'database' (specific),
        # remove the 'url' key to prioritize the specific 'database' name.
        if ENV['DATABASE_URL'] && primary_config['url'] && primary_config['database']
          print_debug "Removing inherited 'url' key to prioritize specific 'database' key from config file."
          primary_config.delete('url')
        end

        # Convert keys to symbols for Sequel
        symbolize_keys(primary_config)
      rescue Psych::SyntaxError => e
        print_warning "Error parsing configuration file #{CONFIG_FILE_PATH}: #{e.message}"
        nil
      rescue StandardError => e
        print_warning "Could not load or process config file #{CONFIG_FILE_PATH}: #{e.message}"
        nil
      end
    end

    # Applies the --database override if present.
    # @param config [String, Hash, nil] The configuration found so far.
    # @return [String, Hash, nil] The potentially modified configuration.
    def apply_database_override(config)
      return config unless @options[:database] && config

      print_debug "Overriding database name with: #{@options[:database]}"

      case config
      when String # Database URL
        begin
          uri = URI.parse(config)
          # Basic path manipulation, might need refinement for complex URLs
          uri.path = "/#{@options[:database]}" # Assumes path is /dbname
          # Return the modified URI string
          return uri.to_s
        rescue URI::InvalidURIError => e
          print_warning "Could not override database in URL '#{config}': #{e.message}"
          # Return original config if override failed
          return config
        end
      when Hash # Config Hash
        config[:database] = @options[:database]
        # Return the modified hash
        return config
      end
      # Return original config if type wasn't String or Hash (shouldn't happen)
      config
    end

    # Helper to recursively symbolize keys in a hash
    def symbolize_keys(hash)
      hash.each_with_object({}) do |(key, value), result|
        new_key = key.is_a?(String) ? key.to_sym : key
        new_value = value.is_a?(Hash) ? symbolize_keys(value) : value
        result[new_key] = new_value
      end
    end
  end
end
