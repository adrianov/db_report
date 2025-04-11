# frozen_string_literal: true

require 'sequel'
require 'logger'
require 'uri'
require_relative 'utils' # Need utils for printing, constants

module DbReport
  class Connector
    include Utils # Include Utils for logging and constants

    attr_reader :db_connection, :config, :pool_size, :connect_timeout, :debug

    def initialize(db_config, pool_size, connect_timeout, debug)
      @config = db_config
      @pool_size = pool_size || DEFAULT_POOL_SIZE
      @connect_timeout = connect_timeout || DEFAULT_CONNECT_TIMEOUT
      @debug = debug
      @db_connection = nil
    end

    # Get the required gem name for a given adapter
    def adapter_gem_name(adapter)
      case adapter.to_s
      when /mysql/ then 'mysql2'
      when /postgres/ then 'pg'
      when /sqlite/ then 'sqlite3'
      else adapter # For adapters like jdbc, etc.
      end
    end

    # Check if the required adapter gem is loaded
    def check_adapter_availability(adapter_name)
      gem_name = adapter_gem_name(adapter_name)
      return unless gem_name

      begin
        require gem_name
        print_info "Loaded #{gem_name} adapter"
      rescue LoadError => e
        abort_with_error("Couldn't load database adapter gem '#{gem_name}' for Sequel. Please add `gem '#{gem_name}'` to your Gemfile or install it.\nError: #{e.message}")
      end
    end

    # Build helpful error messages for connection failures
    def build_connection_error_tips(error, config)
      username = 'unknown'
      adapter = 'unknown'
      database = 'unknown'
      host = 'unknown'

      if config.is_a?(String) # URL
        begin
          uri = URI.parse(config)
          username = uri.user || 'unknown'
          adapter = uri.scheme == 'postgres' ? 'postgresql' : uri.scheme
          database = uri.path&.sub(%r{^/}, '') || 'unknown'
          host = uri.host || 'unknown'
        rescue URI::InvalidURIError # rubocop:disable Lint/SuppressedException
        end
      elsif config.is_a?(Hash)
        username = config[:user] || 'unknown'
        adapter = config[:adapter] || 'unknown'
        database = config[:database] || 'unknown'
        host = config[:host] || 'unknown'
      end

      tips = case error.message
             when /could not connect to server/, /Connection refused/
               "Make sure the database server is running and accessible at #{host}."
             when /database .* does not exist/
               "Database '#{database}' doesn't exist. Create it or check the name."
             when /password authentication failed/, /Access denied for user/, /authentication failed/
               "Authentication failed for user '#{username}'. Check user/password in config."
             when /role .* does not exist/, /Unknown user/
               "Database user '#{username}' doesn't exist. Create it or check the username."
             when /No such file or directory/ # SQLite
               "Database file not found (for SQLite at path: #{database})."
             when /timeout|timed out/
               "Connection timed out. Check network or increase --timeout."
             else
               'Check your database configuration, network access, and ensure the server is running.'
             end

      config_tips = if config.is_a?(String)
                      'URL format: adapter://user:password@host:port/database'
                    elsif config.is_a?(Hash)
                      required = [:adapter]
                      required << :database unless adapter == 'sqlite' && database == ':memory:'
                      required << :host unless adapter == 'sqlite'
                      missing = required.select { |k| config[k].to_s.empty? }
                      missing.any? ? "Config missing required keys: #{missing.join(', ')}" : nil
                    end

      [tips, config_tips].compact.join("
  • ")
    end

    # Establish the database connection using Sequel
    def connect
      print_info 'Attempting to connect to database using Sequel...'

      connection_config = config # Can be String (URL) or Hash
      connection_options = {
        max_connections: pool_size,
        connect_timeout: connect_timeout
      }

      if connection_config.is_a?(Hash)
        # Ensure hash config keys are symbols for Sequel
        connection_config = connection_config.transform_keys(&:to_sym)
        adapter = connection_config[:adapter]
        print_info "Using config: adapter=#{adapter || '?'}, database=#{connection_config[:database] || '?'}, host=#{connection_config[:host] || 'localhost'}, user=#{connection_config[:user] || '?'}"
        print_info "Connection pool size: #{pool_size}"
        check_adapter_availability(adapter) if adapter
        # Merge options into the config hash for Sequel.connect
        connection_config.merge!(connection_options)
      elsif connection_config.is_a?(String) # URL
        masked_url = connection_config.gsub(/:[^:]*@/, ':*****@')
        print_info "Using connection string: #{masked_url}"
        begin
          adapter = URI.parse(connection_config).scheme
          check_adapter_availability(adapter) if adapter
        rescue URI::InvalidURIError
          print_warning 'Could not parse adapter from URL to check gem.'
        end
        # Pass options separately when connecting with a URL string
      else
        abort_with_error 'Invalid database configuration provided.'
      end

      begin
        @db_connection = if connection_config.is_a?(String)
                          Sequel.connect(connection_config, connection_options)
                        else
                          Sequel.connect(connection_config) # Options are already merged
                        end

        @db_connection.loggers << Logger.new($stdout) if debug
        @db_connection.test_connection
        print_info 'Successfully connected to database using Sequel'

      rescue Sequel::DatabaseConnectionError => e
        error_message = "Failed to connect to database: #{e.message}"
        tips = build_connection_error_tips(e, config)
        abort_with_error "#{error_message}\n  • #{tips}"
      rescue LoadError => e
         abort_with_error("Failed to load database adapter gem: #{e.message}. Ensure the required gem (e.g., 'pg', 'mysql2', 'sqlite3') is installed.")
      rescue StandardError => e
        error_message = "An unexpected error occurred during connection: #{e.message}"
        tips = build_connection_error_tips(e, config)
        abort_with_error "#{error_message}\n  • #{tips}"
      end

      @db_connection # Return the connection object
    end

    # Disconnect the database connection
    def disconnect
      db_connection&.disconnect
      print_debug "Database connection closed." if debug
    rescue StandardError => e
      print_warning "Error disconnecting from database: #{e.message}"
    end

    # Display details about the current database connection
    def display_connection_info
      return unless debug && db_connection

      begin
        opts = db_connection.opts
        adapter = db_connection.adapter_scheme.to_s
        database = db_connection.database rescue opts[:database] || 'unknown'
        version = begin
                    db_connection.database_type == :sqlite ? db_connection.get(Sequel.lit('sqlite_version()')) : db_connection.get(Sequel.function(:version))
                  rescue StandardError => e
                    print_debug "Could not get DB version: #{e.message}"
                    'unknown'
                  end
        host = opts[:host] || 'localhost'
        port = opts[:port] || 'default'
        user = opts[:user] || '(default)'

        puts colored_output("
--- Database Connection Details (Sequel) ---", :yellow)
        puts "  Adapter:       #{adapter}"
        puts "  Database:      #{database}"
        puts "  Version:       #{version}"
        puts "  Host:          #{host}"
        puts "  Port:          #{port}"
        puts "  User:          #{user}"
        puts "  Pool Size:     #{opts[:max_connections]}"
        puts "------------------------------------------"
      rescue StandardError => e
        print_warning "Could not display full connection details: #{e.message}"
      end
    end

    # Print basic connected database info
    def print_database_info
      return unless db_connection
      begin
        adapter = db_connection.adapter_scheme.to_s
        database = begin
                     db_connection.database
                   rescue StandardError
                     db_connection.opts[:database] || 'unknown'
                   end
        print_info "Connected to #{adapter} database: #{database}", :cyan
      rescue StandardError => e
        print_warning "Connected to database (could not determine full details: #{e.message})"
      end
    end

    # List available databases (adapter-specific)
    def list_available_databases
      return [] unless db_connection
      begin
        query = case db_connection.database_type
                when :postgres
                  'SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname'
                when :mysql
                  'SHOW DATABASES'
                when :sqlite
                  print_warning "SQLite doesn't support listing databases via query."
                  return []
                else
                  print_warning "Unsupported adapter for listing databases: #{db_connection.database_type}"
                  return []
                end
        db_connection.fetch(query).map(&:values).flatten
      rescue Sequel::DatabaseError => e
        print_warning "Error listing databases: #{e.message}"
        []
      rescue StandardError => e
        print_warning "Unexpected error listing databases: #{e.message}"
        []
      end
    end

  end
end
