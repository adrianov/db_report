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
    # Handles connecting to a default DB first if listing is required, then to the target DB.
    # @param list_mode [Boolean] If true, prioritize connecting without a specific DB name.
    # @return [Sequel::Database, nil] The connection object or nil if connection fails.
    def connect(list_mode: false)
      print_info "Attempting to connect to database using Sequel..." # Keep general message

      connection_options = {
        max_connections: pool_size,
        connect_timeout: connect_timeout
      }

      # Separate config for initial connection (maybe without DB name) and target DB
      initial_config = nil
      target_config = config # The config determined by Config class
      target_db_name = nil
      adapter = nil

      # --- Determine Adapter and Target DB Name early --- #
      case target_config
      when Hash
        target_config = target_config.transform_keys(&:to_sym)
        adapter = target_config[:adapter]
        target_db_name = target_config[:database]
      when String # URL
        begin
          uri = URI.parse(target_config)
          adapter = uri.scheme
          target_db_name = uri.path&.sub(%r{^/}, '')
        rescue URI::InvalidURIError
          print_warning 'Could not parse adapter/database from URL.'
        end
      else
        abort_with_error 'Invalid database configuration type provided.'
      end

      # Abort if adapter cannot be determined
      abort_with_error "Could not determine database adapter from config." unless adapter

      # Ensure the necessary adapter gem is available
      check_adapter_availability(adapter)

      # --- Prepare Initial Connection Config (for listing or base connection) --- #
      if list_mode || target_db_name.to_s.empty?
        print_debug "List mode or no target DB name specified. Preparing base connection config."
        # Try to connect without a specific DB name or to a default one
        case adapter.to_sym
        when :postgres, :postgresql
          # Connect to 'postgres' maintenance database if target is missing/empty
          default_db = 'postgres'
          print_debug "PostgreSQL: Attempting connection to '#{default_db}' for listing/base."
          initial_config = build_connection_config(target_config, adapter, default_db)
        when :mysql, :mysql2
          # MySQL can often connect without a database name specified
          print_debug "MySQL: Attempting connection without specific database name for listing/base."
          initial_config = build_connection_config(target_config, adapter, nil) # Explicitly nil DB
        when :sqlite, :sqlite3
          # SQLite requires a file path or :memory:. Cannot list other DBs.
          # If target_db_name is missing, we can't connect meaningfully for listing.
          if target_db_name.to_s.empty?
            abort_with_error "SQLite requires a database file path or ':memory:'. Cannot connect without one."
          else
            # If a target DB *is* specified, use that for the initial connection too.
            initial_config = build_connection_config(target_config, adapter, target_db_name)
          end
        else
          print_warning "Adapter '#{adapter}' might not support connecting without a database name. Attempting anyway..."
          initial_config = build_connection_config(target_config, adapter, nil)
        end
      else
        # If not list mode and a target DB *is* specified, use it for the initial connection.
        print_debug "Target DB '#{target_db_name}' specified and not in list mode. Using target config for initial connection."
        initial_config = build_connection_config(target_config, adapter, target_db_name)
      end

      # --- Attempt Initial Connection --- #
      print_connection_info("initial config", initial_config)
      @db_connection = attempt_sequel_connect(initial_config, connection_options)

      # If initial connection failed, abort
      unless @db_connection
        # Error message handled within attempt_sequel_connect
        return nil # Indicate failure
      end

      print_info "Successfully established base connection using Sequel."

      # --- If in list mode, we are done --- #
      if list_mode
        print_debug "Connection established in list mode. Returning base connection."
        return @db_connection
      end

      # --- If not list mode, ensure we are connected to the correct TARGET database --- #
      # If initial connection already used the target DB, we're good.
      if config_database_name(initial_config) == target_db_name
        print_debug "Initial connection already used target database '#{target_db_name}'."
      else
        # Need to switch or reconnect to the target database if possible and necessary
        print_debug "Base connection established. Now attempting connection to target database '#{target_db_name}'."
        target_full_config = build_connection_config(target_config, adapter, target_db_name)

        # Close the initial connection before establishing the target one
        disconnect_safely(@db_connection)
        @db_connection = nil # Reset connection

        print_connection_info("target config", target_full_config)
        @db_connection = attempt_sequel_connect(target_full_config, connection_options)

        unless @db_connection
          # Error handled by attempt_sequel_connect
          return nil
        end
        print_info "Successfully connected to target database '#{target_db_name}' using Sequel."
      end

      # Final check and return the connection (should be target DB connection now)
      @db_connection&.loggers << Logger.new($stdout) if debug && @db_connection
      @db_connection
    end

    # --- Public Methods --- #

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

    private

    # Helper to safely disconnect a specific connection object
    def disconnect_safely(conn)
      conn&.disconnect
    rescue StandardError => e
      print_warning "Error during intermediate disconnect: #{e.message}"
    end

    # Helper to get database name from config (String or Hash)
    def config_database_name(cfg)
      case cfg
      when Hash then cfg[:database]
      when String
        begin
          URI.parse(cfg).path&.sub(%r{^/}, '')
        rescue URI::InvalidURIError
          nil
        end
      else nil
      end
    end

    # Helper to build a connection config (Hash or String) for Sequel
    # @param base_config [Hash, String] Original config from options/file/env
    # @param adapter [Symbol, String] The database adapter
    # @param db_name [String, nil] The specific database name to use (or nil)
    # @return [Hash, String] A config suitable for Sequel.connect
    def build_connection_config(base_config, adapter, db_name)
      case base_config
      when Hash
        # Create a new hash to avoid modifying the original
        new_config = base_config.dup
        new_config[:adapter] = adapter.to_s # Ensure adapter is string if needed
        if db_name.nil?
          # Remove database key if we want to connect without one (e.g., MySQL listing)
          new_config.delete(:database)
        else
          new_config[:database] = db_name
        end
        new_config
      when String # URL
        begin
          uri = URI.parse(base_config)
          uri.scheme = adapter.to_s # Ensure adapter is correct
          # Set path only if db_name is provided
          uri.path = db_name ? "/#{db_name}" : '' # Empty path for no DB
          uri.to_s
        rescue URI::InvalidURIError => e
          print_warning "Could not build connection URL: #{e.message}"
          base_config # Fallback to original
        end
      else
        base_config # Return as is if not Hash or String
      end
    end

    # Helper to print connection info, masking password in URL
    def print_connection_info(label, conn_config)
      info_str = if conn_config.is_a?(String)
                   conn_config.gsub(/:[^:]*@/, ':*****@')
                 elsif conn_config.is_a?(Hash)
                   cfg = conn_config.dup
                   cfg[:password] = '*****' if cfg.key?(:password)
                   cfg.inspect
                 else
                   conn_config.inspect
                 end
      print_info "Using #{label}: #{info_str}"
    end

    # Internal helper to attempt connection and handle errors
    def attempt_sequel_connect(conn_config, conn_options)
      begin
        connection = if conn_config.is_a?(String)
                       Sequel.connect(conn_config, conn_options)
                     else
                       # Merge options into hash config for Sequel.connect
                       merged_config = conn_config.merge(conn_options)
                       Sequel.connect(merged_config)
                     end
        connection.test_connection
        connection # Return the connection object
      rescue Sequel::DatabaseConnectionError => e
        error_message = "Failed to connect to database: #{e.message}"
        # Use the config passed to *this* attempt for tips
        tips = build_connection_error_tips(e, conn_config)
        abort_with_error "#{error_message}\n  • #{tips}"
        nil # Return nil on failure
      rescue LoadError => e
        abort_with_error("Failed to load database adapter gem: #{e.message}. Ensure the required gem (e.g., 'pg', 'mysql2', 'sqlite3') is installed.")
        nil
      rescue StandardError => e
        error_message = "An unexpected error occurred during connection: #{e.message}"
        tips = build_connection_error_tips(e, conn_config)
        abort_with_error "#{error_message}\n  • #{tips}"
        nil
      end
    end

  end
end
