# frozen_string_literal: true

require 'sequel'
require 'set'
require_relative '../utils'

module DbReport
  module DbAnalyzer
    # SchemaHelper module provides functionality for handling database schema and tables
    module SchemaHelper
      include Utils # Include Utils for logging, constants, identifiers
      extend Utils  # Extend Utils to make its methods available as module methods too

      # Fetch and enhance column schema information
      def fetch_and_enhance_schema(db, table_identifier)
        schema = db.schema(table_identifier).to_h { |col_info| [col_info[0], col_info[1]] }
        schema.each do |col_sym, col_info|
          db_type = col_info[:db_type].to_s.downcase
          # Infer type if Sequel couldn't or if it's a generic string for specific db_types
          if col_info[:type].nil? || col_info[:type].to_s.empty? ||
             (col_info[:type] == :string && (db_type.include?('json') || db_type == 'uuid' || db_type == 'tsvector'))
            if db_type.include?('json')
              col_info[:type] = :json
              print_debug("    Inferred type :json for column #{col_sym} based on db_type: #{col_info[:db_type]}") if $debug
            elsif db_type == 'uuid'
              col_info[:type] = :uuid
              print_debug("    Inferred type :uuid for column #{col_sym} based on db_type: #{col_info[:db_type]}") if $debug
            elsif db_type == 'tsvector'
              col_info[:type] = :tsvector
              print_debug("    Inferred type :tsvector for column #{col_sym} based on db_type: #{col_info[:db_type]}") if $debug
            end
          end

          # Check for PostgreSQL enum types
          # PostgreSQL enums are reported as 'user-defined' types
          if db.database_type == :postgres && col_info[:type] == :string && db_type != 'character varying' &&
             db_type != 'varchar' && db_type != 'text' && db_type != 'char' && db_type != 'character' &&
             !db_type.start_with?('varchar(') && !db_type.start_with?('character varying(') &&
             !db_type.start_with?('char(') && !db_type.start_with?('character(')
            # Check if this is likely an enum type in PostgreSQL
            begin
              # Query to check if the type is an enum
              is_enum = db.fetch(<<~SQL, db_type).first
                SELECT EXISTS (
                  SELECT 1 FROM pg_type t
                  JOIN pg_enum e ON t.oid = e.enumtypid
                  WHERE t.typname = ?
                ) AS is_enum
              SQL

              if is_enum && is_enum[:is_enum]
                col_info[:type] = :enum
                print_debug("    Inferred type :enum for column #{col_sym} based on db_type: #{col_info[:db_type]}") if $debug
              end
            rescue Sequel::DatabaseError => e
              # If the query fails, we'll leave the type as it is
              print_debug("    Error checking enum type for column #{col_sym}: #{e.message}") if $debug
            end
          end
        end
        schema
      end

      # Fetch columns with unique indexes
      def fetch_unique_single_columns(db, table_name_string)
        unique_columns = Set.new
        begin
          parsed_ident = parse_table_identifier(table_name_string)
          index_opts = parsed_ident[:schema] ? { schema: parsed_ident[:schema] } : {}
          indexes = db.indexes(parsed_ident[:table], index_opts)

          unique_columns = indexes.select { |_name, idx| idx[:unique] && idx[:columns].length == 1 }
                                  .map { |_name, idx| idx[:columns].first }
                                  .to_set
          print_debug "  Found unique single-column indexes on: #{unique_columns.to_a.join(', ')}" if unique_columns.any? && $debug
        rescue NotImplementedError
          print_debug "  Index fetching not implemented for this adapter."
        rescue Sequel::DatabaseError => e
          print_warning "  Could not fetch index information for #{table_name_string}: #{e.message}"
        rescue StandardError => e
          print_warning "  Unexpected error fetching indexes for #{table_name_string}: #{e.message}"
        end
        unique_columns
      end

      # Initialize the column statistics structure
      def initialize_column_stats(columns_schema, unique_single_columns)
        stats = {}
        columns_schema.each do |column_sym, column_info|
          is_unique = unique_single_columns.include?(column_sym)
          stats[column_sym] = {
            type: column_info[:type].to_s, # Use enhanced type
            db_type: column_info[:db_type],
            count: 0, null_count: 0,
            min: nil, max: nil, avg: nil,
            true_percentage: nil, distinct_count: nil,
            most_frequent: {}, least_frequent: {},
            is_unique: is_unique
          }
        end
        stats
      end
      # Determine which tables to analyze based on options and available tables
      def select_tables_to_analyze(db, options)
        # Include views flag
        include_views = options[:include_views] || false
        include_materialized_views = options[:include_materialized_views] || false
        
        all_qualified_tables = fetch_all_qualified_tables(db)
        tables_in_search_path = fetch_tables_in_search_path(db)

        qualified_set = Set.new(all_qualified_tables)
        search_path_set = Set.new(tables_in_search_path)
        available_table_references = qualified_set + search_path_set

        if available_table_references.empty?
          abort_with_error 'No tables found in the database (checked schemas and search_path).'
        end
        user_requested_tables = options[:tables]
        tables_to_analyze = []
        invalid_tables = []

        # Filter tables based on their type if not explicitly requested by the user
        filtered_tables = all_qualified_tables.map do |qualified_name|
          rel_type = get_relation_type(db, qualified_name)
          { name: qualified_name, type: rel_type }
        end
        
        if user_requested_tables.empty?
          # Apply filtering based on options
          print_debug("Including regular tables in analysis") if $debug
          
          # Filter based on relation type
          if !include_views && !include_materialized_views
            # Only include regular tables
            print_debug("Excluding views and materialized views from analysis") if $debug
            filtered_tables = filtered_tables.select { |t| t[:type] == :table }
          elsif !include_views
            # Include tables and materialized views, but not regular views
            print_debug("Excluding views from analysis") if $debug
            filtered_tables = filtered_tables.select { |t| t[:type] != :view }
          elsif !include_materialized_views
            # Include tables and regular views, but not materialized views
            print_debug("Excluding materialized views from analysis") if $debug
            filtered_tables = filtered_tables.select { |t| t[:type] != :materialized_view }
          else
            # Include all types (tables, views, materialized views)
            print_debug("Including all relation types in analysis: tables, views, and materialized views") if $debug
          end
          
          tables_to_analyze = determine_default_tables(
            tables_in_search_path, 
            filtered_tables.map { |t| t[:name] }
          )
        else
          # User explicitly requested tables - check if they exist and resolve them
          tables_to_analyze, invalid_tables = resolve_user_requested_tables(
            user_requested_tables,
            qualified_set,
            search_path_set,
            all_qualified_tables
          )
          
          # Apply view filtering only if options are specified
          if !include_views || !include_materialized_views
            # Filter user-requested tables based on relation type
            tables_to_analyze = tables_to_analyze.select do |qualified_name|
              rel_type = get_relation_type(db, qualified_name)
              
              case rel_type
              when :view
                if !include_views
                  print_debug("Excluding view from analysis: #{qualified_name}") if $debug
                  false
                else
                  true
                end
              when :materialized_view
                if !include_materialized_views
                  print_debug("Excluding materialized view from analysis: #{qualified_name}") if $debug
                  false
                else
                  true
                end
              else
                true
              end
            end
          end
        end
        
        if invalid_tables.any?
          print_warning "Requested tables not found or could not be resolved: #{invalid_tables.uniq.join(', ')}. Available references: #{available_table_references.to_a.sort.join(', ')}"
        end

        if tables_to_analyze.empty?
          msg = user_requested_tables.empty? ? "No application tables found." : "No valid tables specified or found among available references."
          available_refs_msg = available_table_references.any? ? " Available references: #{available_table_references.to_a.sort.join(', ')}" : ""
          abort_with_error "#{msg}#{available_refs_msg}"
        end
        # Add view type information in debug output
        if $debug
          tables_to_analyze.each do |qualified_name|
            rel_type = get_relation_type(db, qualified_name)
            case rel_type
            when :view
              print_debug("#{qualified_name} is a VIEW")
            when :materialized_view
              print_debug("#{qualified_name} is a MATERIALIZED VIEW")
            when :table
              print_debug("#{qualified_name} is a TABLE")
            else
              print_debug("#{qualified_name} has UNKNOWN type")
            end
          end
        end
        
        print_info "
Analyzing #{tables_to_analyze.length} relation(s): #{tables_to_analyze.join(', ')}"
        tables_to_analyze
      end

      # Create an alias to avoid name collision when used as an instance method
      alias select_tables_for_analysis select_tables_to_analyze

      def fetch_all_qualified_tables(db)
        # PostgreSQL-specific query to get schema.table names
        # TODO: Add support for other adapters (MySQL, SQLite)
        return [] unless db.database_type == :postgres
        query = <<~SQL
          SELECT n.nspname, c.relname, c.relkind
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relkind IN ('r', 'p', 'v', 'm') -- Regular tables, partitioned tables, views, materialized views
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
          ORDER BY n.nspname, c.relname
        SQL
        db.fetch(query).map { |row| { 
          name: "#{row[:nspname]}.#{row[:relname]}", 
          type: case row[:relkind]
                when 'r', 'p' then :table
                when 'v' then :view
                when 'm' then :materialized_view
                end
        } }.sort_by { |obj| obj[:name] }.map { |obj| obj[:name] }
      rescue Sequel::DatabaseError => e
        print_warning("Could not query all schemas for tables: #{e.message}")
        []
      end

      def fetch_tables_in_search_path(db)
        db.tables.map(&:to_s).sort
      rescue Sequel::DatabaseError => e
        print_warning("Could not fetch tables using default search path: #{e.message}")
        []
      end

      def determine_default_tables(tables_in_search_path, all_qualified_tables)
        default_tables = tables_in_search_path.reject { |t| SEQUEL_INTERNAL_TABLES.include?(t) }

        if default_tables.empty?
          all_app_qualified_tables = all_qualified_tables.reject do |t|
            _schema_part, table_part = t.split('.', 2)
            SEQUEL_INTERNAL_TABLES.include?(t) || SEQUEL_INTERNAL_TABLES.include?(table_part)
          end

          if all_app_qualified_tables.any?
             print_warning("No application tables found in the default search path. Using all found qualified tables.")
             return all_app_qualified_tables
          else
             print_warning "No application tables found in the default search path or across schemas. Internal tables: #{SEQUEL_INTERNAL_TABLES.join(', ')}."
             return []
          end
        end
        default_tables
      end

      def resolve_user_requested_tables(requested_tables, qualified_set, search_path_set, all_qualified_tables)
        resolved = []
        invalid = []

        requested_tables.each do |req_table|
          found_table = find_matching_table(req_table, qualified_set, search_path_set, all_qualified_tables)
          if found_table
            resolved << found_table unless resolved.include?(found_table)
          else
            invalid << req_table
          end
        end
        [resolved, invalid]
      end

      def find_matching_table(requested_table, qualified_set, search_path_set, all_qualified_tables)
        if requested_table.include?('.') # Qualified name provided
          return requested_table if qualified_set.include?(requested_table)
        else # Unqualified name provided
          # Priority 1: Direct match in search path
          return requested_table if search_path_set.include?(requested_table)

          # Priority 2: Match in public schema
          public_qualified_name = "public.#{requested_table}"
          return public_qualified_name if qualified_set.include?(public_qualified_name)

          # Priority 3: Unique match in other schemas
          possible_matches = all_qualified_tables.select { |q| !q.start_with?('public.') && q.split('.', 2).last == requested_table }
          if possible_matches.length == 1
            return possible_matches.first
          elsif possible_matches.length > 1
            print_warning "Ambiguous requested table '#{requested_table}'. Matches found in non-public schemas: #{possible_matches.join(', ')}. Skipping."
          end
        end
        nil # Not found or ambiguous
      end
      # Determine the type of database relation (table, view, materialized view) from its qualified name
      def get_relation_type(db, qualified_name)
        return :unknown unless db.database_type == :postgres

        schema_name, rel_name = qualified_name.split(".", 2)
        
        query = <<~SQL
          SELECT c.relkind
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = ? AND c.relname = ?
        SQL
        
        result = db.fetch(query, schema_name, rel_name).first
        
        if result
          case result[:relkind]
          when 'r', 'p' then :table
          when 'v' then :view
          when 'm' then :materialized_view
          else :unknown
          end
        else
          # Try unqualified name in search path
          query = <<~SQL
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = ?
              AND n.nspname IN (SELECT unnest(current_schemas(false)))
            LIMIT 1
          SQL
          
          result = db.fetch(query, qualified_name).first
          
          if result
            case result[:relkind]
            when 'r', 'p' then :table
            when 'v' then :view
            when 'm' then :materialized_view
            else :unknown
            end
          else
            :unknown
          end
        end
      rescue Sequel::DatabaseError => e
        print_warning("Error determining relation type for #{qualified_name}: #{e.message}")
        :unknown
      end

      # Fetch view definition (SQL query) for a view or materialized view
      def fetch_view_definition(db, qualified_name)
        return nil unless db.database_type == :postgres
        
        schema_name, view_name = qualified_name.include?('.') ? qualified_name.split('.', 2) : [nil, qualified_name]
        
        query = if schema_name
          <<~SQL
            SELECT pg_get_viewdef(
              (SELECT c.oid FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = ? AND c.relname = ?
               AND c.relkind IN ('v', 'm')),
              true
            ) AS definition
          SQL
        else
          <<~SQL
            SELECT pg_get_viewdef(
              (SELECT c.oid FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE c.relname = ?
               AND n.nspname IN (SELECT unnest(current_schemas(false)))
               AND c.relkind IN ('v', 'm')
               LIMIT 1),
              true
            ) AS definition
          SQL
        end
        
        result = schema_name ? db.fetch(query, schema_name, view_name).first : db.fetch(query, view_name).first
        result ? result[:definition] : nil
      rescue Sequel::DatabaseError => e
        print_warning("Error fetching view definition for #{qualified_name}: #{e.message}")
        nil
      end
      
      # Fetch dependencies for a view (tables and other views it references)
      def fetch_view_dependencies(db, qualified_name)
        return [] unless db.database_type == :postgres
        
        schema_name, view_name = qualified_name.include?('.') ? qualified_name.split('.', 2) : [nil, view_name]
        
        query = if schema_name
          <<~SQL
            WITH RECURSIVE view_deps AS (
              SELECT DISTINCT d.refobjid::regclass::text AS dependency
              FROM pg_depend d
              JOIN pg_rewrite r ON r.oid = d.objid
              JOIN pg_class c ON c.oid = r.ev_class
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = ? AND c.relname = ?
                AND d.refobjid != c.oid  -- Exclude self-references
            )
            SELECT dependency FROM view_deps
            ORDER BY dependency
          SQL
        else
          <<~SQL
            WITH RECURSIVE view_deps AS (
              SELECT DISTINCT d.refobjid::regclass::text AS dependency
              FROM pg_depend d
              JOIN pg_rewrite r ON r.oid = d.objid
              JOIN pg_class c ON c.oid = r.ev_class
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relname = ?
                AND n.nspname IN (SELECT unnest(current_schemas(false)))
                AND d.refobjid != c.oid  -- Exclude self-references
              LIMIT 1
            )
            SELECT dependency FROM view_deps
            ORDER BY dependency
          SQL
        end
        
        result = schema_name ? db.fetch(query, schema_name, view_name).all : db.fetch(query, view_name).all
        result.map { |row| row[:dependency] }
      rescue Sequel::DatabaseError => e
        print_warning("Error fetching view dependencies for #{qualified_name}: #{e.message}")
        []
      end
      
      # Fetch materialized view specific information like refresh time
      def fetch_materialized_view_info(db, qualified_name)
        return nil unless db.database_type == :postgres
        
        schema_name, mv_name = qualified_name.include?('.') ? qualified_name.split('.', 2) : [nil, qualified_name]
        
        query = if schema_name
          <<~SQL
            SELECT 
              c.relname,
              CASE WHEN c.relkind = 'm' THEN true ELSE false END AS is_materialized,
              s.last_refresh
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN (
              SELECT schemaname, matviewname, 
                     pg_catalog.pg_stat_get_last_analyze_time(c.oid) as last_refresh
              FROM pg_matviews m
              JOIN pg_class c ON c.relname = m.matviewname
              JOIN pg_namespace n ON n.nspname = m.schemaname AND c.relnamespace = n.oid
            ) s ON s.schemaname = n.nspname AND s.matviewname = c.relname
            WHERE n.nspname = ? AND c.relname = ?
              AND c.relkind = 'm'
          SQL
        else
          <<~SQL
            SELECT 
              c.relname,
              CASE WHEN c.relkind = 'm' THEN true ELSE false END AS is_materialized,
              s.last_refresh
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN (
              SELECT schemaname, matviewname, 
                     pg_catalog.pg_stat_get_last_analyze_time(c.oid) as last_refresh
              FROM pg_matviews m
              JOIN pg_class c ON c.relname = m.matviewname
              JOIN pg_namespace n ON n.nspname = m.schemaname AND c.relnamespace = n.oid
            ) s ON s.schemaname = n.nspname AND s.matviewname = c.relname
            WHERE c.relname = ?
              AND n.nspname IN (SELECT unnest(current_schemas(false)))
              AND c.relkind = 'm'
            LIMIT 1
          SQL
        end
        
        result = schema_name ? db.fetch(query, schema_name, mv_name).first : db.fetch(query, mv_name).first
        
        if result
          {
            is_materialized: result[:is_materialized],
            last_refresh: result[:last_refresh] ? Time.at(result[:last_refresh]) : nil
          }
        else
          nil
        end
      rescue Sequel::DatabaseError => e
        print_warning("Error fetching materialized view info for #{qualified_name}: #{e.message}")
        nil
      end

      # Export methods as module functions
      module_function :fetch_and_enhance_schema, :fetch_unique_single_columns, :initialize_column_stats,
                      :select_tables_to_analyze, :select_tables_for_analysis, :fetch_all_qualified_tables, :fetch_tables_in_search_path,
                      :determine_default_tables, :resolve_user_requested_tables, :find_matching_table, 
                      :get_relation_type, :fetch_view_definition, :fetch_view_dependencies, :fetch_materialized_view_info
    end
  end
end
