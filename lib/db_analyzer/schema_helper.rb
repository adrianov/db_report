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

        if user_requested_tables.empty?
          tables_to_analyze = determine_default_tables(tables_in_search_path, all_qualified_tables)
        else
          tables_to_analyze, invalid_tables = resolve_user_requested_tables(
            user_requested_tables,
            qualified_set,
            search_path_set,
            all_qualified_tables
          )
        end

        if invalid_tables.any?
          print_warning "Requested tables not found or could not be resolved: #{invalid_tables.uniq.join(', ')}. Available references: #{available_table_references.to_a.sort.join(', ')}"
        end

        if tables_to_analyze.empty?
          msg = user_requested_tables.empty? ? "No application tables found." : "No valid tables specified or found among available references."
          available_refs_msg = available_table_references.any? ? " Available references: #{available_table_references.to_a.sort.join(', ')}" : ""
          abort_with_error "#{msg}#{available_refs_msg}"
        end

        print_info "
Analyzing #{tables_to_analyze.length} table(s): #{tables_to_analyze.join(', ')}"
        tables_to_analyze
      end

      # Create an alias to avoid name collision when used as an instance method
      alias select_tables_for_analysis select_tables_to_analyze

      def fetch_all_qualified_tables(db)
        # PostgreSQL-specific query to get schema.table names
        # TODO: Add support for other adapters (MySQL, SQLite)
        return [] unless db.database_type == :postgres
        query = <<~SQL
          SELECT n.nspname, c.relname
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relkind IN ('r', 'p') -- Regular tables and partitioned tables
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
          ORDER BY n.nspname, c.relname
        SQL
        db.fetch(query).map { |row| "#{row[:nspname]}.#{row[:relname]}" }.sort
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

      # Export methods as module functions
      module_function :fetch_and_enhance_schema, :fetch_unique_single_columns, :initialize_column_stats,
                      :select_tables_to_analyze, :select_tables_for_analysis, :fetch_all_qualified_tables, :fetch_tables_in_search_path,
                      :determine_default_tables, :resolve_user_requested_tables, :find_matching_table
    end
  end
end
