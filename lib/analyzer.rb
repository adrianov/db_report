# frozen_string_literal: true

require 'sequel'
require 'set'
require_relative 'utils'

module DbReport
  class Analyzer
    include Utils # Include Utils for logging, constants, identifiers

    attr_reader :db, :options, :debug

    def initialize(db_connection, options)
      @db = db_connection
      @options = options
      @debug = options[:debug] # Ensure debug flag is accessible
      # Make $debug accessible globally for Utils module print_debug
      # This is a compromise; ideally, debug state would be passed explicitly.
      $debug = @debug
    end

    # --- Table Selection Logic ---

    # Determine which tables to analyze based on options and available tables
    def select_tables_to_analyze
      all_qualified_tables = fetch_all_qualified_tables
      tables_in_search_path = fetch_tables_in_search_path

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

    private

    def fetch_all_qualified_tables
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

    def fetch_tables_in_search_path
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

    # --- Table Analysis --- #

    public # Switch back to public methods

    # Analyze a single table
    def analyze_table(table_name_string)
      table_identifier = create_sequel_identifier(table_name_string)
      print_info "Analyzing table: #{table_name_string}", :cyan, :bold
      adapter_type = db.database_type
      table_stats = {}

      begin
        columns_schema = fetch_and_enhance_schema(table_identifier)
        return { error: "Could not fetch schema for #{table_name_string}" } if columns_schema.empty?

        base_dataset = db[table_identifier]
        unique_single_columns = fetch_unique_single_columns(table_name_string)

        # --- Aggregation --- #
        all_select_parts = [Sequel.function(:COUNT, Sequel.lit('*')).as(:_total_count)]
        initial_col_stats = initialize_column_stats(columns_schema, unique_single_columns)
        columns_schema.each do |col_sym, col_info|
          col_select_parts = build_aggregate_select_parts(col_info[:type], col_sym, adapter_type, initial_col_stats[col_sym][:is_unique])
          all_select_parts.concat(col_select_parts)
        end

        all_agg_results = execute_aggregate_query(base_dataset, all_select_parts)
        total_count = all_agg_results ? all_agg_results[:_total_count].to_i : 0

        # --- Populate Stats & Frequency --- #
        columns_schema.each do |column_sym, column_info|
          col_stats = initial_col_stats[column_sym]
          print_info "  - Analyzing column: #{column_sym} (#{col_stats[:type]}/#{col_stats[:db_type]})#{col_stats[:is_unique] ? ' (unique)' : ''}", :white

          col_stats[:count] = total_count # Set total count for this column
          populate_stats_from_aggregates(col_stats, all_agg_results, column_info[:type], column_sym, total_count) if all_agg_results

          analyze_frequency(col_stats, column_sym, base_dataset, column_info[:type], unique_single_columns)

          col_stats.delete_if { |_key, value| value.nil? || value == {} } # Clean up empty stats
          table_stats[column_sym.to_s] = col_stats # Store final stats with string key
        end

      rescue Sequel::DatabaseError => e # Catch errors like table not found
        msg = "Schema/Table Error for '#{table_name_string}': #{e.message.lines.first.strip}"
        puts colored_output(msg, :red)
        return { error: msg }
      rescue StandardError => e
        msg = "Unexpected error analyzing table '#{table_name_string}': #{e.message}"
        puts colored_output(msg, :red)
        puts e.backtrace.join("
") if debug
        return { error: msg }
      end

      table_stats
    end

    private # Analysis helpers

    def fetch_and_enhance_schema(table_identifier)
      schema = db.schema(table_identifier).to_h { |col_info| [col_info[0], col_info[1]] }
      schema.each do |col_sym, col_info|
        db_type = col_info[:db_type].to_s.downcase
        # Infer type if Sequel couldn't or if it's a generic string for specific db_types
        if col_info[:type].nil? || col_info[:type].to_s.empty? || (col_info[:type] == :string && (db_type.include?('json') || db_type == 'uuid'))
          if db_type.include?('json')
            col_info[:type] = :json
            print_debug("    Inferred type :json for column #{col_sym} based on db_type: #{col_info[:db_type]}") if debug
          elsif db_type == 'uuid'
            col_info[:type] = :uuid
            print_debug("    Inferred type :uuid for column #{col_sym} based on db_type: #{col_info[:db_type]}") if debug
          end
        end
      end
      schema
    end

    def fetch_unique_single_columns(table_name_string)
      unique_columns = Set.new
      begin
        parsed_ident = parse_table_identifier(table_name_string)
        index_opts = parsed_ident[:schema] ? { schema: parsed_ident[:schema] } : {}
        indexes = db.indexes(parsed_ident[:table], index_opts)

        unique_columns = indexes.select { |_name, idx| idx[:unique] && idx[:columns].length == 1 }
                                  .map { |_name, idx| idx[:columns].first }
                                  .to_set
        print_debug "  Found unique single-column indexes on: #{unique_columns.to_a.join(', ')}" if unique_columns.any?
      rescue NotImplementedError
        print_debug "  Index fetching not implemented for this adapter."
      rescue Sequel::DatabaseError => e
        print_warning "  Could not fetch index information for #{table_name_string}: #{e.message}"
      rescue StandardError => e
        print_warning "  Unexpected error fetching indexes for #{table_name_string}: #{e.message}"
      end
      unique_columns
    end

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

    # Build SELECT clause parts for aggregate query based on column type
    def build_aggregate_select_parts(column_type, column_sym, adapter_type, is_unique)
      non_null_alias = :"non_null_count_#{column_sym}"
      min_alias = :"min_val_#{column_sym}"
      max_alias = :"max_val_#{column_sym}"
      avg_alias = :"avg_val_#{column_sym}"
      true_count_alias = :"true_count_#{column_sym}"
      distinct_count_alias = :"distinct_count_#{column_sym}"

      parts = [Sequel.function(:COUNT, column_sym).as(non_null_alias)]
      # Simple heuristic for PK/FK identification
      is_likely_key = is_unique || column_sym == :id || column_sym.to_s.end_with?('_id')
      groupable = !%i[text blob xml array hstore].include?(column_type)

      # MIN/MAX (handle type specifics)
      min_max_added = false
      case column_type
      when :json, :jsonb
        cast_expr = Sequel.cast(column_sym, :text)
        parts += [Sequel.function(:MIN, cast_expr).as(min_alias), Sequel.function(:MAX, cast_expr).as(max_alias)]
        min_max_added = true
      when :uuid
        if adapter_type == :postgres
          cast_expr = Sequel.cast(column_sym, :text)
          parts += [Sequel.function(:MIN, cast_expr).as(min_alias), Sequel.function(:MAX, cast_expr).as(max_alias)]
          min_max_added = true
        else
          print_warning "MIN/MAX on UUID might not be supported for #{adapter_type}" if debug
        end
      when :boolean
        # Cast to int for MIN/MAX compatibility across DBs
        int_cast_expr = Sequel.cast(column_sym, :integer)
        parts += [Sequel.function(:MIN, int_cast_expr).as(min_alias), Sequel.function(:MAX, int_cast_expr).as(max_alias)]
        min_max_added = true
      when :array
        if adapter_type == :postgres
          parts += [Sequel.function(:MIN, Sequel.function(:array_length, column_sym, 1)).as(min_alias),
                    Sequel.function(:MAX, Sequel.function(:array_length, column_sym, 1)).as(max_alias)]
          min_max_added = true
        end
      end
      # Default MIN/MAX for other types if not handled above
      unless min_max_added || !groupable # Don't add MIN/MAX for non-groupable types like text/blob unless handled above
        parts += [Sequel.function(:MIN, column_sym).as(min_alias), Sequel.function(:MAX, column_sym).as(max_alias)]
      end

      # AVG, Distinct Count, True Count
      case column_type
      when :integer, :float, :decimal # Numeric
        unless is_likely_key
          cast_type = adapter_type == :mysql ? :double : :"double precision"
          parts << Sequel.function(:AVG, Sequel.cast(column_sym, cast_type)).as(avg_alias)
          parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, column_sym)).as(distinct_count_alias)
        end
      when :string, :text, :blob, :enum, :inet, :uuid, :json, :jsonb # String-like/complex
        # Average Length (cast non-strings to text)
        unless is_unique || [:text, :blob].include?(column_type) # Skip avg length for text/blob/unique
          length_expr = case column_type
                        when :json, :jsonb, :uuid, :enum, :inet then Sequel.function(:length, Sequel.cast(column_sym, :text))
                        else Sequel.function(:length, column_sym)
                        end
          parts << Sequel.function(:AVG, length_expr).as(avg_alias)
        end
        # Distinct Count (if groupable and not likely key)
        if groupable && !is_likely_key
           distinct_expr = [:json, :jsonb].include?(column_type) ? Sequel.cast(column_sym, :text) : column_sym
           parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, distinct_expr)).as(distinct_count_alias)
        end

      when :boolean
        parts << Sequel.function(:SUM, Sequel.case({ column_sym => 1 }, 0)).as(true_count_alias)
        unless is_likely_key
          parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, column_sym)).as(distinct_count_alias)
        end
      when :array # PG Array
        if !is_unique && adapter_type == :postgres
          parts << Sequel.function(:AVG, Sequel.function(:array_length, column_sym, 1)).as(avg_alias)
        end
        # Distinct count for arrays is tricky, skip for now
      when :date, :datetime, :time, :timestamp
        unless is_likely_key
          parts << Sequel.function(:COUNT, Sequel.function(:DISTINCT, column_sym)).as(distinct_count_alias)
        end
      end

      parts
    end

    # Populate column stats from aggregate query results
    def populate_stats_from_aggregates(col_stats, agg_results, column_type, column_sym, total_count)
      return unless agg_results # Guard against nil results

      non_null_key = :"non_null_count_#{column_sym}"
      min_key = :"min_val_#{column_sym}"
      max_key = :"max_val_#{column_sym}"
      avg_key = :"avg_val_#{column_sym}"
      true_count_key = :"true_count_#{column_sym}"
      distinct_count_key = :"distinct_count_#{column_sym}"

      non_null_count = agg_results[non_null_key].to_i
      col_stats[:null_count] = total_count - non_null_count
      col_stats[:min] = agg_results[min_key]
      col_stats[:max] = agg_results[max_key]
      col_stats[:distinct_count] = agg_results[distinct_count_key].to_i if agg_results.key?(distinct_count_key)

      if column_type == :boolean && non_null_count > 0
        true_count = agg_results[true_count_key].to_i
        col_stats[:true_percentage] = (true_count.to_f / non_null_count) * 100
      elsif agg_results[avg_key]
        col_stats[:avg] = agg_results[avg_key].to_f rescue nil
      end

      # Format date/time (Sequel often returns objects)
      if %i[date datetime time timestamp].include?(column_type)
        col_stats[:min] = col_stats[:min].iso8601 rescue col_stats[:min].to_s if col_stats[:min].respond_to?(:iso8601)
        col_stats[:max] = col_stats[:max].iso8601 rescue col_stats[:max].to_s if col_stats[:max].respond_to?(:iso8601)
      end
    end

    # Fetch and store most and least frequent values
    def analyze_frequency(col_stats, column_sym, base_dataset, column_type, unique_single_columns)
      is_groupable = !%i[text blob xml array hstore json jsonb].include?(column_type)
      is_likely_key = col_stats[:is_unique] || unique_single_columns.include?(column_sym) || column_sym == :id || column_sym.to_s.end_with?('_id')
      is_json_type = column_type == :json || column_type == :jsonb

      # Skip if not groupable (unless JSON) or is a likely key (unless JSON)
      unless is_groupable || is_json_type
        print_debug("    Skipping frequency analysis for non-groupable type: #{column_sym} (#{column_type})") if debug
        return
      end
      if is_likely_key && !is_json_type
        print_debug("    Skipping frequency analysis for PK/FK or unique column: #{column_sym}") if debug
        return
      end
      return unless col_stats[:count] > 0 # Skip if table is empty

      begin
        if is_json_type
          # Analyze JSON frequency by casting to text
          analyze_json_frequency(col_stats, column_sym, base_dataset)
        else
          # Analyze frequency for standard groupable types
          analyze_standard_frequency(col_stats, column_sym, base_dataset)
        end
      rescue Sequel::DatabaseError => e
        puts colored_output("  SQL Frequency Error for column #{column_sym}: #{e.message.lines.first.strip}", :red)
      rescue StandardError => e
        puts colored_output("  Error during frequency analysis for column #{column_sym}: #{e.message}", :red)
        puts e.backtrace.join("
") if debug
      end
    end

    def analyze_json_frequency(col_stats, column_sym, base_dataset)
      print_info "    Analyzing JSON column frequency..." if debug
      casted_column = Sequel.cast(column_sym, :text)
      json_dataset = base_dataset.select(casted_column.as(column_sym), Sequel.function(:COUNT, Sequel.lit('*')).as(:count))
                             .group(casted_column)
                             .order(Sequel.desc(:count))
                             .limit(5)

      print_debug("    JSON frequency SQL: #{json_dataset.sql}") if debug
      most_freq_results = json_dataset.all

      col_stats[:most_frequent] = most_freq_results.to_h { |row| [row[column_sym].to_s, row[:count].to_i] }
      print_debug "    Found #{col_stats[:most_frequent].size} frequent JSON patterns" if debug
      # Skip least frequent for JSON for simplicity
      col_stats.delete(:least_frequent) # Remove placeholder if it exists
    end

    def analyze_standard_frequency(col_stats, column_sym, base_dataset)
      # Most Frequent
      most_freq_order = [Sequel.desc(:count), column_sym]
      most_freq_results = execute_frequency_query(base_dataset, column_sym, most_freq_order, 5)
      col_stats[:most_frequent] = format_frequency_results(most_freq_results, column_sym)

      # Least Frequent (only if worthwhile)
      distinct_count = col_stats[:distinct_count] || 0
      if distinct_count > 5
        least_freq_order = [Sequel.asc(:count), column_sym]
        least_freq_results = execute_frequency_query(base_dataset, column_sym, least_freq_order, 5)
        col_stats[:least_frequent] = format_frequency_results(least_freq_results, column_sym)
      elsif distinct_count > 0 && distinct_count <= 5 && col_stats[:most_frequent].empty?
        # If few distinct values and most_frequent wasn't useful, get all values
        least_freq_order = [Sequel.asc(:count), column_sym]
        all_freq_results = execute_frequency_query(base_dataset, column_sym, least_freq_order, nil) # No limit
        col_stats[:most_frequent] = format_frequency_results(all_freq_results, column_sym)
        col_stats.delete(:least_frequent) # Remove placeholder
      else
        col_stats.delete(:least_frequent) # Remove placeholder if not calculated
      end
    end

    def format_frequency_results(results, column_sym)
      results.to_h do |row|
        key = row[column_sym].nil? ? "NULL" : row[column_sym].to_s
        [key, row[:count].to_i]
      end
    end

    # Helper to safely run aggregate queries
    def execute_aggregate_query(dataset, select_expressions)
      sql_query = dataset.select(*select_expressions).sql
      print_debug("  Executing Aggregates: #{sql_query}") if debug
      start_time = Time.now
      result = nil
      begin
        result = dataset.select(*select_expressions).first
      rescue Sequel::DatabaseError => e
        puts colored_output("  SQL Aggregate Error: #{e.message.lines.first.strip}", :red)
        print_debug "  Failed SQL: #{sql_query}" if debug
        return nil # Return nil on SQL error
      rescue StandardError => e
        puts colored_output("  Error (Aggregate Query): #{e.message.lines.first.strip}", :red)
        return nil
      ensure
        duration = (Time.now - start_time).round(4)
        print_debug("  Aggregates Duration: #{duration}s") if debug
      end
      result
    end

    # Helper to safely run frequency queries
    def execute_frequency_query(base_dataset, column_sym, order_expressions, limit_count)
      print_debug("    [Debug Frequency] Column: #{column_sym}, Order: #{order_expressions.inspect}, Limit: #{limit_count.inspect}") if debug

      freq_dataset = base_dataset.group_and_count(column_sym).order(*order_expressions)
      freq_dataset = freq_dataset.limit(limit_count) if limit_count

      sql_query = freq_dataset.sql
      print_debug("  Executing Frequency: #{sql_query}") if debug

      start_time = Time.now
      result = []
      begin
        result = freq_dataset.all
      rescue Sequel::DatabaseError => e
        puts colored_output("  SQL Frequency Error: #{e.message.lines.first.strip}", :red)
        print_debug "  Failed SQL: #{sql_query}" if debug
        return [] # Return empty array on SQL error
      rescue StandardError => e
        puts colored_output("  Error (Frequency Query): #{e.message.lines.first.strip}", :red)
        return []
      ensure
        duration = (Time.now - start_time).round(4)
        print_debug("  Frequency Duration: #{duration}s") if debug
      end
      result
    end

  end # class Analyzer
end # module DbReport
