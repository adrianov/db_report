# frozen_string_literal: true

require 'sequel'
require 'set'
require_relative 'utils'
require_relative 'db_analyzer/base'

module DbReport
  # Compatibility class that delegates to the new DbAnalyzer::Base
  # This maintains backwards compatibility while allowing for a cleaner code structure
  class Analyzer
    def self.new(db_connection, options)
      # Use the Base class implementation from the new namespace structure
      DbReport::DbAnalyzer::Base.new(db_connection, options)
    end
  end
end
