require "parse/db/import/utils"

module Parse
  module Db
    class Import
      def import_data(options)
        Dir["#{options.path}/#{options.entity}.json-chunks"].each do |file|
          klass = klass_from_file(file)
          puts "Importing....#{klass.name}"
          process_parse_file(file) do |record|
            begin
              columns = klass.columns_hash.keys
              klass.create!(record.select { |k, v| columns.include?(k) })
            rescue Exception
              puts record
              raise
            end
          end
        end
      end
    end
  end
end
