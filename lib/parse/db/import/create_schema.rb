require "parse/db/import/utils"
require "parse/db/import/activerecord_helpers"

module Parse
  module Db
    class Import
      def create_schema(options)
        Dir["#{options.path}/#{options.entity}.json-chunks"].each do |file|
          missing_columns = {}
          klass = klass_from_file(file)
          puts "Scanning....#{klass.name}"
          process_parse_file(file) do |record|
            columns = get_missing_columns(klass, record.keys)

            unless columns.empty?
              columns.each do |k|
                type = get_column_type(record[k], k)

                if type
                  missing_columns[k] = type
                else
                  puts "Skipping column #{k} for table #{klass.table_name} because of undefined type\n"
                end
              end
            end
            missing_columns.each do |k, v|
              if v[0] == :integer && Array(record[k]).any? { |val| !(-2 ** 32 / 2 ... 2 ** 32 / 2).cover?(val) }
                v[1] = (v[1] || {}).merge(limit: 8)
              end
            end
          end
          if (missing_columns.length)
            columns_description = missing_columns.map do |k, v|
              "#{k} #{v[0]}" + (v[1] ? "(#{v[1]})" : nil).to_s
            end

            puts "Creating....#{klass.name} columns #{columns_description.join(', ')}"

            create_missing_columns(klass, missing_columns)
          end
        end
      end
    end
  end
end
