require "parse/db/import/utils"
module Parse
  module Db
    class Import
      def create_schema
        Dir["#{options.path}/#{options.entity}/data.json"].each do |file|
          missing_columns = {}
          klass = klass_from_file(file)
          puts "Scanning....#{klass.name}"
          process_parse_file(file) do |record|
            columns = get_missing_columns(klass, record.keys)
            unless columns.empty?
              columns.each { |k| missing_columns[k] = 0}
            end
            missing_columns.each do |k, v|
              len = record[k].to_s.length
              missing_columns[k] = len if v < len
            end
          end
          if (missing_columns.length)
            puts "Creating....#{klass.name} columns #{missing_columns.map{|k,v| "#{k} varchar(#{v})"}.join(", ")}"
            create_missing_columns(klass, missing_columns)
          end
        end
      end
    end
  end
end

