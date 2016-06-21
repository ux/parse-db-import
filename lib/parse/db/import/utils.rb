require 'date'

module Parse
  module Db
    class Import

      DATE_CONVENTIONS = [ /At$/, /date$/i ]

      def map_data_types(record)
        #Convert epoch to datetime
        record.each do |k, v|
          next unless is_date_by_naming_convention(k)

          begin
            record[k] = DateTime.strptime(v.to_s, "%Q")
          rescue => e
            puts "Date will be lost due to invalid value: #{v}\n#{e}\n#{record.inspect}\n"
            record[k] = nil
          end
        end
      end

      def is_date_by_naming_convention column_name
        DATE_CONVENTIONS.any? { |regx| column_name =~ regx }
      end

      def klass_from_file(file)
        class_name = File.basename(file, '.json-chunks')
        get_class(class_name)
      end

      def process_parse_file(file, &block)
        IO.foreach(file) do |record|
          record = JSON.parse(record)
          record = map_data_types(record)
          next if record["delete"]
          yield(record)
        end
      end

    end
  end
end
