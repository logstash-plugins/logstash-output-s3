# encoding: utf-8
module LogStash
  module Outputs
    class S3
      class WriteableDirectoryValidator
        def self.valid?(path)
          File.writable?(path)
        end
      end
    end
  end
end
