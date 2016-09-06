# encoding: utf-8
module LogStash
  module Outputs
    class S3
      class PathValidator
        INVALID_CHARACTERS = "\^`><"

        def self.valid?(name)
          name.match(/[#{Regexp.escape(INVALID_CHARACTERS)}]/).nil?
        end
      end
    end
  end
end
