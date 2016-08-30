# encoding: utf-8
module LogStash
  module Ouputs
    class S3
      class PathValidator
        INVALID_CHARACTERS = /[\^`><]/

        def self.valid?(name)
          name !~ INVALID_CHARACTERS
        end
      end
    end
  end
end
