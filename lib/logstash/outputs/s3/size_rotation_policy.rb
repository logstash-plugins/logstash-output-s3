# encoding: utf-8
module LogStash
  module Outputs
    class S3
      class SizeRotationPolicy
        attr_reader :max_size

        def initialize(max_size)
          @max_size = max_size
        end

        def rotate?(file)
          file.size > max_size
        end
      end
    end
  end
end
