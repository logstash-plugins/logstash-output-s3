# encoding: utf-8
module LogStash
  module Outputs
    class S3
      class TimeRotationPolicy
        attr_reader :max_age

        def initialize(max_age)
          @max_age = max_age
        end

        def rotate?(file)
          Time.now - file.ctime >= max_age
        end
      end
    end
  end
end
