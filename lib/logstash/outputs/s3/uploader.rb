# encoding: utf-8
require "logstash/util"
require "aws-sdk-resources"

module LogStash
  module Outputs
    class S3
      class Uploader
        DEFAULT_QUEUE_SIZE = 10
        attr_reader :bucket

        def initialize(bucket, upload_workers, upload_options = {}, queue = DEFAULT_QUEUE_SIZE)
          @bucket = bucket
          @upload_options = upload_options
          @workers_pool = Concurrent::ThreadPoolExecutor.new({
                                                               :min_threads => 1,
                                                               :max_threads => upload_workers,
                                                               :max_queue => queue,
                                                               :fallback_policy => :caller_runs
                                                             })
        end

        def do(file)
          @workers_pool.post do
            LogStash::Util.set_thread_name("S3 output uploader, file: #{file.path}")
            Upload(file)
          end
        end

        private
        def upload(file)
          obj = bucket.object(file.name)
          obj.upload_file(file.path, upload_options.merge)
        end
      end
    end
  end
end
