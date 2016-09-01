# encoding: utf-8
require "logstash/util"
require "aws-sdk-resources"

module LogStash
  module Outputs
    class S3
      class Uploader
        DEFAULT_QUEUE_SIZE = 10
        attr_reader :bucket, :upload_options

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

        def upload_async(file, options = {})
          @workers_pool.post do
            LogStash::Util.set_thread_name("S3 output uploader, file: #{file.path}")
            upload(file, options = {})
          end
        end

        def upload(file, options = {})
          obj = bucket.object(file.key)
          obj.upload_file(file.path, upload_options)

          options[:on_complete].call(file) unless options[:on_complete].nil?
        end

        def stop
          @workers_pool.shutdown
          @workers_pool.wait_for_termination
        end
      end
    end
  end
end
