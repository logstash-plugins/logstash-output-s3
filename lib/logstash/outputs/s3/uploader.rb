# encoding: utf-8
require "logstash/util"
require "aws-sdk-resources"

module LogStash
  module Outputs
    class S3
      class Uploader
        DEFAULT_QUEUE_SIZE = 10
        DEFAULT_THREADPOOL = Concurrent::ThreadPoolExecutor.new({
                                                                  :min_threads => 1,
                                                                  :max_threads => 8,
                                                                  :max_queue => 1,
                                                                  :fallback_policy => :caller_runs
                                                                })


        attr_reader :bucket, :upload_options, :logger

        def initialize(bucket, logger, threadpool = DEFAULT_THREADPOOL)
          @bucket = bucket
          @workers_pool = threadpool
          @logger = logger
        end

        def upload_async(file, options = {})
          @workers_pool.post do
            LogStash::Util.set_thread_name("S3 output uploader, file: #{file.path}")
            upload(file, options = {})
          end
        end

        def upload(file, options = {})
          begin
            obj = bucket.object(file.key)
            s3_options = options.fetch(:s3_options, {})
            obj.upload_file(file.path, s3_options)

            options[:on_complete].call(file) unless options[:on_complete].nil?
          rescue => e
            logger.error("Uploading failed", :exception => e)
          end
        end

        def stop
          @workers_pool.shutdown
          @workers_pool.wait_for_termination
        end
      end
    end
  end
end
