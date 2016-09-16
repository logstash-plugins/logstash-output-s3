# encoding: utf-8
require "concurrent"
require "concurrent/map"
require "concurrent/timer_task"
require "logstash/util"

module LogStash
  module Outputs
    class S3
      class FileRepository
        DEFAULT_STATE_SWEEPER_INTERVAL_SECS = 60
        DEFAULT_STALE_TIME_SECS = 15 * 60
        # Ensure that all access or work done
        # on a factory is threadsafe
        class PrefixedValue
          def initialize(factory, stale_time)
            @factory = factory
            @lock = Mutex.new
            @stale_time = stale_time
          end

          def with_lock
            @lock.synchronize {
              yield @factory
            }
          end

          def stale?
            with_lock { |factory| factory.current.size == 0 && (Time.now - factory.current.ctime > @stale_time) }
          end
        end

        def initialize(tags, encoding, temporary_directory,
                       stale_time = DEFAULT_STALE_TIME_SECS,
                       sweeper_interval = DEFAULT_STATE_SWEEPER_INTERVAL_SECS)
          # The path need to contains the prefix so when we start
          # logtash after a crash we keep the remote structure
          @prefixed_factories =  Concurrent::Map.new

          @tags = tags
          @encoding = encoding
          @temporary_directory = temporary_directory

          @stale_time = stale_time
          @sweeper_interval = sweeper_interval

          start_stale_sweeper
        end

        # This method is not atomic, but in the code we are using
        # to check if file need rotation.
        def keys
          @prefixed_factories.keys
        end

        def each_files
          @prefixed_factories.each_value do |prefixed_file|
            prefixed_file.with_lock { |factory| yield factory.current }
          end
        end

        # Return the file factory
        def get_factory(prefix_key)
          @prefixed_factories.compute_if_absent(prefix_key) { PrefixedValue.new(TemporaryFileFactory.new(prefix_key, @tags, @encoding, @temporary_directory), @stale_time) }
            .with_lock { |factory| yield factory }
        end

        def get_file(prefix_key)
          get_factory(prefix_key) { |factory| yield factory.current }
        end

        def shutdown
          stop_stale_sweeper
        end

        def size
          @prefixed_factories.size
        end

        def start_stale_sweeper
          @stale_sweeper = Concurrent::TimerTask.new(:execution_interval => @sweeper_interval) do
            LogStash::Util.set_thread_name("S3, Stale factory sweeper")

            @prefixed_factories.each_pair do |k, v|
              @prefixed_factories.delete_pair(k, v) if v.stale?
            end
          end

          @stale_sweeper.execute
        end

        def stop_stale_sweeper
          @stale_sweeper.shutdown
        end
      end
    end
  end
end
