# encoding: utf-8
require "java"
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
          def initialize(file_factory, stale_time)
            @file_factory = file_factory
            @lock = Mutex.new
            @stale_time = stale_time
          end

          def with_lock
            @lock.synchronize {
              yield @file_factory
            }
          end

          def stale?
            with_lock { |factory| factory.current.size == 0 && (Time.now - factory.current.ctime > @stale_time) }
          end

          def apply(prefix)
            return self
          end

          def delete!
            with_lock{ |factory| factory.current.delete! }
          end
        end

        class FactoryInitializer

          def initialize(tags, encoding, temporary_directory, stale_time)
            @tags = tags
            @encoding = encoding
            @temporary_directory = temporary_directory
            @stale_time = stale_time
          end

          def create_value(prefix_key)
            PrefixedValue.new(TemporaryFileFactory.new(prefix_key, @tags, @encoding, @temporary_directory), @stale_time)
          end

        end

        def initialize(tags, encoding, temporary_directory,
                       stale_time = DEFAULT_STALE_TIME_SECS,
                       sweeper_interval = DEFAULT_STATE_SWEEPER_INTERVAL_SECS)
          # The path need to contains the prefix so when we start
          # logtash after a crash we keep the remote structure
          @prefixed_factories = Concurrent::Map.new

          @sweeper_interval = sweeper_interval

          @factory_initializer = FactoryInitializer.new(tags, encoding, temporary_directory, stale_time)

          start_stale_sweeper
        end

        def keys
          @prefixed_factories.keys
        end

        def each_files
          @prefixed_factories.values.each do |prefixed_file|
            prefixed_file.with_lock { |factory| yield factory.current }
          end
        end

        # Return the file factory
        def get_factory(prefix_key)
          prefix_val = @prefixed_factories.fetch_or_store(prefix_key) { @factory_initializer.create_value(prefix_key) }
          prefix_val.with_lock { |factory| yield factory }
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

        def remove_stale(k, v)
          if v.stale?
            @prefixed_factories.delete_pair(k, v)
            v.delete!
          end
        end

        def start_stale_sweeper
          @stale_sweeper = Concurrent::TimerTask.new(:execution_interval => @sweeper_interval) do
            LogStash::Util.set_thread_name("S3, Stale factory sweeper")

            @prefixed_factories.each { |k, v| remove_stale(k,v) }
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
