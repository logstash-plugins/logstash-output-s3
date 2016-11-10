# encoding: utf-8
require "java"
require "concurrent"
require "concurrent/timer_task"
require "logstash/util"

java_import "java.util.concurrent.ConcurrentHashMap"

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

          def apply(prefix)
            return self
          end
        end

        def initialize(tags, encoding, temporary_directory,
                       stale_time = DEFAULT_STALE_TIME_SECS,
                       sweeper_interval = DEFAULT_STATE_SWEEPER_INTERVAL_SECS)
          # The path need to contains the prefix so when we start
          # logtash after a crash we keep the remote structure
          @prefixed_factories =  ConcurrentHashMap.new

          @tags = tags
          @encoding = encoding
          @temporary_directory = temporary_directory

          @stale_time = stale_time
          @sweeper_interval = sweeper_interval

          start_stale_sweeper
        end

        def keys
          arr = []
          @prefixed_factories.keys.each {|k| arr << k}
          arr
        end

        def each_files
          @prefixed_factories.elements.each do |prefixed_file|
            prefixed_file.with_lock { |factory| yield factory.current }
          end
        end

        # Return the file factory
        def get_factory(prefix_key)
          @prefixed_factories.computeIfAbsent(prefix_key, PrefixedValue.new(TemporaryFileFactory.new(prefix_key, @tags, @encoding, @temporary_directory), @stale_time)).with_lock { |factory| yield factory }
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
            @prefixed_factories.remove(k, v)
            v.with_lock{ |factor| factor.current.delete!}
          end
        end

        def start_stale_sweeper
          @stale_sweeper = Concurrent::TimerTask.new(:execution_interval => @sweeper_interval) do
            LogStash::Util.set_thread_name("S3, Stale factory sweeper")

            @prefixed_factories.forEach{|k,v| remove_stale(k,v)}
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
