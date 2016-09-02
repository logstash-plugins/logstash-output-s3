# encoding: utf-8
require "concurrent/map"

module LogStash
  module Outputs
    class S3
      class FileRepository
        # Ensure that all access or work done
        # on a factory is threadsafe
        class PrefixedValue
          STALE_TIME_SECS = 60*60 

          attr_accessor :last_access

          def initialize(factory)
            @factory = factory
            @lock = Mutex.new
            @last_access = Time.now
          end

          def with_lock
            @lock.synchronize {
              @last_access = Time.now
              yield @factory
            }
          end

          def stale?
            with_lock { |factory| factory.current.empty? && Time.now - last_access > STALE_TIME_SECS  }
          end
        end

        def initialize(tags, encoding, temporary_directory)
          # The path need to contains the prefix so when we start
          # logtash after a crash we keep the remote structure
          @prefixed_factories =  Concurrent::Map.new

          @tags = tags
          @encoding = encoding
          @temporary_directory = temporary_directory

          start_stale_sweeper
        end

        def each_files
          @prefixed_factories.values do |prefixed_file|
            prefixed_file.with_lock { |factory| yield factory.current }
          end
        end


        # Return the file factory
        def get_factory(prefix_key)
          @prefixed_factories.compute_if_absent(prefix_key) { PrefixedValue.new(TemporaryFileFactory.new(prefix_key, @tags, @encoding, @temporary_directory)) }
            .with_lock { |factory| yield factory }
        end

        def get_file(prefix_key)
          get_factory(prefix_key) { |factory| yield factory.current }
        end

        def shutdown
          stop_stale_sweeper
        end

        def start_stale_sweeper
          @stale_sweeper = Concurrent::TimerTask.new(:execution_interval => 1) do
            LogStash::Util.set_name("S3, Stale file sweeper")
            #@prefixed_facto\RIES.delete_if u{ |k, v| }
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
