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
            @lock = Monitor.new
            @stale_time = stale_time
            @is_deleted = false
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
            with_lock do |factory|
              factory.current.delete!
              @is_deleted = true
            end
          end

          def deleted?
            with_lock { |_| @is_deleted }
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
          each_factory(keys) do |factory|
            yield factory.current
          end
        end

        ##
        # Yields the file factory while the current thread has exclusive access to it, creating a new
        # one if one does not exist or if the current one is being reaped by the stale watcher.
        # @param prefix_key [String]: the prefix key
        # @yieldparam factory [TemporaryFileFactory]: a temporary file factory that this thread has exclusive access to
        # @yieldreturn [Object]: a value to return; should NOT be the factory, which should be contained by the exclusive access scope.
        # @return [Object]: the value returned by the provided block
        def get_factory(prefix_key)

          # fast-path: if factory exists and is not deleted, yield it with exclusive access and return
          prefix_val = @prefixed_factories.get(prefix_key)
          prefix_val&.with_lock do |factory|
            # intentional local-jump to ensure deletion detection
            # is done inside the exclusive access.
            return yield(factory) unless prefix_val.deleted?
          end

          # slow-path:
          # the Concurrent::Map#get operation is lock-free, but may have returned an entry that was being deleted by
          # another thread (such as via stale detection). If we failed to retrieve a value, or retrieved one that had
          # been marked deleted, use the atomic Concurrent::Map#compute to retrieve a non-deleted entry.
          prefix_val = @prefixed_factories.compute(prefix_key) do |existing|
            existing && !existing.deleted? ? existing : @factory_initializer.create_value(prefix_key)
          end
          prefix_val.with_lock { |factory| yield factory }
        end

        ##
        # Yields each non-deleted file factory while the current thread has exclusive access to it.
        # @param prefixes [Array<String>]: the prefix keys
        # @yieldparam factory [TemporaryFileFactory]
        # @return [void]
        def each_factory(prefixes)
          prefixes.each do |prefix_key|
            prefix_val = @prefixed_factories.get(prefix_key)
            prefix_val&.with_lock do |factory|
              yield factory unless prefix_val.deleted?
            end
          end
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

        def remove_if_stale(prefix_key)
          # we use the ATOMIC `Concurrent::Map#compute_if_present` to atomically
          # detect the staleness, mark a stale prefixed factory as deleted, and delete from the map.
          @prefixed_factories.compute_if_present(prefix_key) do |prefixed_factory|
            # once we have retrieved an instance, we acquire exclusive access to it
            # for stale detection, marking it as deleted before releasing the lock
            # and causing it to become deleted from the map.
            prefixed_factory.with_lock do |_|
              if prefixed_factory.stale?
                prefixed_factory.delete! # mark deleted to prevent reuse
                nil # cause deletion
              else
                prefixed_factory # keep existing
              end
            end
          end
        end

        def start_stale_sweeper
          @stale_sweeper = Concurrent::TimerTask.new(:execution_interval => @sweeper_interval) do
            LogStash::Util.set_thread_name("S3, Stale factory sweeper")

            @prefixed_factories.keys.each do |prefix|
              remove_if_stale(prefix)
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
