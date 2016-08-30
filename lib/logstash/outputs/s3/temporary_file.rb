# encoding: utf-8
require "thread"

module LogStash
  module Outputs
    class S3
      class TemporaryFile
        FILE_MODE = "w+"

        attr_reader :name

        def initialize(temporary_directory, name)
          target = ::File.join(temporary_directory, name)
          @fd = ::File.open(target, FILE_MODE)
          @name = name

          @write_lock = Mutex.new
        end

        def path
          @fd.path
        end

        def write(content)
          @write_lock.synchronize { @fd.write(content) }
        end

        def close
          @write_lock.synchronize { @fd.close }
        end

        def size
          @write_lock.synchronize { ::File.size(@fd.path) }
        end

        def ctime
          @write_lock.synchronize { ::File.ctime(@fd.path) }
        end

        def delete!
          @write_lock.synchronize { ::File.delete(@fd.path) }
        end
      end
    end
  end
end
