# encoding: utf-8
require "thread"
require "forwardable"
require "fileutils"

module LogStash
  module Outputs
    class S3
      # Wrap the actual file descriptor into an utility classe
      # It make it more OOP and easier to reason with the paths.
      class TemporaryFile
        extend Forwardable
        DELEGATES_METHODS = [:path, :write, :close, :size, :fsync]

        def_delegators :@fd, *DELEGATES_METHODS

        def initialize(key, fd, temp_path)
          @fd = fd
          @key = key
          @temp_path = temp_path
          @created_at = Time.now
        end

        def ctime
          @created_at
        end

        def temp_path
          @temp_path
        end

        def key
          @key.gsub(/^\//, "")
        end

        # Each temporary file is made inside a directory named with an UUID,
        # instead of deleting the file directly and having the risk of deleting other files
        # we delete the root of the UUID, using a UUID also remove the risk of deleting unwanted file, it acts as
        # a sandbox.
        def delete!
          @fd.close
          ::FileUtils.rm_rf(@temp_path, :secure => true)
        end

        def empty?
          size == 0
        end
      end
    end
  end
end
