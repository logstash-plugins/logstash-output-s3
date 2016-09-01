# encoding: utf-8
require "thread"
require "forwardable"
require "fileutils"

module LogStash
  module Outputs
    class S3
      # Make the delete a bit more sane
      class TemporaryFile
        extend Forwardable
        def_delegators :@fd, :path, :write, :close, :size, :ctime

        def initialize(key, fd)
          @fd = fd
          @key = key
        end

        def key
          @key.gsub(/^\//, "")
        end

        def delete!
          ::FileUtils.rm_rf(@fd.path.gsub(/#{Regexp.escape(key)}$/, ""))
        end

        def empty?
          size == 0
        end
      end
    end
  end
end
