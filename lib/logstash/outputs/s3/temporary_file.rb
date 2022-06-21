# encoding: utf-8
require "thread"
require "forwardable"
require "fileutils"
require "logstash-output-s3_jars"

module LogStash
  module Outputs
    class S3

      java_import 'org.logstash.outputs.s3.GzipRecover'

      # Wrap the actual file descriptor into an utility class
      # Make it more OOP and easier to reason with the paths.
      class TemporaryFile
        extend Forwardable

        def_delegators :@fd, :path, :write, :close, :fsync

        attr_reader :fd

        def initialize(key, fd, temp_path, created_at = Time.now)
          @fd = fd
          @key = key
          @temp_path = temp_path
          @created_at = created_at
        end

        def ctime
          @created_at
        end

        def temp_path
          @temp_path
        end

        def size
          # Use the fd size to get the accurate result,
          # so we dont have to deal with fsync
          # if the file is close, fd.size raises an IO exception so we use the File::size
          begin
            @fd.size
          rescue IOError
            ::File.size(path)
          end
        end

        def key
          @key.gsub(/^\//, "")
        end

        # Each temporary file is created inside a directory named with an UUID,
        # instead of deleting the file directly and having the risk of deleting other files
        # we delete the root of the UUID, using a UUID also remove the risk of deleting unwanted file, it acts as
        # a sandbox.
        def delete!
          @fd.close rescue IOError # force close anyway
          FileUtils.rm_r(@temp_path, :secure => true)
        end

        def empty?
          size == 0
        end

        def self.create_from_existing_file(file_path, temporary_folder)
          key_parts = Pathname.new(file_path).relative_path_from(temporary_folder).to_s.split(::File::SEPARATOR)

          puts "File path: #{file_path}"
          GzipRecover.decompressGzip("", "")

          TemporaryFile.new(key_parts.slice(1, key_parts.size).join("/"),
                         ::File.open(file_path, "r"),
                         ::File.join(temporary_folder, key_parts.slice(0, 1)))
        end
      end
    end
  end
end
