# encoding: utf-8
require "socket"
require "securerandom"
require "fileutils"

module LogStash
  module Outputs
    class S3
      # Since the file can contains dynamic part, we have to handle a more local structure to
      # allow a nice recovery from a crash.
      #
      # The local structure will look like this.
      #
      # <TEMPORARY_PATH>/<UUID>/<prefix>/ls.s3.localhost.%Y-%m-%dT%H.%m.tag_es_fb.part1.txt.gz
      #
      # Since the UUID should be fairly unique I can destroy the whole path when an upload is complete.
      # I do not have to mess around to check if the other directory have file in it before destroying them.
      class TemporaryFileFactory
        FILE_MODE = "a"
        GZIP_ENCODING = "gzip"
        GZIP_EXTENSION = "txt.gz"
        TXT_EXTENSION = "txt"
        STRFTIME = "%Y-%m-%dT%H.%M"

        attr_accessor :counter, :tags, :prefix, :encoding, :temporary_directory, :current

        def initialize(prefix, tags, encoding, temporary_directory)
          @counter = 0
          @prefix = prefix

          @tags = tags
          @encoding = encoding
          @temporary_directory = temporary_directory

          rotate!
        end

        def rotate!
          @current = new_file
          increment_counter
          @current
        end

        private
        def extension
          gzip? ? GZIP_EXTENSION : TXT_EXTENSION
        end

        def gzip?
          encoding == GZIP_ENCODING
        end

        def increment_counter
          @counter += 1
        end

        def current_time
          Time.now.strftime(STRFTIME)
        end

        def generate_name
          filename = "ls.s3.#{SecureRandom.uuid}.#{current_time}"

          if tags.size > 0
            "#{filename}.tag_#{tags.join('.')}.part#{counter}.#{extension}"
          else
            "#{filename}.part#{counter}.#{extension}"
          end
        end

        def new_file
          uuid = SecureRandom.uuid
          name = generate_name
          path = ::File.join(temporary_directory, uuid, prefix)
          key = ::File.join(prefix, name)

          FileUtils.mkdir_p(path)

          io = if gzip?
                 Zlib::GzipWriter.open(::File.join(path, name))
               else
                 ::File.open(::File.join(path, name), FILE_MODE)
               end

          TemporaryFile.new(key, io)
        end
      end
    end
  end
end
