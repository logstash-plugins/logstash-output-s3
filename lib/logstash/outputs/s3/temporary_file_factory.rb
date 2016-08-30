# encoding: utf-8
require "socket"

module LogStash
  module Ouputs
    class S3
      class TemporaryFileFactory
        FILE_MODE = "a"
        GZIP_ENCODING = "gzip"
        GZIP_EXTENSION = "txt.gz"
        TXT_EXTENSION = "txt"
        STR_FTIME = "%Y-%m-%dT%H.%M"

        attr_accessor :conter, :tags, :prefix, :encoding, :temporary_directory

        def initialize(input)
          @counter = 0
          @tags = input.tags
          @prefix = input.prefix
          @encoding = input.encoding
          @temporary_directory = input.temporary_directory
        end

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
          Time.now.strftime(STR_FTIME)
        end

        def generate_name
          filename = "ls.s3.#{Socket.gethostname}.#{current_time}"

          if tags.size > 0
            return "#{filename}.tag_#{tags.join('.')}.part#{page_counter}.#{extension}"
          else
            return "#{filename}.part#{counter}.#{extension}"
          end
        end

        def filename
          ::File.join(temporary_directory, generate_name)
        end

        def new_file
          if gzip?
            Zlib::GzipWriter.open(filename)
          else
            File.open(filename, "a")
          end
        end

        def get
          file = TemporaryFile.new(generate_name)
          increment_counter
          file
        end
      end
    end
  end
end
