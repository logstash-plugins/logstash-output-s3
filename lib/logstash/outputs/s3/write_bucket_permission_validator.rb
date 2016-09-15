# encoding: utf-8
require "stud/temporary"
require "socket"
require "fileutils"

module LogStash
  module Outputs
    class S3
      class WriteBucketPermissionValidator
        def self.valid?(bucket_resource)
          begin
            upload_test_file(bucket_resource)
            true
          rescue
            false
          end
        end

        private
        def self.upload_test_file(bucket_resource)
          generated_at = Time.now

          key = "logstash-programmatic-access-test-object-#{generated_at}"
          content = "Logstash permission check on #{generated_at}, by #{Socket.gethostname}"

          begin
            f = Stud::Temporary.file
            f.write(content)
            f.fsync
            f.close

            obj = bucket_resource.object(key)
            obj.upload_file(f)

            begin
              obj.delete
            rescue
              # Try to remove the files on the remote bucket,
              # but don't raise any errors if that doesn't work.
              # since we only really need `putobject`.
            end
          ensure
            FileUtils.rm_rf(f.path)
          end
        end
      end
    end
  end
end
