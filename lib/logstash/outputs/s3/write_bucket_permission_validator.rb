# encoding: utf-8
require "stud/temporary"
require "socket"

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

          Stud::Temporary.file do |f|
            f.write(content)
            f.fsync

            obj = bucket_resource.object(key)
            obj.upload_file(key)

            begin
              obj.delete
            rescue
              # Try to remove the files on the remote bucket,
              # but don't raise any errors if that doesn't work.
              # since we only really need `putobject`.
            end
          end
        end
      end
    end
  end
end
