# Encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/uploader"
require "logstash/outputs/s3/temporary_file"
require "aws-sdk"
require "stud/temporary"

describe LogStash::Outputs::S3::Uploader do
  let(:logger) { spy(:logger ) }
  let(:max_upload_workers) { 1 }
  let(:bucket_name) { "foobar-bucket" }
  let(:client) { Aws::S3::Client.new(stub_responses: true) }
  let(:bucket) { Aws::S3::Bucket.new(bucket_name, :client => client) }
  let(:temporary_directory) { Stud::Temporary.pathname }
  let(:temporary_file) { Stud::Temporary.file }
  let(:key) { "foobar" }
  let(:upload_options) { {} }
  let(:threadpool) do
    Concurrent::ThreadPoolExecutor.new({
                                         :min_threads => 1,
                                         :max_threads => 8,
                                         :max_queue => 1,
                                         :fallback_policy => :caller_runs
                                       })
  end

  let(:file) do
    f = LogStash::Outputs::S3::TemporaryFile.new(key, temporary_file, temporary_directory)
    f.write("random content")
    f.fsync
    f
  end

  subject { described_class.new(bucket, logger, threadpool) }

  it "upload file to the s3 bucket" do
    subject.upload(file)
  end

  it "execute a callback when the upload is complete" do
    callback = proc { |f| }

    expect(callback).to receive(:call).with(file)
    subject.upload(file, { :on_complete => callback })
  end

  it "retries errors indefinitively" do
    s3 = double("s3").as_null_object

    expect(logger).to receive(:error).with(any_args).once
    expect(bucket).to receive(:object).with(file.key).and_return(s3).twice
    expect(s3).to receive(:upload_file).with(any_args).and_raise(StandardError)
    expect(s3).to receive(:upload_file).with(any_args).and_return(true)

    subject.upload(file)
  end
end
