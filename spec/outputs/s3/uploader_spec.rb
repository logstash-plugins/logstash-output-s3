# Encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/uploader"
require "logstash/outputs/s3/temporary_file"
require "aws-sdk-s3"
require "stud/temporary"

describe LogStash::Outputs::S3::Uploader do
  let(:logger) { spy(:logger) }
  let(:max_upload_workers) { 1 }
  let(:bucket_name) { "foobar-bucket" }
  let(:client) { Aws::S3::Client.new(stub_responses: true) }
  let(:bucket) { Aws::S3::Bucket.new(bucket_name, :client => client) }
  let(:temporary_directory) { Stud::Temporary.pathname }
  let(:temporary_file) { Stud::Temporary.file }
  let(:key) { "foobar" }
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

  subject { described_class.new(bucket, logger, threadpool, retry_delay: 0.01) }

  it "upload file to the s3 bucket" do
    expect { subject.upload(file) }.not_to raise_error
  end

  it "execute a callback when the upload is complete" do
    callback = proc { |_| }

    expect(callback).to receive(:call).with(file)
    subject.upload(file, :on_complete => callback)
  end

  it "retries errors indefinitely" do
    s3 = double("s3").as_null_object

    allow(bucket).to receive(:object).with(file.key).and_return(s3)

    expect(logger).to receive(:warn).with(any_args)
    expect(s3).to receive(:upload_file).with(any_args).and_raise(RuntimeError.new('UPLOAD FAILED')).exactly(5).times
    expect(s3).to receive(:upload_file).with(any_args).and_return(true)

    subject.upload(file)
  end

  it "retries errors specified times" do
    subject = described_class.new(bucket, logger, threadpool, retry_count: 3, retry_delay: 0.01)
    s3 = double("s3").as_null_object

    allow(bucket).to receive(:object).with(file.key).and_return(s3)

    expect(logger).to receive(:warn).with(any_args).exactly(3).times
    expect(logger).to receive(:error).with(any_args).once
    expect(s3).to receive(:upload_file).with(file.path, {}).and_raise(RuntimeError).at_least(1).times

    subject.upload(file)
  end
end
