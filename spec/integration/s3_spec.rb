require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3"
require 'socket'
require "aws-sdk"
require "fileutils"
require "stud/temporary"
require_relative "../supports/helpers"

describe LogStash::Outputs::S3, :integration => true, :s3 => true do
  before do
    Thread.abort_on_exception = true
  end

  let!(:minimal_settings)  {  { "access_key_id" => ENV['AWS_ACCESS_KEY_ID'],
                                "secret_access_key" => ENV['AWS_SECRET_ACCESS_KEY'],
                                "bucket" => ENV['AWS_LOGSTASH_TEST_BUCKET'],
                                "region" => ENV["AWS_REGION"] || "us-east-1",
                                "temporary_directory" => Stud::Temporary.pathname('temporary_directory') }}

  let!(:s3_object) do
      s3output = LogStash::Outputs::S3.new(minimal_settings)
      s3output.register
      s3output.s3
  end

  after(:all) do
    delete_matching_keys_on_bucket('studtmp')
    delete_matching_keys_on_bucket('my-prefix')
  end

  describe "#register" do
    it "write a file on the bucket to check permissions" do
      s3 = LogStash::Outputs::S3.new(minimal_settings)
      expect(s3.register).not_to raise_error
    end
  end

  describe "#write_on_bucket" do
    after(:all) do
      File.unlink(fake_data.path)
    end

    let!(:fake_data) { Stud::Temporary.file }

    it "should prefix the file on the bucket if a prefix is specified" do
      prefix = "my-prefix"

      config = minimal_settings.merge({
        "prefix" => prefix,
      })

      s3 = LogStash::Outputs::S3.new(config)
      s3.register
      s3.write_on_bucket(fake_data)

      expect(key_exists_on_bucket?("#{prefix}#{File.basename(fake_data.path)}")).to eq(true)
    end

    it 'should use the same local filename if no prefix is specified' do
      s3 = LogStash::Outputs::S3.new(minimal_settings)
      s3.register
      s3.write_on_bucket(fake_data)

      expect(key_exists_on_bucket?(File.basename(fake_data.path))).to eq(true)
    end
  end

  describe "#move_file_to_bucket" do
    let!(:s3) { LogStash::Outputs::S3.new(minimal_settings) }

    before do
      s3.register
    end

    it "should upload the file if the size > 0" do
      tmp = Stud::Temporary.file
      allow(File).to receive(:zero?).and_return(false)
      s3.move_file_to_bucket(tmp)
      expect(key_exists_on_bucket?(File.basename(tmp.path))).to eq(true)
    end
  end

  describe "#restore_from_crashes" do
    it "read the temp directory and upload the matching file to s3" do
      Stud::Temporary.pathname do |temp_path|
        tempfile = File.open(File.join(temp_path, 'A'), 'w+') { |f| f.write('test')}

        s3 = LogStash::Outputs::S3.new(minimal_settings.merge({ "temporary_directory" => temp_path }))
        s3.restore_from_crashes

        expect(File.exist?(tempfile.path)).to eq(false)
        expect(key_exists_on_bucket?(File.basename(tempfile.path))).to eq(true)
      end
    end
  end
end
