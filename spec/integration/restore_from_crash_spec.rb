# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "Restore from crash", :integration => true do
  include_context "setup plugin"

  let(:options) { main_options.merge({ "restore" => true }) }

  let(:number_of_files) { 5 }
  let(:dummy_content) { "foobar\n" * 100 }

  before do
    clean_remote_files(prefix)
    # Use the S3 factory to create mutliples files with dummy content
    factory = LogStash::Outputs::S3::TemporaryFileFactory.new(prefix, tags, "none", temporary_directory)

    # Creating a factory always create a file
    factory.current.write(dummy_content)
    factory.current.fsync

    (number_of_files - 1).times do
      factory.current.write(dummy_content)
      factory.current.fsync
      factory.rotate!
    end
  end

  it "uploads the file to the bucket" do
    subject.register
    try(20) do
      expect(Dir.glob(File.join(temporary_directory, "*")).size).to eq(0)
      expect(bucket_resource.objects(:prefix => prefix).count).to eq(number_of_files)
    end
  end
end

