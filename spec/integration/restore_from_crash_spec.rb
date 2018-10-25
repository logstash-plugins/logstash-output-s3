# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "Restore from crash", :integration => true do
  include_context "setup plugin"

  let(:options) { main_options.merge({ "restore" => true, "canned_acl" => "public-read-write" }) }

  let(:number_of_files) { 5 }
  let(:dummy_content) { "foobar\n" * 100 }
  let(:factory) { LogStash::Outputs::S3::TemporaryFileFactory.new(prefix, tags, "none", temporary_directory)}

  before do
    clean_remote_files(prefix)
  end


  context 'with a non-empty tempfile' do
    before do
      # Creating a factory always create a file
      factory.current.write(dummy_content)
      factory.current.fsync

      (number_of_files - 1).times do
        factory.rotate!
        factory.current.write(dummy_content)
        factory.current.fsync
      end
    end
    it "uploads the file to the bucket" do
      subject.register
      try(20) do
        expect(bucket_resource.objects(:prefix => prefix).count).to eq(number_of_files)
        expect(Dir.glob(File.join(temporary_directory, "*")).size).to eq(0)
        expect(bucket_resource.objects(:prefix => prefix).first.acl.grants.collect(&:permission)).to include("READ", "WRITE")
      end
    end
  end

  context 'with an empty tempfile' do
    before do
      factory.current
      factory.rotate!
    end

    it "should remove the temporary file" do
      expect(Dir.glob(::File.join(temporary_directory, "**", "*")).size).to be > 0
      subject.register
      puts Dir.glob(::File.join(temporary_directory, "**", "*"))
      expect(Dir.glob(::File.join(temporary_directory, "**", "*")).size).to eq(0)
    end

    it "should not upload the file to the bucket" do
      expect(bucket_resource.objects(:prefix => prefix).count).to eq(0)
      expect(Dir.glob(::File.join(temporary_directory, "**", "*")).size).to be > 0
      subject.register

      # Sleep to give enough time for plugin upload to s3 if it attempts to upload empty temporary file to S3
      sleep 5
      expect(bucket_resource.objects(:prefix => prefix).count).to eq(0)
    end
  end
end

