# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/size_rotation_policy"
require "logstash/outputs/s3/temporary_file"
require "fileutils"

describe LogStash::Outputs::S3::SizeRotationPolicy do
  subject { described_class.new(max_size) }

  let(:temporary_directory) { Stud::Temporary.directory }
  let(:name) { "foobar" }
  let(:content) { "hello" * 1000 }
  let(:max_size) { 10 } # in bytes
  let(:file) { LogStash::Outputs::S3::TemporaryFile.new(temporary_directory, name) }

  before :each do
    FileUtils.mkdir_p(temporary_directory)
  end

  it "returns true if the size on disk is higher than the `max_size`" do
    file.write(content)
    expect(subject.rotate?(file)).to be_truthy
  end

  it "returns false if the size is inferior than the `max_size`" do
    expect(subject.rotate?(file)).to be_falsey
  end
end
