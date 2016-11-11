# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/size_rotation_policy"
require "logstash/outputs/s3/temporary_file"
require "fileutils"

describe LogStash::Outputs::S3::SizeRotationPolicy do
  subject { described_class.new(size_file) }

  let(:temporary_directory) {  Stud::Temporary.directory }
  let(:temporary_file) { Stud::Temporary.file }
  let(:name) { "foobar" }
  let(:content) { "hello" * 1000 }
  let(:size_file) { 10 } # in bytes
  let(:file) { LogStash::Outputs::S3::TemporaryFile.new(name, temporary_file, temporary_directory) }

  it "returns true if the size on disk is higher than the `size_file`" do
    file.write(content)
    file.fsync
    expect(subject.rotate?(file)).to be_truthy
  end

  it "returns false if the size is inferior than the `size_file`" do
    expect(subject.rotate?(file)).to be_falsey
  end

  it "raises an exception if the `size_file` is 0" do
    expect { described_class.new(0) }.to raise_error(LogStash::ConfigurationError, /need to be greather than 0/)
  end

  it "raises an exception if the `size_file` is < 0" do
    expect { described_class.new(-100) }.to raise_error(LogStash::ConfigurationError, /need to be greather than 0/)
  end

  context "#need_periodic?" do
    it "return false" do
      expect(subject.need_periodic?).to be_falsey
    end
  end

end
