# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/size_and_time_rotation_policy"
require "logstash/outputs/s3/temporary_file"

describe LogStash::Outputs::S3::SizeAndTimeRotationPolicy do
  let(:file_size) { 10 }
  let(:time_file) { 1 }
  subject { described_class.new(file_size, time_file) }

  let(:temporary_directory) { Stud::Temporary.pathname }
  let(:temporary_file) { Stud::Temporary.file }
  let(:name) { "foobar" }
  let(:content) { "hello" * 1000 }
  let(:file) { LogStash::Outputs::S3::TemporaryFile.new(name, temporary_file, temporary_directory) }

  it "raises an exception if the `time_file` is set to 0" do
    expect { described_class.new(100, 0) }.to raise_error(LogStash::ConfigurationError, /time_file/)
  end

  it "raises an exception if the `time_file` is < 0" do
    expect { described_class.new(100, -100) }.to raise_error(LogStash::ConfigurationError, /time_file/)
  end

  it "raises an exception if the `size_file` is 0" do
    expect { described_class.new(0, 100) }.to raise_error(LogStash::ConfigurationError, /size_file/)
  end

  it "raises an exception if the `size_file` is < 0" do
    expect { described_class.new(-100, 100) }.to raise_error(LogStash::ConfigurationError, /size_file/)
  end

  it "returns true if the size on disk is higher than the `file_size`" do
    file.write(content)
    file.fsync
    expect(subject.rotate?(file)).to be_truthy
  end

  it "returns false if the size is inferior than the `file_size`" do
    expect(subject.rotate?(file)).to be_falsey
  end

  context "when the size of the file is superior to 0" do
    let(:file_size) { 10000 }

    before :each do
      file.write(content)
      file.fsync
    end

    it "returns true if the file old enough" do
      allow(file).to receive(:ctime).and_return(Time.now - (time_file * 2 * 60) )
      expect(subject.rotate?(file)).to be_truthy
    end

    it "returns false is not old enough" do
      allow(file).to receive(:ctime).and_return(Time.now + time_file * 10)
      expect(subject.rotate?(file)).to be_falsey
    end
  end

  context "When the size of the file is 0" do
    it "returns false if the file old enough" do
      expect(subject.rotate?(file)).to be_falsey
    end

    it "returns false is not old enough" do
      expect(subject.rotate?(file)).to be_falsey
    end
  end

  context "#need_periodic?" do
    it "return true" do
      expect(subject.need_periodic?).to be_truthy
    end
  end
end
