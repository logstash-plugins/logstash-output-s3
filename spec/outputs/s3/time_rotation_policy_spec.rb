# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/time_rotation_policy"
require "logstash/outputs/s3/temporary_file"

describe LogStash::Outputs::S3::TimeRotationPolicy do
  subject { described_class.new(max_time) }

  let(:max_time) { 1 }
  let(:temporary_file) { Stud::Temporary.file }
  let(:name) { "foobar" }
  let(:content) { "hello" * 1000 }
  let(:file) { LogStash::Outputs::S3::TemporaryFile.new(name, temporary_file) }

  it "raises an exception if the `file_time` is set to 0" do
    expect { described_class.new(0) }.to raise_error(LogStash::ConfigurationError, /`time_file` need to be greather than 0/)
  end

  it "raises an exception if the `file_time` is < 0" do
    expect { described_class.new(-100) }.to raise_error(LogStash::ConfigurationError, /`time_file` need to be greather than 0/)
  end

  context "when the size of the file is superior to 0" do
    before :each do
      file.write(content)
      file.fsync
    end

    it "returns true if the file old enough" do
      allow(file).to receive(:ctime).and_return(Time.now - (max_time * 2 * 60))
      expect(subject.rotate?(file)).to be_truthy
    end

    it "returns false is not old enough" do
      expect(subject.rotate?(file)).to be_falsey
    end
  end

  context "When the size of the file is 0" do
    it "returns false if the file old enough" do
      allow(file).to receive(:ctime).and_return(Time.now - (max_time * 2 * 60))
      expect(subject.rotate?(file)).to be_falsey
    end

    it "returns false is not old enough" do
      expect(subject.rotate?(file)).to be_falsey
    end
  end

  context "#need_periodic?" do
    it "return false" do
      expect(subject.need_periodic?).to be_truthy
    end
  end
end
