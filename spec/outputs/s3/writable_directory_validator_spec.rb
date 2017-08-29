# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/writable_directory_validator"
require "stud/temporary"

describe LogStash::Outputs::S3::WritableDirectoryValidator do
  let(:temporary_directory) {  File.join(Stud::Temporary.directory, Time.now.to_i.to_s) }

  subject { described_class }

  context "when the directory doesn't exists" do
    it "creates the directory" do
      expect(Dir.exist?(temporary_directory)).to be_falsey
      expect(subject.valid?(temporary_directory)).to be_truthy
      expect(Dir.exist?(temporary_directory)).to be_truthy
    end
  end

  context "when the directory exist" do
    before do
      FileUtils.mkdir_p(temporary_directory)
    end

    it "doesn't change the directory" do
      expect(Dir.exist?(temporary_directory)).to be_truthy
      expect(subject.valid?(temporary_directory)).to be_truthy
      expect(Dir.exist?(temporary_directory)).to be_truthy
    end
  end

  it "return false if the directory is not writable" do
    expect(::File).to receive(:writable?).with(temporary_directory).and_return(false)
    expect(subject.valid?(temporary_directory)).to be_falsey
  end

  it "return true if the directory is writable" do
    expect(::File).to receive(:writable?).with(temporary_directory).and_return(true)
    expect(subject.valid?(temporary_directory)).to be_truthy
  end
end
