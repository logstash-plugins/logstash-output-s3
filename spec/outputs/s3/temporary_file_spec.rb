# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/temporary_file"
require "stud/temporary"
require "fileutils"

describe LogStash::Outputs::S3::TemporaryFile do
  let(:content) { "hello world" }
  let(:name) { "foo" }
  let(:temporary_directory) {  Stud::Temporary.directory }
  before :each do
    FileUtils.mkdir_p(temporary_directory)
  end

  subject { described_class.new(temporary_directory, name) }

  it "returns the name of the file" do
    expect(subject.name).to eq(name)
  end

  it "saves content to a file" do
    subject.write(content)
    subject.close
    expect(File.read(subject.path).strip).to eq(content)
  end

  it "deletes a file" do
    expect(File.exist?(subject.path)).to be_truthy
    subject.delete!
    expect(File.exist?(subject.path)).to be_falsey
  end

  it "returns the size of the file" do
    subject.write(content)
    subject.close
    expect(subject.size).to be > 0
  end

  it "return the `ctime` of the file" do
    t = Time.now
    expect(subject.ctime).to be < t
  end
end
