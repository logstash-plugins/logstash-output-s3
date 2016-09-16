# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/temporary_file"
require "stud/temporary"
require "fileutils"
require "securerandom"

describe LogStash::Outputs::S3::TemporaryFile do
  let(:content) { "hello world" }
  let(:key) { "foo" }
  let(:uuid) { SecureRandom.uuid }
  let(:temporary_file) { ::File.open(::File.join(temporary_directory, uuid, key), "w+") }
  let(:temporary_directory) {  Stud::Temporary.directory }

  before :each do
    FileUtils.mkdir_p(::File.join(temporary_directory, uuid))
  end

  subject { described_class.new(key, temporary_file) }

  it "returns the key of the file" do
    expect(subject.key).to eq(key)
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

  described_class::DELEGATES_METHODS.each do |method_name|
    it "delegates method `#{method_name}` to file descriptor" do
      expect(subject.respond_to?(method_name)).to be_truthy
    end
  end

  it "returns the creation time" do
    expect(subject.ctime).to be < Time.now
  end
end
