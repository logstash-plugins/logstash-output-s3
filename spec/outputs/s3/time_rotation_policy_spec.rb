# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3/size_rotation_policy"
require "logstash/outputs/s3/temporary_file"

describe LogStash::Outputs::S3::TimeRotationPolicy do
  subject { described_class.new(max_time) }

  let(:max_time) { 1 }
  let(:temporary_directory) { Stud::Temporary.directory }
  let(:name) { "foobar" }
  let(:content) { "hello" * 1000 }
  let(:file) { LogStash::Outputs::S3::TemporaryFile.new(temporary_directory, name) }

  before :each do
    FileUtils.mkdir_p(temporary_directory)
    file.write(content)
    file.close
  end

  it "returns true if the file old enough" do
    sleep(max_time * 2)
    expect(subject.rotate?(file)).to be_truthy
  end

  it "returns false is not old enough" do
    expect(subject.rotate?(file)).to be_falsey
  end
end
