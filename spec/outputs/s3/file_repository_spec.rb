# encoding: utf-8
require "logstash/outputs/s3"
require "stud/temporary"
require "fileutils"
require_relative "../../spec_helper"

describe LogStash::Outputs::S3::FileRepository do
  let(:tags) { ["secret", "service"] }
  let(:encoding) { "none" }
  let(:temporary_directory) { Stud::Temporary.pathname }
  let(:prefix_key) { "a-key" }
  let(:filename_key) { "a-filename-key" }

  before do
    FileUtils.mkdir_p(temporary_directory)
  end

  subject { described_class.new(tags, encoding, temporary_directory) }

  it "returns a temporary file" do
    subject.get_file(prefix_key) do |file|
      expect(file).to be_kind_of(LogStash::Outputs::S3::TemporaryFile)
    end

    subject.get_file(prefix_key, filename_key: filename_key) do |file|
      expect(file).to be_kind_of(LogStash::Outputs::S3::TemporaryFile)
    end
  end

  it "returns the same file for the same prefix key and filename key" do
    file_path = nil

    subject.get_file(prefix_key) do |file|
      file_path = file.path
    end
    subject.get_file(prefix_key) do |file|
      expect(file.path).to eq(file_path)
    end

    subject.get_file(prefix_key, filename_key: filename_key) do |file|
      file_path = file.path
    end
    subject.get_file(prefix_key, filename_key: filename_key) do |file|
      expect(file.path).to eq(file_path)
    end
  end

  it "returns the same file for the same dynamic prefix key and filename key" do
    prefix = "%{type}/%{+YYYY}/%{+MM}/%{+dd}/"
    name = "${type}.txt"
    event = LogStash::Event.new({ "type" => "syslog"})
    key = event.sprintf(prefix)
    name = event.sprintf(name)
    file_path = nil

    subject.get_file(key) do |file|
      file_path = file.path
    end
    subject.get_file(key) do |file|
      expect(file.path).to eq(file_path)
    end

    subject.get_file(key, filename_key: name) do |file|
      file_path = file.path
    end
    subject.get_file(key, filename_key: name) do |file|
      expect(file.path).to eq(file_path)
    end
  end

  it "returns different file for different prefix keys and filename keys" do
    file_path = nil

    subject.get_file(prefix_key) do |file|
      file_path = file.path
    end
    subject.get_file("another_prefix_key") do |file|
      expect(file.path).not_to eq(file_path)
    end

    subject.get_file(prefix_key, filename_key: filename_key) do |file|
      file_path = file.path
    end
    subject.get_file(prefix_key, filename_key: "another_filename_key") do |file|
      expect(file.path).not_to eq(file_path)
    end
  end

  it "allows to get the file factory for a specific prefix or filename" do
    subject.get_factory(prefix_key) do |factory|
      expect(factory).to be_kind_of(LogStash::Outputs::S3::TemporaryFileFactory)
    end

    subject.get_factory(prefix_key, filename_key: filename_key) do |factory|
      expect(factory).to be_kind_of(LogStash::Outputs::S3::TemporaryFileFactory)
    end
  end

  it "returns a different file factory for different prefix keys and filename keys" do
    factory = nil

    subject.get_factory(prefix_key) do |f|
      factory = f
    end
    subject.get_factory("another_prefix_key") do |f|
      expect(factory).not_to eq(f)
    end

    subject.get_factory(prefix_key, filename_key: filename_key) do |f|
      factory = f
    end
    subject.get_factory(prefix_key, filename_key: "another_filename)key") do |f|
      expect(factory).not_to eq(f)
    end
  end

  it "returns the number of prefix keys" do
    expect(subject.size).to eq(0)
    subject.get_file(prefix_key) { |file| file.write("something") }
    expect(subject.size).to eq(1)
  end

  it "returns the number of prefix with filename keys" do
    expect(subject.size).to eq(0)
    subject.get_file(prefix_key, filename_key: filename_key) { |file| file.write("something else") }
    expect(subject.size).to eq(1)
  end

  it "returns all available prefix keys" do
    subject.get_file(prefix_key) { |file| file.write("something") }
    expect(subject.keys.toArray).to include(prefix_key)
    expect(subject.keys.toArray.size).to eq(1)
  end

  it "returns all available prefix with filename keys" do
    subject.get_file(prefix_key, filename_key: filename_key) { |file| file.write("something else") }
    expect(subject.keys.toArray).to include(prefix_key + "/" + filename_key)
    expect(subject.keys.toArray.size).to eq(1)
  end

  it "cleans stale prefix factories" do
    @file_repository = described_class.new(tags, encoding, temporary_directory, 1, 1)
    expect(@file_repository.size).to eq(0)
    path = ""
    @file_repository.get_factory(prefix_key) do |factory|
      factory.current.write("hello")
      # force a rotation so we get an empty file that will get stale.
      factory.rotate!
      path = factory.current.temp_path
    end

    @file_repository.get_file("another-prefix") { |file| file.write("hello") }
    expect(@file_repository.size).to eq(2)
    try(10) { expect(@file_repository.size).to eq(1) }
    expect(File.directory?(path)).to be_falsey
  end

  it "cleans stale prefix with filename factories" do
    @file_repository = described_class.new(tags, encoding, temporary_directory, 1, 1)
    expect(@file_repository.size).to eq(0)
    path = ""
    @file_repository.get_factory(prefix_key, filename_key: filename_key) do |factory|
      factory.current.write("hello again")
      # force a rotation so we get an empty file that will get stale.
      factory.rotate!
      path = factory.current.temp_path
    end

    @file_repository.get_file(prefix_key, filename_key: "another-filename") { |file| file.write("hello again") }
    expect(@file_repository.size).to eq(2)
    try(10) { expect(@file_repository.size).to eq(1) }
    expect(File.directory?(path)).to be_falsey
  end
end


describe LogStash::Outputs::S3::FileRepository::PrefixedValue do
  let(:factory) { spy("factory", :current => file) }
  subject { described_class.new(factory, 1) }

  context "#stale?" do
    context "the file is empty and older than stale time" do
      let(:file) { double("file", :size => 0, :ctime => Time.now - 5) }

      it "returns true" do
        expect(subject.stale?).to be_truthy
      end
    end

    context "when the file has data in it" do
      let(:file) { double("file", :size => 200, :ctime => Time.now - 5) }

      it "returns false" do
        expect(subject.stale?).to be_falsey
      end
    end

    context "when the file is not old enough" do
      let(:file) { double("file", :size => 0, :ctime => Time.now + 100) }

      it "returns false" do
        expect(subject.stale?).to be_falsey
      end
    end
  end
end
