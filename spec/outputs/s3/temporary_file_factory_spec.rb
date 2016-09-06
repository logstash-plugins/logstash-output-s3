# encoding: utf-8
require "logstash/outputs/s3/temporary_file_factory"
require "logstash/outputs/s3/temporary_file"
require "stud/temporary"
require "fileutils"

describe LogStash::Outputs::S3::TemporaryFileFactory do
  let(:prefix) { "foobar" }
  let(:tags) { [] }
  let(:temporary_directory) { Stud::Temporary.pathname }

  before do
    FileUtils.mkdir_p(temporary_directory)
  end

  subject { described_class.new(prefix, tags, encoding, temporary_directory) }

  shared_examples "file factory" do
    it "creates the file on disk" do
      expect(File.exist?(subject.current.path)).to be_truthy
    end

    it "create a temporary file when initialized" do
      expect(subject.current).to be_kind_of(LogStash::Outputs::S3::TemporaryFile)
    end

    it "create a file in the right format" do
      expect(subject.current.path).to match(extension)
    end

    it "allow to rotate the file" do
      file_path = subject.current.path
      expect(subject.rotate!.path).not_to eq(file_path)
    end

    it "increments the part name on rotation" do
      expect(subject.current.path).to match(/part0/)
      expect(subject.rotate!.path).to match(/part1/)
    end

    it "includes the date" do
      n = Time.now
      expect(subject.current.path).to match(/ls.s3.#{Socket.gethostname}.#{n.strftime("%Y-%m-%dT")}\d+\.\d+\./)
    end

    it "include the file key in the path"  do
      file = subject.current
      expect(file.path).to match(/#{file.key}/)
    end

    it "create a unique directory in the temporary directory for each file" do
      uuid = "hola"
      expect(SecureRandom).to receive(:uuid).and_return(uuid)
      expect(subject.current.path).to include(uuid)
    end

    context "with tags supplied" do
      let(:tags) { ["secret", "service"] }

      it "adds tags to the filename" do
        expect(subject.current.path).to match(/tag_#{tags.join('.')}.part/)
      end
    end

    context "without tags" do
      it "doesn't add tags to the filename" do
        expect(subject.current.path).not_to match(/tag_/)
      end
    end
  end

  context "when gzip" do
    let(:encoding) { "gzip" }
    let(:extension) { /\.txt.gz$/ }

    include_examples "file factory"
  end

  context "when encoding set to `none`" do
    let(:encoding) { "none" }
    let(:extension) { /\.txt$/ }

    include_examples "file factory"
  end
end
