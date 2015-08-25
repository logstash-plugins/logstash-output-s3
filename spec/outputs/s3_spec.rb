# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "logstash/pipeline"
require "aws-sdk"
require "fileutils"
require_relative "../supports/helpers"

describe LogStash::Outputs::S3 do
  before do
    # We stub all the calls from S3, for more information see:
    # http://ruby.awsblog.com/post/Tx2SU6TYJWQQLC3/Stubbing-AWS-Responses
    AWS.stub!
    Thread.abort_on_exception = true
  end

  let(:minimal_settings)  {  { "access_key_id" => "1234",
                               "secret_access_key" => "secret",
                               "bucket" => "my-bucket" } }

  describe "configuration" do
    let!(:config) { { "endpoint_region" => "sa-east-1" } }

    it "should support the deprecated endpoint_region as a configuration option" do
      s3 = LogStash::Outputs::S3.new(config)
      expect(s3.aws_options_hash[:s3_endpoint]).to eq("s3-sa-east-1.amazonaws.com")
    end

    it "should fallback to region if endpoint_region isnt defined" do
      s3 = LogStash::Outputs::S3.new(config.merge({ "region" => 'sa-east-1' }))
      expect(s3.aws_options_hash).to include(:s3_endpoint => "s3-sa-east-1.amazonaws.com")
    end
  end

  describe "#register" do
    it "should create the tmp directory if it doesn't exist" do
      temporary_directory = Stud::Temporary.pathname("temporary_directory")

      config = {
        "access_key_id" => "1234",
        "secret_access_key" => "secret",
        "bucket" => "logstash",
        "size_file" => 10,
        "temporary_directory" => temporary_directory
      }

      s3 = LogStash::Outputs::S3.new(config)
      allow(s3).to receive(:test_s3_write)
      s3.register

      expect(Dir.exist?(temporary_directory)).to eq(true)
      s3.teardown
      FileUtils.rm_r(temporary_directory)
    end

    it "should raise a ConfigurationError if the prefix contains one or more '\^`><' characters" do
      config = {
        "prefix" => "`no\><^"
      }

      s3 = LogStash::Outputs::S3.new(config)

      expect {
        s3.register
      }.to raise_error(LogStash::ConfigurationError)
    end
  end

  describe "#generate_temporary_filename" do
    before do
      allow(Socket).to receive(:gethostname) { "logstash.local" }
      allow(Time).to receive(:now) { Time.new('2015-10-09-09:00') }
    end

    it "should add tags to the filename if present" do
      config = minimal_settings.merge({ "tags" => ["elasticsearch", "logstash", "kibana"], "temporary_directory" => "/tmp/logstash"})
      s3 = LogStash::Outputs::S3.new(config)
      expect(s3.get_temporary_filename).to eq("ls.s3.logstash.local.2015-01-01T00.00.tag_elasticsearch.logstash.kibana.part0.txt")
    end

    it "should not add the tags to the filename" do
      config = minimal_settings.merge({ "tags" => [], "temporary_directory" => "/tmp/logstash" })
      s3 = LogStash::Outputs::S3.new(config)
      expect(s3.get_temporary_filename(3)).to eq("ls.s3.logstash.local.2015-01-01T00.00.part3.txt")
    end

    it "normalized the temp directory to include the trailing slash if missing" do
      s3 = LogStash::Outputs::S3.new(minimal_settings.merge({ "temporary_directory" => "/tmp/logstash" }))
      expect(s3.get_temporary_filename).to eq("ls.s3.logstash.local.2015-01-01T00.00.part0.txt")
    end
  end

  describe "#write_on_bucket" do
    let!(:fake_data) { Stud::Temporary.file }

    let(:fake_bucket) do
      s3 = double('S3Object')
      allow(s3).to receive(:write)
      s3
    end

    it "should prefix the file on the bucket if a prefix is specified" do
      prefix = "my-prefix"

      config = minimal_settings.merge({
        "prefix" => prefix,
        "bucket" => "my-bucket"
      })

      expect_any_instance_of(AWS::S3::ObjectCollection).to receive(:[]).with("#{prefix}#{File.basename(fake_data)}") { fake_bucket }

      s3 = LogStash::Outputs::S3.new(config)
      allow(s3).to receive(:test_s3_write)
      s3.register
      s3.write_on_bucket(fake_data)
    end

    it 'should use the same local filename if no prefix is specified' do
      config = minimal_settings.merge({
        "bucket" => "my-bucket"
      })

      expect_any_instance_of(AWS::S3::ObjectCollection).to receive(:[]).with(File.basename(fake_data)) { fake_bucket }

      s3 = LogStash::Outputs::S3.new(minimal_settings)
      allow(s3).to receive(:test_s3_write)
      s3.register
      s3.write_on_bucket(fake_data)
    end
  end

  describe "#write_events_to_multiple_files?" do
    it 'returns true if the size_file is != 0 ' do
      s3 = LogStash::Outputs::S3.new(minimal_settings.merge({ "size_file" => 200 }))
      expect(s3.write_events_to_multiple_files?).to eq(true)
    end

    it 'returns false if size_file is zero or not set' do
      s3 = LogStash::Outputs::S3.new(minimal_settings)
      expect(s3.write_events_to_multiple_files?).to eq(false)
    end
  end

  describe "#write_to_tempfile" do
    it "should append the event to a file" do
      Stud::Temporary.file("logstash", "a+") do |tmp|
        s3 = LogStash::Outputs::S3.new(minimal_settings)
        allow(s3).to receive(:test_s3_write)
        s3.register
        s3.tempfile = tmp
        s3.write_to_tempfile("test-write")
        tmp.rewind
        expect(tmp.read).to eq("test-write")
      end
    end
  end

  describe "#rotate_events_log" do

    context "having a single worker" do
      let(:s3) { LogStash::Outputs::S3.new(minimal_settings.merge({ "size_file" => 1024 })) }

      before(:each) do
        s3.register
      end

      it "returns true if the tempfile is over the file_size limit" do
        Stud::Temporary.file do |tmp|
          allow(tmp).to receive(:size) { 2024001 }

          s3.tempfile = tmp
          expect(s3.rotate_events_log?).to be(true)
        end
      end

      it "returns false if the tempfile is under the file_size limit" do
        Stud::Temporary.file do |tmp|
          allow(tmp).to receive(:size) { 100 }

          s3.tempfile = tmp
          expect(s3.rotate_events_log?).to eq(false)
        end
      end
    end

    context "having periodic rotations" do
      let(:s3)  { LogStash::Outputs::S3.new(minimal_settings.merge({ "size_file" => 1024, "time_file" => 6e-10 })) }
      let(:tmp) { Tempfile.new('s3_rotation_temp_file') }

      before(:each) do
        s3.tempfile = tmp
        s3.register
      end

      after(:each) do
        s3.teardown
        tmp.close 
        tmp.unlink
      end

      it "raises no error when periodic rotation happen" do
        1000.times do
          expect { s3.rotate_events_log? }.not_to raise_error
        end
      end
    end
  end

  describe "#move_file_to_bucket" do
    subject { LogStash::Outputs::S3.new(minimal_settings) }

    it "should always delete the source file" do
      tmp = Stud::Temporary.file

      allow(File).to receive(:zero?).and_return(true)
      expect(File).to receive(:delete).with(tmp)

      subject.move_file_to_bucket(tmp)
    end

    it 'should not upload the file if the size of the file is zero' do
      temp_file = Stud::Temporary.file
      allow(temp_file).to receive(:zero?).and_return(true)

      expect(subject).not_to receive(:write_on_bucket)
      subject.move_file_to_bucket(temp_file)
    end

    it "should upload the file if the size > 0" do
      tmp = Stud::Temporary.file

      allow(File).to receive(:zero?).and_return(false)
      expect(subject).to receive(:write_on_bucket)

      subject.move_file_to_bucket(tmp)
    end
  end

  describe "#restore_from_crashes" do
    it "read the temp directory and upload the matching file to s3" do
      s3 = LogStash::Outputs::S3.new(minimal_settings.merge({ "temporary_directory" => "/tmp/logstash/" }))

      expect(Dir).to receive(:[]).with("/tmp/logstash/*.txt").and_return(["/tmp/logstash/01.txt"])
      expect(s3).to receive(:move_file_to_bucket_async).with("/tmp/logstash/01.txt")


      s3.restore_from_crashes
    end
  end

  describe "#receive" do
    it "should send the event through the codecs" do
      data = {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"}
      event = LogStash::Event.new(data)

      expect_any_instance_of(LogStash::Codecs::Line).to receive(:encode).with(event)

      s3 = LogStash::Outputs::S3.new(minimal_settings)
      allow(s3).to receive(:test_s3_write)
      s3.register

      s3.receive(event)
    end
  end

  describe "when rotating the temporary file" do
    before { allow(File).to receive(:delete) }

    it "doesn't skip events if using the size_file option" do
      Stud::Temporary.directory do |temporary_directory|
        size_file = rand(200..20000)
        event_count = rand(300..15000)

        config = %Q[
        input {
          generator {
            count => #{event_count}
          }
        }
        output {
          s3 {
            access_key_id => "1234"
            secret_access_key => "secret"
            size_file => #{size_file}
            codec => line
            temporary_directory => '#{temporary_directory}'
            bucket => 'testing'
          }
        }
        ]

        pipeline = LogStash::Pipeline.new(config)

        pipeline_thread = Thread.new { pipeline.run }
        sleep 0.1 while !pipeline.ready?
        pipeline_thread.join

        events_written_count = events_in_files(Dir[File.join(temporary_directory, 'ls.*.txt')])
        expect(events_written_count).to eq(event_count)
      end
    end

    it "doesn't skip events if using the time_file option", :tag => :slow do
      Stud::Temporary.directory do |temporary_directory|
        time_file = rand(5..10)
        number_of_rotation = rand(4..10)

        config = {
          "time_file" => time_file,
          "codec" => "line",
          "temporary_directory" => temporary_directory,
          "bucket" => "testing"
        }

        s3 = LogStash::Outputs::S3.new(minimal_settings.merge(config))
        # Make the test run in seconds intead of minutes..
        allow(s3).to receive(:periodic_interval).and_return(time_file)
        s3.register

        # Force to have a few files rotation
        stop_time = Time.now + (number_of_rotation * time_file)
        event_count = 0

        event = LogStash::Event.new("message" => "Hello World")

        until Time.now > stop_time do
          s3.receive(event)
          event_count += 1
        end
        s3.teardown

        generated_files = Dir[File.join(temporary_directory, 'ls.*.txt')]

        events_written_count = events_in_files(generated_files)

        # Skew times can affect the number of rotation..
        expect(generated_files.count).to be_within(number_of_rotation).of(number_of_rotation + 1)
        expect(events_written_count).to eq(event_count)
      end
    end
  end
end
