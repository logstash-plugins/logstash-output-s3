# encoding: utf-8
require "logstash/outputs/s3"
require "logstash/event"
require "logstash/codecs/line"
require "stud/temporary"

describe LogStash::Outputs::S3 do
  let(:prefix) { "super/%{server}" }
  let(:region) { "us-east-1" }
  let(:bucket_name) { "mybucket" }
  let(:options) { { "region" => region,
                    "bucket" => bucket_name,
                    "prefix" => prefix,
                    "restore" => false,
                    "access_key_id" => "access_key_id",
                    "secret_access_key" => "secret_access_key"
  } }
  let(:client) { Aws::S3::Client.new(stub_responses: true) }
  let(:mock_bucket) { Aws::S3::Bucket.new(:name => bucket_name, :stub_responses => true, :client => client) }
  let(:event) { LogStash::Event.new({ "server" => "overwatch" }) }
  let(:event_encoded) { "super hype" }
  let(:events_and_encoded) { { event => event_encoded } }

  subject { described_class.new(options) }

  before do
    allow(subject).to receive(:bucket_resource).and_return(mock_bucket)
    allow_any_instance_of(LogStash::Outputs::S3::WriteBucketPermissionValidator).to receive(:valid?).with(mock_bucket).and_return(true)
  end

  context "#register configuration validation" do
    describe "signature version" do
      it "should set the signature version if specified" do
        ["v2", "v4"].each do |version|
          s3 = described_class.new(options.merge({ "signature_version" => version }))
          expect(s3.full_options).to include(:s3_signature_version => version)
        end
      end

      it "should omit the option completely if not specified" do
        s3 = described_class.new(options)
        expect(s3.full_options.has_key?(:s3_signature_version)).to eql(false)
      end
    end

    describe "Access control list" do
      context "when configured" do
        ["private", "public_read", "public_read_write", "authenticated_read"].each do |permission|
          it "should return the configured ACL permissions: #{permission}" do
            s3 = described_class.new(options.merge({ "canned_acl" => permission }))
            expect(s3.upload_options).to include(:acl => permission)
          end
        end
      end

      context "when not configured" do
        it "uses private as the default" do
          s3 = described_class.new(options)
          expect(s3.upload_options).to include(:acl => "private")
        end
      end
    end

    describe "temporary directory" do
      let(:temporary_directory) { Stud::Temporary.pathname }
      let(:options) { super.merge({ "temporary_directory" => temporary_directory }) }

      it "creates the directory when it doesn't exist" do
        expect(Dir.exist?(temporary_directory)).to be_falsey
        subject.register
        expect(Dir.exist?(temporary_directory)).to be_truthy
      end

      it "raises an error if we cannot write to the directory" do
        expect(LogStash::Outputs::S3::WritableDirectoryValidator).to receive(:valid?).with(temporary_directory).and_return(false)
        expect { subject.register }.to raise_error(LogStash::ConfigurationError)
      end
    end

    it "validates the prefix" do
      s3 = described_class.new(options.merge({ "prefix" => "`no\><^" }))
      expect { s3.register }.to raise_error(LogStash::ConfigurationError)
    end

    it "allow to not validate credentials" do
      s3 = described_class.new(options.merge({"validate_credentials_on_root_bucket" => false}))
      expect_any_instance_of(LogStash::Outputs::S3::WriteBucketPermissionValidator).not_to receive(:valid?).with(any_args)
      s3.register
    end
  end

  context "receiving events" do
    before do
      subject.register
    end

    after do
      subject.close
    end

    it "uses `Event#sprintf` for the prefix" do
      expect(event).to receive(:sprintf).with(prefix).and_return("super/overwatch")
      subject.multi_receive_encoded(events_and_encoded)
    end
  end
end
