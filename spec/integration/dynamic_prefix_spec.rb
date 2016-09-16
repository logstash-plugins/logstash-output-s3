# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "Dynamic Prefix", :integration => true do
  include_context "setup plugin"

  let(:options) { main_options.merge({ "rotation_strategy" => "size" }) }
  let(:sandbox) { "test" }

  before do
    clean_remote_files(sandbox)
    subject.register
    subject.multi_receive_encoded(batch)
    subject.close
  end

  context "With field string" do
    let(:prefix) { "/#{sandbox}/%{server}/%{language}" }
    let(:batch) do
      b = {}
      e1 = LogStash::Event.new({ "server" => "es1", "language" => "ruby"})
      b[e1] = "es1-ruby"
      e2 = LogStash::Event.new({ "server" => "es2", "language" => "java"})
      b[e2] = "es2-ruby"
      b
    end

    it "creates a specific quantity of files" do
      expect(bucket_resource.objects(:prefix => sandbox).count).to eq(batch.size)
    end

    it "creates specific keys" do
      re = Regexp.union(/^es1\/ruby\/ls.s3.sashimi/, /^es2\/java\/ls.s3.sashimi/)

      bucket_resource.objects(:prefix => sandbox) do |obj|
        expect(obj.key).to match(re)
      end
    end

    it "Persists all events" do
      download_directory = Stud::Temporary.pathname

      FileUtils.rm_rf(download_directory)
      FileUtils.mkdir_p(download_directory)

      counter = 0
      bucket_resource.objects(:prefix => sandbox).each do |object|
        target = File.join(download_directory, "#{counter}.txt")
        object.get(:response_target => target)
        counter += 1
      end
      expect(Dir.glob(File.join(download_directory, "**", "*.txt")).inject(0) { |sum, f| sum + IO.readlines(f).size }).to eq(batch.size)
    end
  end

  context "with unsupported char" do
    let(:prefix) { "/#{sandbox}/%{server}/%{language}" }
    let(:batch) do
      b = {}
      e1 = LogStash::Event.new({ "server" => "e>s1", "language" => "ruby"})
      b[e1] = "es2-ruby"
      b
    end

    it "convert them to underscore" do
      re = Regexp.union(/^e_s1\/ruby\/ls.s3.sashimi/)

      bucket_resource.objects(:prefix => sandbox) do |obj|
        expect(obj.key).to match(re)
      end
    end
  end

  context "with dates" do
    let(:prefix) { "/#{sandbox}/%{+YYYY-MM-d}" }

    let(:batch) do
      b = {}
      e1 = LogStash::Event.new({ "server" => "e>s1", "language" => "ruby"})
      b[e1] = "es2-ruby"
      b
    end

    it "creates dated path" do
      re = /^#{sandbox}\/\d{4}-\d{2}-\d{2}\/ls\.s3\./
      expect(bucket_resource.objects(:prefix => sandbox).first.key).to match(re)
    end
  end
end
