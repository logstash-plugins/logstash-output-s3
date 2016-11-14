# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "Upload current file on shutdown", :integration => true do
  include_context "setup plugin"
  let(:options) { main_options }

  let(:size_file) { 1000000 }
  let(:time_file) { 100000 }
  let(:number_of_events) { 5000 }
  let(:batch_size) { 125 }
  let(:event_encoded) { "Hello world" }
  let(:batch) do
    b = {}
    number_of_events.times do
      event = LogStash::Event.new({ "message" => event_encoded })
      b[event] = "#{event_encoded}\n"
    end
    b
  end

  before do
    clean_remote_files(prefix)
    subject.register
    subject.multi_receive_encoded(batch)
    subject.close
  end

  it "creates a specific quantity of files" do
    # Since we have really big value of time_file and size_file
    expect(bucket_resource.objects(:prefix => prefix).count).to eq(1)
  end

  it "Persists all events" do
    download_directory = Stud::Temporary.pathname

    FileUtils.rm_rf(download_directory)
    FileUtils.mkdir_p(download_directory)

    counter = 0
    bucket_resource.objects(:prefix => prefix).each do |object|
      target = File.join(download_directory, "#{counter}.txt")
      object.get(:response_target => target)
      counter += 1
    end
    expect(Dir.glob(File.join(download_directory, "**", "*.txt")).inject(0) { |sum, f| sum + IO.readlines(f).size }).to eq(number_of_events)
  end
end
