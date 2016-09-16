# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "Upload current file on shutdown", :integration => true do
  include_context "setup plugin"
  let(:stress_time) { ENV["RUNTIME"] || 10 * 60}
  let(:options) { main_options }

  let(:time_file) { 15 }
  let(:batch_size) { 125 }
  let(:event_encoded) { "Hello world" }
  let(:batch) do
    b = {}
    batch_size.times do
      event = LogStash::Event.new({ "message" => event_encoded })
      b[event] = "#{event_encoded}\n"
    end
    b
  end

  it "Persists all events" do
    started_at = Time.now
    events_sent = 0

    clean_remote_files(prefix)
    subject.register

    while Time.now - started_at < stress_time
      subject.multi_receive_encoded(batch)
      events_sent += batch_size
    end

    subject.close

    download_directory = Stud::Temporary.pathname

    FileUtils.rm_rf(download_directory)
    FileUtils.mkdir_p(download_directory)

    counter = 0
    bucket_resource.objects(:prefix => prefix).each do |object|
      target = File.join(download_directory, "#{counter}.txt")
      object.get(:response_target => target)
      counter += 1
    end
    expect(Dir.glob(File.join(download_directory, "**", "*.txt")).inject(0) { |sum, f| sum + IO.readlines(f).size }).to eq(events_sent)
  end
end
