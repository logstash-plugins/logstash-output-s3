# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "File Time rotation with constant write", :integration => true do
  include_context "setup plugin"

  let(:time_file) { 0.5 }
  let(:options) { main_options.merge({ "rotation_strategy" => "time" }) }
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
    stub_const('LogStash::Outputs::S3::PERIODIC_CHECK_INTERVAL_IN_SECONDS', 1)
    clean_remote_files(prefix)
    subject.register
    subject.multi_receive_encoded(batch)
    sleep(time_file * 3) # the periodic check should have kick int
  end

  after do
    subject.close
  end

  it "create one file" do
    # using close will upload the current file
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
