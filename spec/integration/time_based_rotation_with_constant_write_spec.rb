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
  let(:minimum_number_of_time_rotation) { 3 }
  let(:batch_step) { (number_of_events / minimum_number_of_time_rotation).ceil }

  before do
    clean_remote_files(prefix)
    subject.register

    # simulate batch read/write
    batch.each_slice(batch_step) do |batch_time|
      batch_time.each_slice(batch_size) do |smaller_batch|
        subject.multi_receive_encoded(smaller_batch)
      end
      sleep(time_file * 2)
    end

    subject.close
  end

  it "creates multiples files" do
    # using close will upload the current file
    expect(bucket_resource.objects(:prefix => prefix).count).to be_between(minimum_number_of_time_rotation, minimum_number_of_time_rotation + 1).inclusive
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
