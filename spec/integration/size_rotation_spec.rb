# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "Size rotation", :integration => true do
  include_context "setup plugin"

  let(:size_file) { batch_size.times.inject(0) { |sum, i| sum + "#{event_encoded}\n".bytesize } }
  let(:options) { main_options.merge({ "rotation_strategy" => "size" }) }
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
  let(:number_of_files) { number_of_events / batch_size }

  before do
    clean_remote_files(prefix)
    subject.register
    batch.each_slice(batch_size) do |smaller_batch|
      subject.multi_receive_encoded(smaller_batch)
    end
    subject.close
  end

  it "creates a specific quantity of files" do
    expect(bucket_resource.objects(:prefix => prefix).count).to eq(number_of_files)
  end

  it "Rotates the files based on size" do
    bucket_resource.objects(:prefix => prefix).each do |f|
      expect(f.size).to eq(size_file)
    end
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
