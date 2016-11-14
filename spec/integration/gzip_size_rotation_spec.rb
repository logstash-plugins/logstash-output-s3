# encoding: utf-8
require_relative "../spec_helper"
require "logstash/outputs/s3"
require "logstash/codecs/line"
require "stud/temporary"

describe "Gzip Size rotation", :integration => true do
  include_context "setup plugin"

  let(:document_size) { 20 * 1024 } # in bytes

  let(:options) do
    main_options.merge({
      "encoding" => "gzip",
      "size_file" => document_size,
      "rotation_strategy" => "size" })
  end

  let(:number_of_events) { 1_000_000 }
  let(:batch_size) { 125 }
  let(:event_encoded) { "Hello world" * 20 }
  let(:batch) do
    b = {}
    batch_size.times do
      event = LogStash::Event.new({ "message" => event_encoded })
      b[event] = "#{event_encoded}\n"
    end
    b
  end
  let(:number_of_files) { number_of_events / 50000 }

  before do
    clean_remote_files(prefix)
    subject.register
    (number_of_events/batch_size).times do
      subject.multi_receive_encoded(batch)
    end
    subject.close
  end

  it "Rotates the files based on size" do
    f = bucket_resource.objects(:prefix => prefix).first
    expect(f.size).to be_between(document_size, document_size * 2).inclusive
  end

  it "Persists all events" do
    download_directory = Stud::Temporary.pathname

    FileUtils.rm_rf(download_directory)
    FileUtils.mkdir_p(download_directory)

    counter = 0
    bucket_resource.objects(:prefix => prefix).each do |object|
      target = File.join(download_directory, "#{counter}.txt.gz")
      object.get(:response_target => target)
      counter += 1
    end

    expect(Dir.glob(File.join(download_directory, "**", "*.gz")).inject(0) do |sum, f|
      sum + Zlib::GzipReader.new(File.open(f)).readlines.size
    end).to eq(number_of_events)
  end
end
