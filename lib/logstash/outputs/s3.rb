# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "stud/temporary"
require "stud/task"
require "socket" # for Socket.gethostname
require "thread"
require "tmpdir"
require "fileutils"


# INFORMATION:
#
# This plugin batches and uploads logstash events into Amazon Simple Storage Service (Amazon S3).
# 
# Requirements: 
# * Amazon S3 Bucket and S3 Access Permissions (Typically access_key_id and secret_access_key)
# * S3 PutObject permission
# * Run logstash as superuser to establish connection
#
# S3 outputs create temporary files into "/opt/logstash/S3_temp/". If you want, you can change the path at the start of register method.
#
# S3 output files have the following format
#
# ls.s3.ip-10-228-27-95.2013-04-18T10.00.tag_hello.part0.txt
#
# ls.s3 : indicate logstash plugin s3
#
# "ip-10-228-27-95" : indicates the ip of your machine.
# "2013-04-18T10.00" : represents the time whenever you specify time_file.
# "tag_hello" : this indicates the event's tag.
# "part0" : this means if you indicate size_file then it will generate more parts if you file.size > size_file.
#           When a file is full it will be pushed to the bucket and then deleted from the temporary directory.
#           If a file is empty, it is simply deleted.  Empty files will not be pushed
#
# Crash Recovery:
# * This plugin will recover and upload temporary log files after crash/abnormal termination
#
##[Note regarding time_file and size_file] :
#
# Both time_file and size_file settings can trigger a log "file rotation"
# A log rotation pushes the current log "part" to s3 and deleted from local temporary storage.
#
## If you specify BOTH size_file and time_file then it will create file for each tag (if specified). 
## When EITHER time_file minutes have elapsed OR log file size > size_file, a log rotation is triggered.
##
## If you ONLY specify time_file but NOT file_size, one file for each tag (if specified) will be created..
## When time_file minutes elapses, a log rotation will be triggered.
#
## If you ONLY specify size_file, but NOT time_file, one files for each tag (if specified) will be created.
## When size of log file part > size_file, a log rotation will be triggered.
#
## If NEITHER size_file nor time_file is specified, ONLY one file for each tag (if specified) will be created.
## WARNING: Since no log rotation is triggered, S3 Upload will only occur when logstash restarts.
#
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
#    s3{
#      access_key_id => "crazy_key"             (required)
#      secret_access_key => "monkey_access_key" (required)
#      region => "eu-west-1"                    (optional, default = "us-east-1")
#      bucket => "boss_please_open_your_bucket" (required)
#      size_file => 2048                        (optional) - Bytes
#      time_file => 5                           (optional) - Minutes
#      format => "plain"                        (optional)
#      canned_acl => "private"                  (optional. Options are "private", "public_read", "public_read_write", "authenticated_read". Defaults to "private" )
#    }
#
class LogStash::Outputs::S3 < LogStash::Outputs::Base
  require "logstash/outputs/s3/writable_directory_validator"
  require "logstash/outputs/s3/path_validator"
  require "logstash/outputs/s3/write_bucket_permission_validator"
  require "logstash/outputs/s3/writable_directory_validator"
  require "logstash/outputs/s3/size_rotation_policy"
  require "logstash/outputs/s3/time_rotation_policy"
  require "logstash/outputs/s3/temporary_file"
  require "logstash/outputs/s3/temporary_file_factory"
  require "logstash/outputs/s3/uploader"

  include LogStash::PluginMixins::AwsConfig::V2

  config_name "s3"
  default :codec, 'line'

  concurrency :shared

  # S3 bucket
  config :bucket, :validate => :string

  # Set the size of file in bytes, this means that files on bucket when have dimension > file_size, they are stored in two or more file.
  # If you have tags then it will generate a specific size file for every tags
  ##NOTE: define size of file is the better thing, because generate a local temporary file on disk and then put it in bucket.
  config :size_file, :validate => :number, :default => 0

  # Set the time, in MINUTES, to close the current sub_time_section of bucket.
  # If you define file_size you have a number of files in consideration of the section and the current tag.
  # 0 stay all time on listerner, beware if you specific 0 and size_file 0, because you will not put the file on bucket,
  # for now the only thing this plugin can do is to put the file when logstash restart.
  config :time_file, :validate => :number, :default => 0

  ## IMPORTANT: if you use multiple instance of s3, you should specify on one of them the "restore=> true" and on the others "restore => false".
  ## This is hack for not destroy the new files after restoring the initial files.
  ## If you do not specify "restore => true" when logstash crashes or is restarted, the files are not sent into the bucket,
  ## for example if you have single Instance.
  config :restore, :validate => :boolean, :default => false

  # The S3 canned ACL to use when putting the file. Defaults to "private".
  config :canned_acl, :validate => ["private", "public_read", "public_read_write", "authenticated_read"],
         :default => "private"

  # Specifies wether or not to use S3's AES256 server side encryption. Defaults to false.
  config :server_side_encryption, :validate => :boolean, :default => false

  # Set the directory where logstash will store the tmp files before sending it to S3
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # Specify a prefix to the uploaded filename, this can simulate directories on S3.  Prefix does not require leading slash.
  config :prefix, :validate => :string, :default => ''

  # Specify how many workers to use to upload the files to S3
  config :upload_workers_count, :validate => :number, :default => 1

  # The version of the S3 signature hash to use. Normally uses the internal client default, can be explicitly
  # specified here
  config :signature_version, :validate => ['v2', 'v4']

  # Define tags to be appended to the file on the S3 bucket.
  #
  # Example:
  # tags => ["elasticsearch", "logstash", "kibana"]
  #
  # Will generate this file:
  # "ls.s3.logstash.local.2015-01-01T00.00.tag_elasticsearch.logstash.kibana.part0.txt"
  #
  config :tags, :validate => :array, :default => []

  # Specify the content encoding. Supports ("gzip"). Defaults to "none"
  config :encoding, :validate => ["none", "gzip"], :default => "none"

  def register
    unless @prefix.nil?
      if !PathValidator.valid?(prefix) 
        raise LogStash::ConfigurationError, "Prefix must not contains, #{Bucketnamevalidator::INVALID_CHARACTERS}"
      end
    end

    configure_temporary_directory
    if !WritableDirectoryValidator.valid?(@temporary_directory)
      raise LogStash::ConfigurationError, "Logstash must have the permissions to write to the temporary directory: #{@temporary_directory}"
    end

    @rotation_lock = Mutex.new

    @temp_file = create_new_temporary_file
    @rotation = rotation_strategy
    @uploader = Uploader.new(@upload_workers_count)

    # Async upload existing files after a crash
    restore_from_crash if @restore
  end

  def multi_receive(events_and_encoded)
    events_and_encoded.each do |event, encoded|
      temp_file(event).write(encoded)
    end

    rotate_file_if_needed
  end

  def close
    @rotation_lock.synchronize { @uploader.do(@tempfile) } # schedule for upload the current file
    @uploader.stop # wait until all the current uplaod are complete
  end

  # This need to be called by the event receive and
  # also by a tick
  def rotate_file_if_needed
    @rotation_lock.synchronize do
      if @rotation.required?(@temp_file)
        @uploader.do(@temp_file, :on_complete => method(&:clean_temporary_file) # TODO on delete?
        @temp_file = create_new_temporary_file
      end
    end
  end

  private
  def tempfile(event)
    @tempfile
  end
 
  def rotation_strategy
    SizeRotationPolicy.new(size_file)
  end

  def clean_temporary_file(file)
    FileUtils.rm_rf(file.path)
  end

  def create_new_temporary_file
    TemporaryFile.new
  end

  def restore_from_crash
  end

  def configure_temporary_directory
    FileUtils.mkdir_p(@temporary_directory) unless Dir.exists?(@temporary_directory)
  end
end
