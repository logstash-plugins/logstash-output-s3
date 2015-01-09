def delete_matching_keys_on_bucket(prefix)
  s3_object.buckets[minimal_settings["bucket"]].objects.with_prefix(prefix).each do |obj|
    obj.delete
  end
end

def key_exists_on_bucket?(key)
  s3_object.buckets[minimal_settings["bucket"]].objects[key].exists?
end

def events_in_files(files)
  files.collect { |file| File.foreach(file).count }.inject(&:+)
end

