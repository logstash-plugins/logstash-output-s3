## 3.2.0
  - Move to the new concurrency model `:single`
  - use correct license identifier #99
  - add support for `bucket_owner_full_control` in the canned ACL #87
  - delete the test file but ignore any errors, because we actually only need to be able to write to S3. #97

## 3.1.2
  - Fix improper shutdown of output worker threads
  - improve exception handling

## 3.0.1
 - Republish all the gems under jruby.

## 3.0.0
 - Update the plugin to the version 2.0 of the plugin api, this change is required for Logstash 5.0 compatibility. See https://github.com/elastic/logstash/issues/5141

## 2.0.7
 - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 2.0.6
 - New dependency requirements for logstash-core for the 5.0 release

## 2.0.5
 - Support signature_version option for v4 S3 keys

## 2.0.4
 - Remove the `Time.now` stub in the spec, it was conflicting with other test when running inside the default plugins test #63
 - Make the spec run faster by adjusting the values of time rotation test.

## 2.0.3
 - Update deps for logstash 2.0

## 2.0.2
 - Fixes an issue when tags were defined #39

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

## 1.0.1
- Fix a synchronization issue when doing file rotation and checking the size of the current file
- Fix an issue with synchronization when shutting down the plugin and closing the current temp file
