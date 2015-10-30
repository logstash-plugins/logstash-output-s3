# 1.0.2
- Explicitly require `stud/task` to make the test pass on when running them inside logstash 1.5.X fixes #36
  
# 1.0.1
- Fix a synchronization issue when doing file rotation and checking the size of the current file
- Fix an issue with synchronization when shutting down the plugin and closing the current temp file
