# test_channel_perf
Test rust sync vs async channel performance
The test send 10 millions messages across a sync and an asynchronous channel and reported messages received.

./target/release/channel_benchmark

Running crossbeam_channel benchmark...
crossbeam_channel: 11008029.87 messages/sec
Running async_channel benchmark...
async_channel: 7764291.89 messages/sec
