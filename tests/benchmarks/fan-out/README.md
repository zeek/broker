The `broker-fan-out-benchmark` program is a standalone, self-contained tool to
measure the performance of a single publisher and multiple subscribers. All
Broker endpoints are created in a single process.

By default, the program has no output and simply runs until completion. The
intended use is to run the program with a tool like `time` to measure the time
it takes to send a certain number of messages to all subscribers.

To see all available options, run the program with the `--help` flag.
