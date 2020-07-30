# Broker Benchmarks

Broker ships with benchmarking tools that allow developers and users to
investigate system performance in various deployment and configuration setups.

## Clustering: `broker-cluster-benchmark`

This is the primary benchmark suite that runs Broker in a full end-to-end
deployment. Unlike real deployments, this tool allows all Broker endpoints run
in a single OS process.

### Setup and Configuration

Running `broker-cluster-benchmark` requires a cluster configuration file using
CAF's config syntax:


```sh
; comments start with a semicolon
foo = "bar"                   ; strings use double quotes
homepage = <https://zeek.org> ; URIs use angle brackets
list = [1, 2, 3]              ; Lists use  square brackets
```

The cluster config contains all participating Broker endpoints under `nodes`.
Each node must have at least an `id` (URI) and `topics` (list of strings). The
`id` is the network-wide identifier for peering. Use `local:$name` if a node
does not accept incoming connections and `tcp://$ip:$port` otherwise.

Nodes that publish data must have a `generator-file`. Nodes that wait for data
must set `num-inputs`. A minimal example file might look like this:

```sh
nodes {
  earth {
    id = <local:earth>
    peers = ["mars"]
    topics = ["/benchmark/events"]
    num-inputs = 100000
  }
  mars {
    id = <tcp://[::1]:8001>
    topics = ["/benchmark/events"]
    generator-file = "mars.dat"
    num-outputs = 100000
  }
}
```

This config file will start two nodes: `earth` and `mars`. On startup, `mars`
opens port 8001 and waits for its peers to connect while `earth` will not open
any port since it has a `local:` ID. The entry  `peers` for `earth` will cause
this node to connect to `mars` by trying to connect to `tcp://[::1]:8001`.

The generator file `mars.dat` contains previously recorded meta data from a
live system. Setting `num-outputs` causes `broker-cluster-benchmark` to emit
exactly that amount of messages. The node will ignore additional messages in
the generator file if it contains more than `num-outputs` entries or loop
through the file if it contains less entries.

### Recording Meta Data

Setting the configuration parameter `broker.recording-directory` (or setting
the environment variable `BROKER_RECORDING_DIRECTORY`) to a non-empty path
triggers Broker to record meta data such as subscriptions, peerings, and
published data at this endpoint. The meta data is about 2MB for each 1M
recorded messages (depending on the structure of the data).

Setting the configuration parameter `broker.output-generator-file-cap` (or
setting the environment variable `BROKER_OUTPUT_GENERATOR_FILE_CAP`) to an
unsigned integer limits recording to that many published messages.

An example for how to record data from a Zeek cluster simply involves adding
a line for each node in `/usr/local/zeek/etc/node.cfg` like:

```
env_vars=BROKER_RECORDING_DIRECTORY=/your/desired/path/zeek-recording-<node>
```

Where `<node>` would be replaced by the specific node name to avoid nodes
overwriting each other's data.

### Generating Config Files from Recorded Meta Data

After recording meta data for *all* Broker nodes, the tool
`broker-cluster-benchmark` can automatically generate a cluster configuration
by analyzing the recorded files. The generated config file uses the directory
names as node names and establishes the recorded peering relations.

The tool generates config files when passing the `--generate-config` option
by scanning all specified directories. For example, the following command
prints a configuration for a recorded Broker session with two endpoints:

```sh
broker-cluster-benchmark --mode=generate-config recordings/server recordings/client
```

The tool assumes the directories `server` and `client` to contain the following
files:

```
recordings/
├── client
│   ├── id.txt
│   ├── messages.dat
│   ├── peers.txt
│   └── topics.txt
└── server
    ├── id.txt
    ├── messages.dat
    ├── peers.txt
    └── topics.txt
```

The produced configuration will contain two nodes: `client` and `server`. All
other fields and peering relations are automatically generated from the file
contents. It is worth mentioning that the tool does a linear scan over all
`messages.dat` files to compute the number of expected messages in the system.
This step may take some time.

### Running the Benchmark

The tool `broker-cluster-benchmark` expects at least `-c $configFile`. Passing
`-v` also enables verbose output to get a glimpse into the program state at
runtime. When running a configuration for the first time, we strongly recommend
running in verbose mode:

```sh
broker-cluster-benchmark -c cluster.conf -v
```

Running in verbose mode prints various state messages to the console:

```sh
Peering tree (multiple roots are allowed):
mars, topics: ["/benchmark/events"]
└── earth, topics: ["/benchmark/events"]

mars starts listening at [::1]:8001
mars up and running
earth starts peering to [::1]:8001 (mars)
earth up and running
all nodes are up and running, run benchmark
earth waits for messages
mars starts publishing
... snip ...
```

Before the tool spins up all Broker endpoints, it makes sure that the
configured topology is safe to deploy:

- No loops allowed.
- Each node must set the mandatory fields `id` and `topics`.

Broker's source distribution includes a working setup to get started at
`tests/benchmark/cluster-example.zip`.

### Inspecting Generator Files

If you're unsure which topics appear in a generator file or how many messages
it contains, you can add the `dump-stats`  mode:

```sh
broker-cluster-benchmark -c cluster.conf -v --mode=dump-stats
```

In this mode, the tool only prints the contents of all generator files and then
exits. The output simply includes all generator files, which topics they contain
and how many messages they produce:

```sh
mars.dat
├── entries: 1000
|   ├── data-entries: 1000
|   └── command-entries: 0
└── topics:
    └── /benchmark/events
```

Note that the tool has to linearly scan each generator file, which may take
some time.

## Rate Testing: `broker-benchmark`

Running the rate benchmark allows users to configure varying (or even
increasing) rates to investigate system behavior at different loads.

This tool requires two processes: a client and a server.

### Starting the Server

The server simply creates a Broker endpoint and binds it to the given port:

```sh
broker-benchmark --verbose --server :8080
```

### Staring the Client

After starting the server, clients can start peering to it. The important
parameters of the benchmark are message type (`-t`) and rate (`-r`). The
message type is 1 for trivial strings, 2 for Broker vectors that resemble a
line in conn.log, or 3 for large Broker tables. The rate parameter configures
how many messages per second the client should send.

For sending 1,000 large table messages per second, the client could get started
as follows:

```sh
broker-benchmark --verbose -t 3 -r 1000 localhost:8080
```
