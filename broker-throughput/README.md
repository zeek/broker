The utility program `broker-throughput` is a standalone tool to measure the
throughput of a network of Broker endpoints.

A test setup requires at least two processes: a client and a server.

### Starting the Server

The server simply creates a Broker endpoint and binds it to the given port:

```sh
broker-throughput --verbose --server :8080
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
broker-throughput --verbose -t 3 -r 1000 localhost:8080
```
