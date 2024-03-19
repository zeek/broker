The `broker-routing-table-benchmark` program is a standalone, self-contained
tool to compare the performance of different routing table implementations.

The routing table represents the central data structure in ALM-enabled Broker.
This data structure provides essential algorithms for the source routing as well
as for looking up shortest paths. Hence, the scalability of Broker naturally
depends on the data structures and algorithms used for representing routing
information.

In order to measure performance and scalability of the central functionality, we
have implemented a new set of micro benchmarks for Broker. The main algorithms
that Broker calls frequently are:

- `add_or_update_path`: this algorithm adds entries to the routing table or
  updates the vector timestamp for a path. Called on each update Broker receives
  from one of its peers.
- `shortest_path`: frequently called for retrieving a path to a single node.
- `generate_paths`: implements the source routing in Broker by converting a list
  of receivers to a multipath. Basically calls shortest_path for all receivers
  and merges the paths with shared hops into a single tree-like structure.
- `erase`: removes an entry from the routing table. Among the algorithms listed
  here, this algorithm is called least frequently since Broker only calls it
  when endpoints leave the overlay.

  The benchmark program exercises these algorithms with different workloads and
  with different routing table implementations.
