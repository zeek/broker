The routing table represents the central data structure for application-level
multicast (ALM). This data structure provides essential algorithms for the
source routing as well as for looking up shortest paths. Hence, the scalability
of an ALM system naturally depends on the data structures and algorithms used
for representing routing information.

This set of micro benchmarks
measures performance and scalability of the routing table. The main algorithms
for ALM are:

- `add_or_update_path`: this algorithm adds entries to the routing table or
  updates the vector timestamp for a path. Called on each update Broker receives
  from one of its peers.
- `shortest_path`: frequently called for retrieving a path to a single node.
- `generate_paths`: implements the source routing in Broker by converting a list
  of receivers to a multipath. Basically calls `shortest_path` for all receivers
  and merges the paths with shared hops into a single tree-like structure.
- `erase`: removes an entry from the routing table. Among the algorithms listed
  here, this algorithm is called least frequently since Broker only calls it
  when endpoints leave the overlay.

The benchmark uses a simple tree topology, in order to generate a “deep” routing
table with increasing path length. In each step, we add two leaf nodes to the
previously lowest layer to gain a binary tree topology like this:

```
                             .  .  .  ..  .  .   .
                              \ /   \ / \ /   \ /
                               O --- O   O --- O
                                \   /     \   /
                                 \ /       \ /
                                  O ------- O
                                   \       /
                                    \     /
                                     \   /
                                      \ /
                                       O
```

The number of nodes that we need to store in the routing table depends on the tree depth `n`:

- `nodes(1) = 2`
- `nodes(n) = nodes(n-1) + 2^n`

Further, we connect each added pair to their parent as well as to each other in
order to increase the number of connections in the topology. The number of
connections also depends on the tree depth `n`:

- `connections(1) = 3`
- `connections(n) = connections(n-1) + 2^(n-1)*3`

The endpoints use randomized IDs and we generate a random set of receivers.
However, two runs are guaranteed to use the same setup since we always use the
same seed for initializing the random number generator.

The benchmark also compares the performance of the routing table to alternative
implementation ideas that we have considered.

*NOTE*: While the ALM classes are present in Broker, we currently do not use
them in the Broker core. Peering relations are currently managed by the user
directly and the topology formed by the peering relations must not include
loops.
