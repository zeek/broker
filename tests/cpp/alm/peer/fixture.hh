#include "test.hh"

// -- fixture ------------------------------------------------------------------

// In this fixture, we're setting up this messy topology full of loops:
//
//                                     +---+
//                               +-----+ D +-----+
//                               |     +---+     |
//                               |               |
//                             +---+           +---+
//                       +-----+ B |           | I +-+
//                       |     +---+           +---+ |
//                       |       |               |   |
//                       |       |     +---+     |   |
//                       |       +-----+ E +-----+   |
//                       |             +---+         |
//                     +---+                       +---+
//                     | A +-----------------------+ J |
//                     +---+                       +---+
//                       |             +---+        | |
//                       |       +-----+ F |        | |
//                       |       |     +-+-+        | |
//                       |       |       |          | |
//                       |     +---+   +-+-+        | |
//                       +-----+ C +---+ G +--------+ |
//                             +---+   +-+-+          |
//                               |       |            |
//                               |     +-+-+          |
//                               +-----+ H +----------+
//                                     +---+
//

#define PEER_ID(var, num) broker::endpoint_id var = make_peer_id(num)
#define PEER_EXPAND(var) std::make_pair(std::string{#var}, var)

template <class Impl>
struct fixture : time_aware_fixture<fixture<Impl>, test_coordinator_fixture<>> {
  static broker::endpoint_id make_peer_id(uint8_t num) {
    std::array<uint8_t, 20> host_id;
    host_id.fill(num);
    return caf::make_node_id(num, host_id);
  }

  PEER_ID(A, 1);
  PEER_ID(B, 2);
  PEER_ID(C, 3);
  PEER_ID(D, 4);
  PEER_ID(E, 5);
  PEER_ID(F, 6);
  PEER_ID(G, 7);
  PEER_ID(H, 8);
  PEER_ID(I, 9);
  PEER_ID(J, 10);

  fixture() {
    std::vector<std::pair<std::string, broker::endpoint_id>> cfg{
      PEER_EXPAND(A), PEER_EXPAND(B), PEER_EXPAND(C), PEER_EXPAND(D),
      PEER_EXPAND(E), PEER_EXPAND(F), PEER_EXPAND(G), PEER_EXPAND(H),
      PEER_EXPAND(I), PEER_EXPAND(J),
    };
    for (const auto& [name, id] : cfg) {
      names[id] = name;
      peers[name] = this->sys.template spawn<Impl>(id);
    }
  }

  auto& get(const broker::endpoint_id& id) {
    return this->template deref<Impl>(peers[names[id]]).state.mgr();
  }

  auto& get(const caf::actor& hdl) {
    return this->template deref<Impl>(hdl).state.mgr();
  }

  auto shortest_path(const broker::endpoint_id& from,
                     const broker::endpoint_id& to) {
    return get(from).shortest_path(to);
  }

  void connect_peers() {
    std::map<std::string, std::vector<std::string>> connections{
      {"A", {"B", "C", "J"}},      {"B", {"A", "D", "E"}},
      {"C", {"A", "F", "G", "H"}}, {"D", {"B", "I"}},
      {"E", {"B", "I"}},           {"F", {"C", "G"}},
      {"I", {"D", "E", "J"}},      {"G", {"C", "F", "H", "J"}},
      {"H", {"C", "G", "J"}},      {"J", {"A", "I", "G", "H"}},
    };
    for (auto& [id, links] : connections)
      for (auto& link : links)
        caf::anon_send(peers[id], broker::atom::peer_v, get(peers[link]).id(),
                       peers[link]);
    this->run(broker::defaults::store::tick_interval);
    BROKER_ASSERT(get(A).connected_to(peers["B"]));
    BROKER_ASSERT(get(A).connected_to(peers["C"]));
    BROKER_ASSERT(get(A).connected_to(peers["J"]));
    BROKER_ASSERT(not get(A).connected_to(peers["D"]));
    BROKER_ASSERT(not get(A).connected_to(peers["E"]));
    BROKER_ASSERT(not get(A).connected_to(peers["F"]));
    BROKER_ASSERT(not get(A).connected_to(peers["G"]));
    BROKER_ASSERT(not get(A).connected_to(peers["H"]));
    BROKER_ASSERT(not get(A).connected_to(peers["I"]));
  }

  ~fixture() {
    for (auto& kvp : peers)
      caf::anon_send_exit(kvp.second, caf::exit_reason::kill);
  }

  std::map<broker::endpoint_id, std::string> names;
  std::map<std::string, caf::actor> peers;
};
