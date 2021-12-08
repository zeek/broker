#define SUITE flooding

#include "broker/core_actor.hh"

#include "test.hh"

#include <caf/scheduled_actor/flow.hpp>

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using namespace broker;
using namespace std::literals;

namespace {

size_t max_iterations = 500000;

struct fixture : base_fixture {
  using endpoint_state = base_fixture::endpoint_state;

  std::map<char, endpoint_state> nodes;

  std::vector<caf::actor> bridges;

  fixture() {
    // The endpoint in the fixture has ID A. We use the core of the endpoint
    // accordingly for `ep.a` and spawn cores for the other IDs.
    nodes['A'] = ep_state(ep.core());
    anon_send(nodes['A'].hdl, atom::subscribe_v,
              broker::filter_type{"test/node/A"});
    run();
     for (char id = 'B'; id < 'U'; ++id) {
       auto& var = nodes[id];
       var = make_ep_state(id, filter_type{"test/node/"s + id});
     }
     if (nodes.size() != 20)
       FAIL("expected exactly 20 nodes");
    run();
  }

  ~fixture() {
    for (auto& [id, st] : nodes)
      if (id != 'A')
        caf::anon_send_exit(st.hdl, caf::exit_reason::user_shutdown);
    for (auto& hdl : bridges)
      caf::anon_send_exit(hdl, caf::exit_reason::user_shutdown);
    run();
  }

  auto& state(caf::actor hdl) {
    return deref<core_actor_type>(hdl).state;
  }

  auto& state(const endpoint_state& ep) {
    return deref<core_actor_type>(ep.hdl).state;
  }

  auto& tbl(caf::actor hdl) {
    return state(hdl).tbl();
  }

  auto& tbl(const endpoint_state& ep) {
    return state(ep).tbl();
  }

  caf::actor bridge(const endpoint_state& left, const endpoint_state& right) {
    auto res = base_fixture::bridge(left, right);
    bridges.emplace_back(res);
    return res;
  }

  // Connects all nodes in a line without connecting the ends to form a ring. A
  // connects to B, B to C, C to D, etc.
  void init_open_daisy_chain_topology() {
    for (char id = 'B'; id < 'U'; ++id)
      bridge(nodes[id - 1], nodes[id]);
    bridge(nodes['T'], nodes['A']);
  }

  // Connects all nodes in a ring.
  void init_closed_daisy_chain_topology() {
    init_open_daisy_chain_topology();
    bridge(nodes['T'], nodes['A']);
  }

  // Connects all nodes in a cluster-like. A, B, and C are connected to each
  // other in a full mesh and all other nodes are connected to A, B and C.
  // NOTE: this setup currently requires us to manually disable forwarding on
  //       the workers. Otherwise, the system drowns in path discovery messages
  //       because this topology generates thousands of available paths even
  //       though Broker would never use any path with more than two hops in
  //       practice.
  void init_cluster_topology() {
    for (char id = 'D'; id < 'U'; ++id)
      state(nodes[id]).disable_forwarding(true);
    bridge(nodes['A'], nodes['B']);
    bridge(nodes['A'], nodes['C']);
    bridge(nodes['B'], nodes['C']);
    for (char id = 'D'; id < 'U'; ++id) {
      auto& node = nodes[id];
      bridge(node, nodes['A']);
      bridge(node, nodes['B']);
      bridge(node, nodes['C']);
    }
  }

  // Runs up to max_iterations events.
  void run_limited() {
    size_t n = 0;
    while (sched.try_run_once() || sched.trigger_timeout()) {
      if (++n == max_iterations)
        FAIL("the system failed to settle down within " << n << " events");
    }
  }
};

std::optional<size_t> operator""_os(unsigned long long x) {
  return std::optional<size_t>{static_cast<size_t>(x)};
}

} // namespace <anonymous>

// This set of tests spins up a cluster using some form of topology and then
// waits until no more activity "on the wire" remains. Note that we cannot add
// stores to cluster here because they operate with heartbeat messages and thus
// constantly cause some amount of traffic to happen.

FIXTURE_SCOPE(local_tests, fixture)

TEST(open daisy chain topologies eventually reach a stable point) {
  // An open daisy chain topology, essentially a straight line: o-o-...-o-o
  init_open_daisy_chain_topology();
  run_limited();
  for (auto id = 'A'; id < 'U'; ++id) {
    CHECK_EQUAL(state(nodes[id]).tbl().size(), 19u);
    CHECK_EQUAL(state(nodes[id]).peer_filters().size(), 19u);
  }
}

TEST(closed daisy chain topologies eventually reach a stable point) {
  // A closed daisy chain topology, essentially a circle.
  init_closed_daisy_chain_topology();
  run_limited();
  for (auto id = 'A'; id < 'U'; ++id) {
    CHECK_EQUAL(state(nodes[id]).tbl().size(), 19u);
    CHECK_EQUAL(state(nodes[id]).peer_filters().size(), 19u);
  }
}

TEST(partial mesh topologies eventually reach a stable point) {
  // A topology with three nodes in the middle forming a mesh and then "outer"
  // nodes that connect all three nodes in the middle. This is basically how
  // Zeek organizes its nodes: logger, proxy and manager in the middle with the
  // workers connecting to them (but not to other workers).
  init_cluster_topology();
  run_limited();
  for (auto id = 'A'; id < 'U'; ++id) {
    CHECK_EQUAL(state(nodes[id]).tbl().size(), 19u);
    CHECK_EQUAL(state(nodes[id]).peer_filters().size(), 19u);
  }
  MESSAGE("subscribe to a new topic on F");
  anon_send(nodes['F'].hdl, atom::subscribe_v, broker::filter_type{"foo/bar"});
  run_limited();
}

FIXTURE_SCOPE_END()
