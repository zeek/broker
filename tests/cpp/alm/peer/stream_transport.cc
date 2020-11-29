#define SUITE alm.peer.stream_transport

#include "broker/core_actor.hh"

#include "alm/peer/fixture.hh"

#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"
#include "broker/configuration.hh"
#include "broker/defaults.hh"

using broker::defaults::store::tick_interval;

using namespace broker;
using namespace broker::alm;

namespace {

// -- transport layer ----------------------------------------------------------

class stream_peer_manager : public stream_transport {
public:
  using super = stream_transport;

  stream_peer_manager(caf::event_based_actor* self) : super(self) {
    // nop
  }

  caf::actor hdl() noexcept {
    return caf::actor_cast<caf::actor>(self());
  }

  using super::ship_locally;

  void ship_locally(const data_message& msg) {
    buf.emplace_back(msg);
    super::ship_locally(msg);
  }

  std::vector<endpoint_id> shortest_path(const endpoint_id& to) {
    if (auto ptr = alm::shortest_path(tbl(), to))
      return *ptr;
    return {};
  }

  std::vector<data_message> buf;
};

struct stream_peer_actor_state {
  caf::intrusive_ptr<stream_peer_manager> mgr_ptr;

  auto& mgr() {
    return *mgr_ptr;
  }
};

class stream_peer_actor : public caf::stateful_actor<stream_peer_actor_state> {
public:
  using super = caf::stateful_actor<stream_peer_actor_state>;

  stream_peer_actor(caf::actor_config& cfg, endpoint_id id) : super(cfg) {
    auto& mgr_ptr = state.mgr_ptr;
    mgr_ptr = caf::make_counted<stream_peer_manager>(this);
    mgr_ptr->id(std::move(id));
  }

  caf::behavior make_behavior() override {
    return state.mgr_ptr->make_behavior();
  }
};

// Our topology:
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

} // namespace

#define CHECK_UNREACHABLE(src, dst) CHECK(get(src).shortest_path(dst).empty())

FIXTURE_SCOPE(stream_peer_tests, fixture<stream_peer_actor>)

TEST(peers can revoke paths) {
  connect_peers();
  MESSAGE("after B loses its connection to E, all paths to E go through I");
  anon_send(peers["B"], atom::unpeer_v, peers["E"]);
  run(tick_interval);
  CHECK_EQUAL(shortest_path(A, E), endpoint_id_list({J, I, E}));
  CHECK_EQUAL(shortest_path(B, E), endpoint_id_list({D, I, E}));
  CHECK_EQUAL(shortest_path(D, E), endpoint_id_list({I, E}));
  MESSAGE("B and E both revoked the path");
  CHECK_EQUAL(get(A).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(B).blacklist().entries.size(), 1u);
  CHECK_EQUAL(get(C).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(D).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(E).blacklist().entries.size(), 1u);
  CHECK_EQUAL(get(F).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(H).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(I).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(J).blacklist().entries.size(), 2u);
  MESSAGE("after I loses its connection to E, no paths to E remain");
  anon_send(peers["I"], atom::unpeer_v, peers["E"]);
  run(tick_interval);
  CHECK_UNREACHABLE(A, E);
  CHECK_UNREACHABLE(B, E);
  CHECK_UNREACHABLE(C, E);
  CHECK_UNREACHABLE(D, E);
  CHECK_UNREACHABLE(F, E);
  CHECK_UNREACHABLE(G, E);
  CHECK_UNREACHABLE(H, E);
  CHECK_UNREACHABLE(I, E);
  CHECK_UNREACHABLE(J, E);
  MESSAGE("blacklists contain one additional entry after I <-> E revocation");
  // Note: we skip E on purpose here.
  CHECK_EQUAL(get(A).blacklist().entries.size(), 3u);
  CHECK_EQUAL(get(B).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(C).blacklist().entries.size(), 3u);
  CHECK_EQUAL(get(D).blacklist().entries.size(), 3u);
  CHECK_EQUAL(get(F).blacklist().entries.size(), 3u);
  CHECK_EQUAL(get(H).blacklist().entries.size(), 3u);
  CHECK_EQUAL(get(I).blacklist().entries.size(), 2u);
  CHECK_EQUAL(get(J).blacklist().entries.size(), 3u);
  MESSAGE("after max-age has expired, all peers clear their blacklist");
  sched.clock().current_time += defaults::path_blacklist::max_age;
  for (auto& id : {A, B, C, D, F, H, I, J})
    get(id).age_blacklist();
  CHECK_EQUAL(get(A).blacklist().entries.size(), 0u);
  CHECK_EQUAL(get(B).blacklist().entries.size(), 0u);
  CHECK_EQUAL(get(C).blacklist().entries.size(), 0u);
  CHECK_EQUAL(get(D).blacklist().entries.size(), 0u);
  CHECK_EQUAL(get(F).blacklist().entries.size(), 0u);
  CHECK_EQUAL(get(H).blacklist().entries.size(), 0u);
  CHECK_EQUAL(get(I).blacklist().entries.size(), 0u);
  CHECK_EQUAL(get(J).blacklist().entries.size(), 0u);
}

TEST(only receivers forward messages locally) {
  connect_peers();
  MESSAGE("after all links are connected, G subscribes to topic 'foo'");
  anon_send(peers["G"], atom::subscribe_v, filter_type{topic{"foo"}});
  run(tick_interval);
  MESSAGE("publishing to foo on A will result in only G having the message");
  anon_send(peers["A"], atom::publish_v, make_data_message("foo", 42));
  run(tick_interval);
  CHECK_EQUAL(get(A).buf.size(), 0u);
  CHECK_EQUAL(get(B).buf.size(), 0u);
  CHECK_EQUAL(get(C).buf.size(), 0u);
  CHECK_EQUAL(get(D).buf.size(), 0u);
  CHECK_EQUAL(get(E).buf.size(), 0u);
  CHECK_EQUAL(get(F).buf.size(), 0u);
  CHECK_EQUAL(get(G).buf.size(), 1u);
  CHECK_EQUAL(get(H).buf.size(), 0u);
  CHECK_EQUAL(get(I).buf.size(), 0u);
  CHECK_EQUAL(get(J).buf.size(), 0u);
}

TEST(disabling forwarding turns peers into leaf nodes) {
  run(tick_interval);
  get(E).disable_forwarding(true);
  connect_peers();
  MESSAGE("without forwarding, E only appears as leaf node in routing tables");
  using path_type = std::vector<endpoint_id>;
  std::vector<path_type> paths;
  for (auto& id : {A, B, C, D, F, H, I, J})
    for (auto& kvp : get(id).tbl())
      for (auto& versioned_path : kvp.second.versioned_paths)
        paths.emplace_back(versioned_path.first);
  auto predicate = [this](auto& path) {
    if (path.empty())
      return true;
    auto i = std::find(path.begin(), path.end(), E);
    return i == path.end() || i == std::prev(path.end());
  };
  CHECK(std::all_of(paths.begin(), paths.end(), predicate));
}

FIXTURE_SCOPE_END()
