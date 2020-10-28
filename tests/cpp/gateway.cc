#define SUITE gateway

#include "broker/gateway.hh"

#include "test.hh"

#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"

using namespace broker;

namespace {

// -- actor type: peer with stream transport -----------------------------------

using peer_id = std::string;

class peer_manager : public alm::stream_transport<peer_manager, peer_id> {
public:
  using super = alm::stream_transport<peer_manager, peer_id>;

  peer_manager(caf::event_based_actor* self) : super(self) {
    // nop
  }

  const auto& id() const noexcept {
    return id_;
  }

  void id(peer_id new_id) noexcept {
    id_ = std::move(new_id);
  }

  auto hdl() noexcept {
    return caf::actor_cast<caf::actor>(self());
  }

  template <class T>
  void ship_locally(const T& msg) {
    if constexpr (std::is_same<T, data_message>::value)
      buf.emplace_back(msg);
    super::ship_locally(msg);
  }

  std::vector<peer_id> shortest_path(const peer_id& to) {
    if (auto ptr = alm::shortest_path(tbl(), to))
      return *ptr;
    return {};
  }

  std::vector<data_message> buf;

private:
  peer_id id_;
};

struct peer_actor_state {
  caf::intrusive_ptr<peer_manager> mgr;
  static inline const char* name = "peer";
};

using peer_actor_type = caf::stateful_actor<peer_actor_state>;

caf::behavior peer_actor(peer_actor_type* self, peer_id id) {
  auto& mgr = self->state.mgr;
  mgr = caf::make_counted<peer_manager>(self);
  mgr->id(std::move(id));
  return mgr->make_behavior();
}

// -- fixture ------------------------------------------------------------------


// This fixture configures the following topology:
//
//
//               internal                            external
//      <------------------------->         <------------------------->
//
//
//                      +---+                     +---+           +---+
//                +-----+ D +---+             +---+ H +-----------+ K |
//                |     +---+   |             |   +---+           +---+
//              +-+-+           |             |
//        +-----+ B |           |  +-------+  |
//        |     +---+           +--+       +--+
//      +-+-+           +---+      |       |      +---+
//      | A |           | E +------+   G   +------+ I |
//      +-+-+           +---+      |       |      +---+
//        |     +---+           +--+       +--+
//        +-----+ C |           |  +-------+  |
//              +-+-+           |             |
//                |     +---+   |             |   +---+           +---+
//                +-----+ F +---+             +---+ J +-----------+ L |
//                      +---+                     +---+           +---+
//

#define PEER_ID(id) peer_id id = #id

struct fixture : test_coordinator_fixture<> {
  using peer_ids = std::vector<peer_id>;

  PEER_ID(A);
  PEER_ID(B);
  PEER_ID(C);
  PEER_ID(D);
  PEER_ID(E);
  PEER_ID(F);
  PEER_ID(G);
  PEER_ID(H);
  PEER_ID(I);
  PEER_ID(J);
  PEER_ID(K);
  PEER_ID(L);

  fixture() {
    peers["internal"] = sys.spawn(peer_actor, G);
    peers["external"] = sys.spawn(peer_actor, G);
    gateway::setup(peers["internal"], peers["external"]);
    // Note: skips G on purpose. This ID is used by `internal` and `external`.
    for (const auto& id : {A, B, C, D, E, F, H, I, J, K, L})
      peers[id] = sys.spawn(peer_actor, id);
    run();
  }

  void connect_peers() {
    std::map<peer_id, peer_ids> connections{
      {A, {B, C}},
      {B, {A, D}},
      {C, {A, F}},
      {D, {B, "internal"}},
      {E, {"internal"}},
      {F, {C, "internal"}},
      {H, {K, "external"}},
      {I, {"external"}},
      {J, {L, "external"}},
      {K, {H}},
      {L, {J}},
    };
    auto link_id = [this](const std::string& str) {
      return (str == "internal" || str == "external") ? G : str;
    };
    for (auto& [id, links] : connections)
      for (auto& link : links)
        anon_send(peers[id], atom::peer_v, link_id(link), peers[link]);
    run();
  }

  ~fixture() {
    for (auto& kvp : peers)
      anon_send_exit(kvp.second, caf::exit_reason::kill);
  }

  auto& get(const peer_id& id) {
    return *deref<peer_actor_type>(peers[id]).state.mgr;
  }

  template <class Fun>
  void for_each_peer(Fun fun) {
    for (const auto& id : {A, B, C, D, E, F, H, I, J, K, L})
      fun(get(id));
    run();
  }

  std::map<peer_id, caf::actor> peers;
};

} // namespace

#define CHECK_DISTANCE(src, dst, val)                                          \
  CHECK_EQUAL(alm::distance_to(get(src).tbl(), dst), optional<size_t>{val})

FIXTURE_SCOPE(gateway_tests, fixture)

TEST(gateways separate internal and external domain) {
  connect_peers();
  MESSAGE("peer A only sees peers from the internal domain");
  CHECK_DISTANCE(A, B, 1);
  CHECK_DISTANCE(A, C, 1);
  CHECK_DISTANCE(A, D, 2);
  CHECK_DISTANCE(A, E, 4);
  CHECK_DISTANCE(A, F, 2);
  CHECK_DISTANCE(A, G, 3);
  CHECK_DISTANCE(A, H, nil);
  CHECK_DISTANCE(A, I, nil);
  CHECK_DISTANCE(A, J, nil);
  CHECK_DISTANCE(A, K, nil);
  CHECK_DISTANCE(A, L, nil);
  MESSAGE("peer I only sees peers from the external domain");
  CHECK_DISTANCE(I, A, nil);
  CHECK_DISTANCE(I, B, nil);
  CHECK_DISTANCE(I, C, nil);
  CHECK_DISTANCE(I, D, nil);
  CHECK_DISTANCE(I, E, nil);
  CHECK_DISTANCE(I, F, nil);
  CHECK_DISTANCE(I, G, 1);
  CHECK_DISTANCE(I, H, 2);
  CHECK_DISTANCE(I, J, 2);
  CHECK_DISTANCE(I, K, 3);
  CHECK_DISTANCE(I, L, 3);
}

TEST(gateways forward messages between the domains) {
  for_each_peer([](auto& state) { state.subscribe({"foo", "bar"}); });
  connect_peers();
  MESSAGE("publish to 'foo' on A");
  anon_send(peers[A], atom::publish_v, make_data_message("foo", 42));
  run();
  MESSAGE("publish to 'bar' on I");
  anon_send(peers[I], atom::publish_v, make_data_message("bar", 23));
  run();
  MESSAGE("all peers must have received messages from both domains");
  using log_t = std::vector<data_message>;
  log_t log{make_data_message("foo", 42), make_data_message("bar", 23)};
  CHECK_EQUAL(get(A).buf, log_t{make_data_message("bar", 23)});
  CHECK_EQUAL(get(B).buf, log);
  CHECK_EQUAL(get(C).buf, log);
  CHECK_EQUAL(get(D).buf, log);
  CHECK_EQUAL(get(E).buf, log);
  CHECK_EQUAL(get(F).buf, log);
  CHECK_EQUAL(get(H).buf, log);
  CHECK_EQUAL(get(I).buf, log_t{make_data_message("foo", 42)});
  CHECK_EQUAL(get(J).buf, log);
  CHECK_EQUAL(get(K).buf, log);
  CHECK_EQUAL(get(L).buf, log);
}

FIXTURE_SCOPE_END()
