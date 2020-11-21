#define SUITE alm.peer.async_transport

#include "broker/core_actor.hh"

#include "test.hh"

#include "broker/alm/async_transport.hh"
#include "broker/alm/peer.hh"
#include "broker/configuration.hh"
#include "broker/defaults.hh"
#include "broker/detail/lift.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using broker::alm::async_transport;
using broker::alm::peer;
using broker::defaults::store::tick_interval;
using broker::detail::lift;

using namespace broker;

namespace {

using peer_id = std::string;

using message_type = generic_node_message<peer_id>;

// -- transport layer ----------------------------------------------------------

class async_peer_actor_state
  : public async_transport<async_peer_actor_state, peer_id> {
public:
  async_peer_actor_state(caf::event_based_actor* self) : self_(self) {
    // nop
  }

  const auto& id() const noexcept {
    return id_;
  }

  void id(peer_id new_id) noexcept {
    id_ = std::move(new_id);
  }

  auto self() {
    return self_;
  }

  bool connected_to(const caf::actor& hdl) const noexcept {
    auto predicate = [&](const auto& kvp) { return kvp.second.hdl == hdl; };
    return std::any_of(tbl().begin(), tbl().end(), predicate);
  }

private:
  caf::event_based_actor* self_;
  peer_id id_;
};

class async_peer_actor : public caf::stateful_actor<async_peer_actor_state> {
public:
  using super = caf::stateful_actor<async_peer_actor_state>;

  async_peer_actor(caf::actor_config& cfg, peer_id id) : super(cfg) {
    state.id(std::move(id));
  }

  caf::behavior make_behavior() override {
    return state.make_behavior();
  }
};

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

#define PEER_ID(id) std::string id = #id

struct fixture : time_aware_fixture<fixture, test_coordinator_fixture<>> {
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

  fixture() {
    for (auto& id : peer_ids{A, B, C, D, E, F, G, H, I, J})
      peers[id] = sys.spawn<async_peer_actor>(id);
  }

  auto& get(const peer_id& id) {
    return deref<async_peer_actor>(peers[id]).state;
  }

  template <class... Ts>
  auto ls(Ts... xs) {
    return std::vector<peer_id>{std::move(xs)...};
  }

  void connect_peers() {
    std::map<peer_id, peer_ids> connections{
      {A, {B, C, J}}, {B, {A, D, E}},    {C, {A, F, G, H}}, {D, {B, I}},
      {E, {B, I}},    {F, {C, G}},       {I, {D, E, J}},    {G, {C, F, H, J}},
      {H, {C, G, J}}, {J, {A, I, G, H}},
    };
    for (auto& [id, links] : connections)
      for (auto& link : links)
        anon_send(peers[id], atom::peer_v, link, peers[link]);
    this->run(tick_interval);
    BROKER_ASSERT(get(A).connected_to(peers[B]));
    BROKER_ASSERT(get(A).connected_to(peers[C]));
    BROKER_ASSERT(get(A).connected_to(peers[J]));
    BROKER_ASSERT(not get(A).connected_to(peers[D]));
    BROKER_ASSERT(not get(A).connected_to(peers[E]));
    BROKER_ASSERT(not get(A).connected_to(peers[F]));
    BROKER_ASSERT(not get(A).connected_to(peers[G]));
    BROKER_ASSERT(not get(A).connected_to(peers[H]));
    BROKER_ASSERT(not get(A).connected_to(peers[I]));
  }

  ~fixture() {
    for (auto& kvp : peers)
      anon_send_exit(kvp.second, caf::exit_reason::kill);
  }

  std::map<peer_id, caf::actor> peers;
};

struct message_pattern {
  topic t;
  data d;
  std::vector<peer_id> ps;
};

bool operator==(const message_pattern& x, const message_type& y) {
  if (!is_data_message(y))
    return false;
  const auto& dm = get_data_message(y);
  if (x.t != get_topic(dm))
    return false;
  if (x.d != get_data(dm))
    return false;
  return x.ps == get_receivers(y);
}

bool operator==(const message_type& x, const message_pattern& y) {
  return y == x;
}

} // namespace

#define CHECK_DISTANCE(src, dst, val)                                          \
  CHECK_EQUAL(alm::distance_to(get(src).tbl(), dst), size_t{val})

FIXTURE_SCOPE(async_peer_tests, fixture)

TEST(topologies with loops resolve to simple forwarding tables) {
  connect_peers();
  using peer_vec = std::vector<peer_id>;
  MESSAGE("after all links are connected, G subscribes to topic 'foo'");
  anon_send(peers[G], atom::subscribe_v, filter_type{topic{"foo"}});
  run(tick_interval);
  MESSAGE("after the subscription, all routing tables store a distance to G");
  CHECK_DISTANCE(A, G, 2);
  CHECK_DISTANCE(B, G, 3);
  CHECK_DISTANCE(C, G, 1);
  CHECK_DISTANCE(D, G, 3);
  CHECK_DISTANCE(E, G, 3);
  CHECK_DISTANCE(F, G, 1);
  CHECK_DISTANCE(H, G, 1);
  CHECK_DISTANCE(I, G, 2);
  CHECK_DISTANCE(J, G, 1);
  MESSAGE("publishing to foo on A will send through C");
  anon_send(peers[A], atom::publish_v, make_data_message("foo", 42));
  expect((atom::publish, data_message), from(_).to(peers["A"]));
  expect((atom::publish, message_type),
         from(peers[A])
           .to(peers[C])
           .with(_, message_pattern{"foo", 42, peer_vec{G}}));
  expect((atom::publish, message_type),
         from(peers[C])
           .to(peers[G])
           .with(_, message_pattern{"foo", 42, peer_vec{G}}));
}

FIXTURE_SCOPE_END()
