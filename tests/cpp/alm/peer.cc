#define SUITE alm.peer

#include "broker/core_actor.hh"

#include "test.hh"

#include "broker/alm/async_transport.hh"
#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"
#include "broker/configuration.hh"
#include "broker/detail/lift.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using broker::alm::async_transport;
using broker::alm::peer;
using broker::alm::stream_transport;
using broker::detail::lift;

using namespace broker;

namespace {

using peer_id = std::string;

using message_type = generic_node_message<peer_id>;

// -- async transport ----------------------------------------------------------

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

using async_peer_actor_type = caf::stateful_actor<async_peer_actor_state>;

struct async_peer_actor {
  using type = async_peer_actor_type;

  caf::behavior operator()(async_peer_actor_type* self, peer_id id) const {
    auto& st = self->state;
    st.id(std::move(id));
    return st.make_behavior();
  }
};

// -- stream transport ---------------------------------------------------------

class stream_peer_manager
: public stream_transport<stream_peer_manager, peer_id> {
public:
  using super = stream_transport<stream_peer_manager, peer_id>;

  stream_peer_manager(caf::event_based_actor* self) : super(self) {
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

struct stream_peer_actor_state {
  caf::intrusive_ptr<stream_peer_manager> mgr;
};

using stream_peer_actor_type = caf::stateful_actor<stream_peer_actor_state>;

struct stream_peer_actor {
  using type = stream_peer_actor_type;

  caf::behavior operator()(type* self, peer_id id) const {
    auto& mgr = self->state.mgr;
    mgr = caf::make_counted<stream_peer_manager>(self);
    mgr->id(std::move(id));
    return mgr->make_behavior();
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
template<class ActorImpl>
struct fixture : test_coordinator_fixture<> {
  using peer_ids = std::vector<peer_id>;

  std::string A = "A";

  std::string B = "B";

  std::string C = "C";

  std::string D = "D";

  std::string E = "E";

  std::string F = "F";

  std::string G = "G";

  std::string H = "H";

  std::string I = "I";

  std::string J = "J";

  fixture() {
    for (auto& id : peer_ids{A, B, C, D, E, F, G, H, I, J})
      peers[id] = sys.spawn(ActorImpl{}, id);
  }

  void connect_peers() {
    std::map<peer_id, peer_ids> connections{
      {A, {B, C, J}}, {B, {A, D, E}},    {C, {A, F, G, H}}, {D, {B, I}},
      {E, {B, I}},    {F, {C, G}},       {I, {D, E, J}},    {G, {C, F, H, J}},
      {H, {C, G, J}}, {J, {A, I, G, H}},
    };
    for (auto& [id, links] : connections)
      for (auto& link : links)
        anon_send(peers[id], atom::peer::value, link, peers[link]);
    run();
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
      anon_send_exit(kvp.second, caf::exit_reason::user_shutdown);
  }

  auto& get(const peer_id& id) {
    if constexpr (std::is_same<ActorImpl, stream_peer_actor>::value)
      return *deref<typename ActorImpl::type>(peers[id]).state.mgr;
    else
      return deref<typename ActorImpl::type>(peers[id]).state;
  }

  template <class... Ts>
  auto ls(Ts... xs) {
    return std::vector<peer_id>{std::move(xs)...};
  }

  auto shortest_path(const peer_id& from, const peer_id& to) {
    return get(from).shortest_path(to);
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

// -- async transport tests ----------------------------------------------------

#define CHECK_DISTANCE(src, dst, val)                                          \
  CHECK_EQUAL(alm::distance_to(get(src).tbl(), dst), size_t{val})

FIXTURE_SCOPE(async_peer_tests, fixture<async_peer_actor>)

TEST(topologies with loops resolve to simple forwarding tables) {
  connect_peers();
  using peer_vec = std::vector<peer_id>;
  MESSAGE("after all links are connected, G subscribes to topic 'foo'");
  anon_send(peers[G], atom::subscribe::value, filter_type{topic{"foo"}});
  run();
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
  anon_send(peers[A], atom::publish::value, make_data_message("foo", 42));
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

// -- stream transport tests ---------------------------------------------------

#define CHECK_UNREACHABLE(src, dst)                                            \
  CHECK_EQUAL(alm::distance_to(get(src).tbl(), dst), nil)

FIXTURE_SCOPE(stream_peer_tests, fixture<stream_peer_actor>)

TEST(peers can revoke paths) {
  connect_peers();
  MESSAGE("after B loses its connection to E, all paths to E go through I");
  anon_send(peers[B], atom::unpeer::value, peers[E]);
  run();
  CHECK_EQUAL(shortest_path(A, E), ls(J, I, E));
  CHECK_EQUAL(shortest_path(B, E), ls(D, I, E));
  CHECK_EQUAL(shortest_path(D, E), ls(I, E));
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
  anon_send(peers[I], atom::unpeer::value, peers[E]);
  run();
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
  anon_send(peers[G], atom::subscribe::value, filter_type{topic{"foo"}});
  run();
  MESSAGE("publishing to foo on A will result in only G having the message");
  anon_send(peers[A], atom::publish::value, make_data_message("foo", 42));
  run();
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
  run();
  get(E).disable_forwarding(true);
  connect_peers();
  MESSAGE("without forwarding, E only appears as leaf node in routing tables");
  using path_type = std::vector<peer_id>;
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
