#define SUITE alm.peer.async_transport

#include "broker/alm/peer.hh"

#include "alm/peer/fixture.hh"

#include "broker/alm/peer.hh"
#include "broker/configuration.hh"
#include "broker/defaults.hh"

using broker::defaults::store::tick_interval;

using namespace broker;
using namespace broker::alm;

namespace {

/// A transport based on asynchronous messages. For testing only.
class async_transport : public peer {
public:
  using super = peer;

  async_transport(caf::event_based_actor* self) : peer(self) {
    // nop
  }

  void start_peering(const endpoint_id& remote_peer, caf::actor hdl) {
    BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
    if (!tbl().emplace(std::move(remote_peer), std::move(hdl)).second) {
      BROKER_INFO("start_peering ignored: already peering with "
                  << remote_peer);
      return;
    }
    self()->send(hdl, atom::peer_v, id(), filter(), timestamp());
  }

  auto handle_peering(const endpoint_id& remote_id,
                      const filter_type& remote_filter,
                      lamport_timestamp remote_timestamp) {
    BROKER_TRACE(BROKER_ARG(remote_id));
    // Check whether we already send outbound traffic to the peer. Could use
    // `BROKER_ASSERT` instead, because this mustn't get called for known peers.
    auto src = caf::actor_cast<caf::actor>(self()->current_sender());
    if (!tbl().emplace(remote_id, src).second)
      BROKER_INFO("received repeated peering request");
    // Propagate filter to peers.
    std::vector<endpoint_id> path{remote_id};
    vector_timestamp path_ts{remote_timestamp};
    handle_filter_update(path, path_ts, remote_filter);
    // Reply with our own filter.
    return caf::make_message(atom::peer_v, atom::ok_v, id(), filter(),
                             timestamp());
  }

  auto handle_peering_response(const endpoint_id& remote_id,
                               const filter_type& filter,
                               lamport_timestamp timestamp) {
    auto src = caf::actor_cast<caf::actor>(self()->current_sender());
    if (!tbl().emplace(remote_id, src).second)
      BROKER_INFO("received repeated peering response");
    // Propagate filter to peers.
    std::vector<endpoint_id> path{remote_id};
    vector_timestamp path_ts{timestamp};
    handle_filter_update(path, path_ts, filter);
  }

  void send(const caf::actor& receiver, atom::publish,
            node_message content) override {
    self()->send(receiver, atom::publish_v, std::move(content));
  }

  void send(const caf::actor& receiver, atom::subscribe,
            const endpoint_id_list& path, const vector_timestamp& ts,
            const filter_type& filter) override {
    self()->send(receiver, atom::subscribe_v, path, ts, filter);
  }

  void send(const caf::actor& receiver, atom::revoke,
            const endpoint_id_list& path, const vector_timestamp& ts,
            const endpoint_id& lost_peer, const filter_type& filter) override {
    self()->send(receiver, atom::revoke_v, path, ts, lost_peer, filter);
  }

  void ship_locally(const data_message&) override {
    // nop
  }

  void ship_locally(const command_message& msg) override {
    // nop
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    return {
      std::move(fs)...,
      lift<atom::peer>(*this, &async_transport::start_peering),
      lift<atom::peer>(*this, &async_transport::handle_peering),
      lift<atom::peer, atom::ok>(*this,
                                 &async_transport::handle_peering_response),
      lift<atom::publish>(*this, &async_transport::publish_data),
      lift<atom::publish>(*this, &async_transport::publish_command),
      lift<atom::subscribe>(*this, &async_transport::subscribe),
      lift<atom::publish>(*this, &async_transport::handle_publication),
      lift<atom::subscribe>(*this, &async_transport::handle_filter_update),
    };
  }
};

// -- transport layer ----------------------------------------------------------

class async_peer_actor_state : public async_transport {
public:
  using self_pointer = caf::event_based_actor*;

  async_peer_actor_state(self_pointer self) : async_transport(self) {
    // nop
  }

  async_peer_actor_state() = delete;

  async_peer_actor_state(const async_peer_actor_state&) = delete;

  async_peer_actor_state& operator=(const async_peer_actor_state&) = delete;

  auto& mgr() {
    return *this;
  }

  bool connected_to(const caf::actor& hdl) const noexcept {
    auto predicate = [&](const auto& kvp) { return kvp.second.hdl == hdl; };
    return std::any_of(tbl().begin(), tbl().end(), predicate);
  }

  std::vector<endpoint_id> shortest_path(const endpoint_id& to) {
    if (auto ptr = alm::shortest_path(tbl(), to))
      return *ptr;
    return {};
  }
};

class async_peer_actor : public caf::stateful_actor<async_peer_actor_state> {
public:
  using super = caf::stateful_actor<async_peer_actor_state>;

  async_peer_actor(caf::actor_config& cfg, endpoint_id id) : super(cfg) {
    state.id(std::move(id));
  }

  caf::behavior make_behavior() override {
    return state.make_behavior();
  }
};

struct message_pattern {
  topic t;
  data d;
  std::vector<endpoint_id> ps;
};

bool operator==(const message_pattern& x, const node_message& y) {
  if (!is_data_message(y))
    return false;
  const auto& dm = get_data_message(y);
  if (x.t != get_topic(dm))
    return false;
  if (x.d != get_data(dm))
    return false;
  return x.ps == get_receivers(y);
}

bool operator==(const node_message& x, const message_pattern& y) {
  return y == x;
}

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

#define CHECK_DISTANCE(src, dst, val)                                          \
  CHECK_EQUAL(alm::distance_to(get(src).tbl(), dst), size_t{val})

FIXTURE_SCOPE(async_peer_tests, fixture<async_peer_actor>)

TEST(topologies with loops resolve to simple forwarding tables) {
  connect_peers();
  MESSAGE("after all links are connected, G subscribes to topic 'foo'");
  anon_send(peers["G"], atom::subscribe_v, filter_type{topic{"foo"}});
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
  anon_send(peers["A"], atom::publish_v, make_data_message("foo", 42));
  expect((atom::publish, data_message), from(_).to(peers["A"]));
  expect((atom::publish, node_message),
         from(peers["A"]) //
           .to(peers["C"])
           .with(_, message_pattern{"foo", 42, endpoint_id_list{G}}));
  expect((atom::publish, node_message),
         from(peers["C"]) //
           .to(peers["G"])
           .with(_, message_pattern{"foo", 42, endpoint_id_list{G}}));
}

FIXTURE_SCOPE_END()
