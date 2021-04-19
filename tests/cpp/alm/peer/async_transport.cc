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

  void flush() override {
    // nop
  }

  template <class T>
  void dispatch_impl(const T& msg) {
    const auto& topic = get_topic(msg);
    detail::prefix_matcher matches;
    endpoint_id_list receivers;
    for (const auto& [peer, filter] : peer_filters_)
      if (matches(filter, topic))
        receivers.emplace_back(peer);
    if (!receivers.empty()) {
      std::vector<multipath> paths;
      std::vector<endpoint_id> unreachables;
      multipath::generate(receivers, tbl_, paths, unreachables);
      for (auto&& path : paths) {
        auto wrapped = node_message{msg, std::move(path)};
        if (auto row = find_row(tbl_, get_path(wrapped).head().id()))
          self()->send(row->hdl, atom::publish_v, std::move(wrapped));
        else
          BROKER_WARNING("cannot ship message: no path");
      }
      if (!unreachables.empty())
        BROKER_WARNING("cannot ship message: no path to any of"
                       << unreachables);
    }
  }

  void dispatch(const data_message& msg) override {
    dispatch_impl(msg);
  }

  void dispatch(const command_message& msg) override {
    dispatch_impl(msg);
  }

  void dispatch(node_message&& msg) override {
    auto& [content, path] = msg.unshared();
    if (path.head().id() != id()) {
      BROKER_WARNING("received a message for another node");
    } else {
      if (path.head().is_receiver())
        publish_locally(content);
      path.for_each_node([this, cptr{&content}](multipath&& nested) {
        if (auto row = find_row(tbl_, nested.head().id()); row && row->hdl)
          self()->send(row->hdl, atom::publish_v,
                       make_node_message(*cptr, std::move(nested)));
        else
          BROKER_WARNING("cannot ship message: no direct connection to"
                         << nested.head().id());
      });
    }
  }

  void publish(const caf::actor& receiver, atom::subscribe,
               const endpoint_id_list& path, const vector_timestamp& ts,
               const filter_type& filter) override {
    self()->send(receiver, atom::subscribe_v, path, ts, filter);
  }

  void publish(const caf::actor& receiver, atom::revoke,
               const endpoint_id_list& path, const vector_timestamp& ts,
               const endpoint_id& lost_peer,
               const filter_type& filter) override {
    self()->send(receiver, atom::revoke_v, path, ts, lost_peer, filter);
  }

  using super::publish_locally;

  void publish_locally(const data_message&) override {
    // nop
  }

  void publish_locally(const command_message&) override {
    // nop
  }

  caf::behavior make_behavior() override {
    using detail::lift;
    return caf::message_handler{
      [this](atom::publish, node_message& msg) {
        this->dispatch(std::move(msg));
      },
      [this](atom::publish, const data_message& msg) {
        this->dispatch(msg);
      },
      lift<atom::peer>(*this, &async_transport::start_peering),
      lift<atom::peer>(*this, &async_transport::handle_peering),
      lift<atom::peer, atom::ok>(*this,
                                 &async_transport::handle_peering_response),
      lift<atom::subscribe>(*this, &async_transport::subscribe),
      lift<atom::subscribe>(*this, &async_transport::handle_filter_update),
    }
      .or_else(super::make_behavior());
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

bool matches(const multipath& path, const std::vector<endpoint_id>& receivers) {
  auto& head = path.head();
  auto i = std::find(receivers.begin(), receivers.end(), head.id());
  if (head.is_receiver() == (i != receivers.end())) {
    auto result = true;
    path.for_each_node([&result, &receivers](const multipath& nested) {
      result &= matches(nested, receivers);
    });
    return result;
  } else {
    return false;
  }
}

bool operator==(const message_pattern& x, const node_message& y) {
  const auto& [content, path] = y.data();
  if (!is_data_message(content)) {
    return false;
  } else {
    const auto& dm = get_data_message(content);
    return x.t == get_topic(dm) && x.d == get_data(dm) && matches(path, x.ps);
  }
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
