#define SUITE mixin.notifier

#include "broker/mixin/notifier.hh"

#include "test.hh"

#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"

using broker::alm::peer;
using broker::alm::stream_transport;

using namespace broker;

namespace {

using peer_id = caf::node_id;

struct dummy_cache {
  template <class OnSuccess, class OnError>
  void fetch(const caf::actor&, OnSuccess f, OnError g) {
    if (enabled)
      f(network_info{"localhost", 8080});
    else
      g(make_error(caf::sec::cannot_connect_to_node));
  }

  optional<network_info> find(const caf::actor&) {
    if (enabled)
      return network_info{"localhost", 8080};
    return nil;
  }

  bool enabled = true;
};

class stream_peer_manager
  : public //
    caf::extend<stream_transport<stream_peer_manager, peer_id>,
                stream_peer_manager>:: //
    with<mixin::notifier> {
public:
  using super = extended_base;

  stream_peer_manager(caf::event_based_actor* self) : super(self) {
    // nop
  }

  const auto& id() const noexcept {
    return id_;
  }

  void id(caf::node_id new_id) noexcept {
    id_ = std::move(new_id);
  }

  auto& cache() noexcept {
    return cache_;
  }

  using super::ship_locally;

  void ship_locally(const data_message& msg) {
    if (is_internal(get_topic(msg)))
    {
      if (auto status = status_view::make(get_data(msg)))
        log.emplace_back(to_string(status.code()));
      else if (auto err = error_view::make(get_data(msg)))
        log.emplace_back(to_string(err.code()));
    }
    super::ship_locally(msg);
  }

  std::vector<std::string> log;

private:
  caf::node_id id_;
  dummy_cache cache_;
};

struct stream_peer_actor_state {
  static inline const char* name = "stream_peer_actor";
  caf::intrusive_ptr<stream_peer_manager> mgr;
  bool connected_to(const caf::actor& hdl) const noexcept {
    return mgr->connected_to(hdl);
  }
  const auto& log() const noexcept {
    return mgr->log;
  }
};

using stream_peer_actor_type = caf::stateful_actor<stream_peer_actor_state>;

caf::behavior stream_peer_actor(stream_peer_actor_type* self, caf::node_id id) {
  auto& mgr = self->state.mgr;
  mgr = caf::make_counted<stream_peer_manager>(self);
  mgr->id(std::move(id));
  return mgr->make_behavior();
}

struct subscriber_state {
  static inline const char* name = "subscriber";
  std::vector<std::string> log;
};

using subscriber_type = caf::stateful_actor<subscriber_state>;

caf::behavior subscriber(subscriber_type* self, caf::actor aut) {
  return {
    [=](atom::local, status& x) {
      if (self->current_sender() == aut)
        self->state.log.emplace_back(to_string(x.code()));
    },
  };
}

struct fixture : test_coordinator_fixture<> {
  using peer_ids = std::vector<peer_id>;

  auto make_id(caf::string_view str) {
    return caf::make_node_id(unbox(caf::make_uri(str)));
  }

  fixture() {
    id_a = make_id("test:a");
    id_b = make_id("test:b");
    for (auto& id : peer_ids{id_a, id_b})
      peers[id] = sys.spawn(stream_peer_actor, id);
    run();
  }

  ~fixture() {
    for (auto& kvp : peers)
      anon_send_exit(kvp.second, caf::exit_reason::user_shutdown);
    anon_send_exit(logger, caf::exit_reason::user_shutdown);
  }

  auto& get(const peer_id& id) {
    return deref<stream_peer_actor_type>(peers[id]).state;
  }

  auto& log(const peer_id& id) {
    return get(id).log();
  }

  template <class... Ts>
  std::vector<std::string> make_log(Ts&&... xs) {
    return {std::forward<Ts>(xs)...};
  }

  peer_id id_a;

  peer_id id_b;

  std::map<peer_id, caf::actor> peers;

  caf::actor logger;
};

} // namespace

FIXTURE_SCOPE(notifier_tests, fixture)

TEST(connect and graceful disconnect emits peer_added and peer_lost) {
  anon_send(peers[id_a], atom::peer::value, id_b, peers[id_b]);
  run();
  CHECK_EQUAL(log(id_a), make_log("endpoint_discovered", "peer_added"));
  anon_send_exit(peers[id_b], caf::exit_reason::user_shutdown);
  run();
  CHECK_EQUAL(log(id_a), make_log("endpoint_discovered", "peer_added",
                                  "peer_lost", "endpoint_unreachable"));
}

TEST(connect and ungraceful disconnect emits peer_added and peer_lost) {
  anon_send(peers[id_a], atom::peer::value, id_b, peers[id_b]);
  run();
  CHECK_EQUAL(log(id_a), make_log("endpoint_discovered", "peer_added"));
  anon_send_exit(peers[id_b], caf::exit_reason::kill);
  run();
  CHECK_EQUAL(log(id_a), make_log("endpoint_discovered", "peer_added",
                                  "peer_lost", "endpoint_unreachable"));
}

FIXTURE_SCOPE_END()
