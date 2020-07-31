#define SUITE mixin.data_store_manager

#include "broker/mixin/data_store_manager.hh"

#include "test.hh"

#include <caf/make_counted.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"

using broker::alm::peer;
using broker::alm::stream_transport;

using namespace broker;

namespace {

using peer_id = endpoint_id;

using message_type = generic_node_message<peer_id>;

using clone_actor_type = caf::stateful_actor<detail::clone_state>;

endpoint_id operator""_e(const char* cstr, size_t len) {
  auto res = caf::make_uri(caf::string_view{cstr, len});
  return caf::make_node_id(unbox(res));
}

class peer_manager
  : public caf::extend<stream_transport<peer_manager, peer_id>,
                       peer_manager>::with<mixin::data_store_manager> {
public:
  using super = extended_base;

  peer_manager(endpoint::clock* clock, caf::event_based_actor* self)
    : super(clock, self) {
    // nop
  }

  const auto& id() const noexcept {
    return id_;
  }

  void id(peer_id new_id) noexcept {
    id_ = std::move(new_id);
  }

  template <class T>
  void ship_locally(const T& msg) {
    if constexpr (std::is_same<T, data_message>::value)
      buf.emplace_back(msg);
    super::ship_locally(msg);
  }

  std::vector<data_message> buf;

private:
  peer_id id_;
};

struct peer_actor_state {
  caf::intrusive_ptr<peer_manager> mgr;
};

using peer_actor_type = caf::stateful_actor<peer_actor_state>;

caf::behavior peer_actor(peer_actor_type* self, endpoint::clock* clock,
                         peer_id id) {
  auto& mgr = self->state.mgr;
  mgr = caf::make_counted<peer_manager>(clock, self);
  mgr->id(std::move(id));
  return mgr->make_behavior();
}

struct fixture : test_coordinator_fixture<> {
  using peer_ids = std::vector<peer_id>;

  endpoint_id A;

  endpoint_id B;

  auto& get(const peer_id& id) {
    return *deref<peer_actor_type>(peers[id]).state.mgr;
  }

  fixture() : clock(&sys, true) {
    A = "node:a"_e;
    B = "node:b"_e;
    for (auto& id : peer_ids{A, B})
      peers[id] = sys.spawn(peer_actor, &clock, id);
    anon_send(peers[A], atom::peer_v, peer_id{B}, peers[B]);
    run();
    BROKER_ASSERT(get(A).connected_to(peers[B]));
  }

  ~fixture() {
    for (auto& kvp : peers)
      anon_send_exit(kvp.second, caf::exit_reason::user_shutdown);
    run();
  }

  endpoint::clock clock;

  std::map<peer_id, caf::actor> peers;
};

} // namespace

FIXTURE_SCOPE(data_store_manager_tests, fixture)

TEST(peers propagate new masters) {
  auto res = self->request(peers[A], caf::infinite, atom::store_v,
                           atom::master_v, atom::attach_v, "kono",
                           backend::memory, backend_options({{"foo", 4.2}}));
  consume_messages();
  MESSAGE("data store managers respond with the actor handle of the master");
  caf::actor kono_master;
  res.receive(
    [&](const caf::actor& hdl) {
      auto i = get(A).masters().find("kono");
      REQUIRE_NOT_EQUAL(i, get(A).masters().end());
      CHECK_EQUAL(i->second, hdl);
      kono_master = hdl;
    },
    [&](const caf::error& err) { FAIL(err); });
  MESSAGE("repeated attach messages return the original actor handle");
  inject((atom::store, atom::master, atom::attach, std::string, backend,
          backend_options),
         from(self).to(peers[A]).with(atom::store_v, atom::master_v,
                                      atom::attach_v, "kono", backend::memory,
                                      backend_options({{"foo", 4.2}})));
  expect((caf::actor), from(peers[A]).to(self).with(kono_master));
  MESSAGE("data store managers respond to get messages");
  inject((atom::store, atom::master, atom::get, std::string),
         from(self).to(peers[A]).with(atom::store_v, atom::master_v,
                                      atom::get_v, "kono"));
  expect((caf::actor), from(peers[A]).to(self).with(kono_master));
  MESSAGE("only node A stores a handle to the master");
  CHECK_EQUAL(get(A).masters().count("kono"), 1u);
  CHECK_EQUAL(get(B).masters().count("kono"), 0u);
  CHECK_EQUAL(get(A).clones().count("kono"), 0u);
  CHECK_EQUAL(get(B).clones().count("kono"), 0u);
  MESSAGE("node B has access to the remote master");
  CHECK_EQUAL(get(A).has_remote_master("kono"), false);
  CHECK_EQUAL(get(B).has_remote_master("kono"), true);
}

TEST(clones wait for remote masters to appear) {
  auto res
    = self->request(peers[B], caf::infinite, atom::store_v, atom::clone_v,
                    atom::attach_v, "kono", 1.0, 1.0, 1.0);
  consume_messages();
  caf::actor clone;
  res.receive(
    [&](const caf::actor& hdl) {
      clone = hdl;
      REQUIRE(clone);
      CHECK_EQUAL(deref<clone_actor_type>(clone).state.input.producer(),
                  entity_id::nil());
    },
    [&](const caf::error& err) { CHECK(err == ec::no_such_master); });
  MESSAGE("initially, no master exists and the clone waits for one to appear");
  CHECK_EQUAL(get(A).masters().count("kono"), 0u);
  CHECK_EQUAL(get(B).masters().count("kono"), 0u);
  CHECK_EQUAL(get(A).clones().count("kono"), 0u);
  CHECK_EQUAL(get(B).clones().count("kono"), 1u);
  CHECK_EQUAL(get(A).has_remote_master("kono"), false);
  CHECK_EQUAL(get(B).has_remote_master("kono"), false);
  MESSAGE("after spawning a master, the clone attaches to it eventually");
  caf::anon_send(peers[A], atom::store_v, atom::master_v, atom::attach_v,
                 "kono", backend::memory, backend_options({{"foo", 4.2}}));
  consume_messages();
  auto& state = deref<clone_actor_type>(clone).state;
  for (size_t round = 0; round < 100 && !state.has_master(); ++round) {
    trigger_timeouts();
    consume_messages();
  }
  CHECK(state.has_master());
  CHECK_NOT_EQUAL(state.input.producer(), entity_id::nil());
}

FIXTURE_SCOPE_END()
