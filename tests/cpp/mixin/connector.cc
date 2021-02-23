#define SUITE mixin.connector

#include "broker/mixin/connector.hh"

#include "test.hh"

#include <string>

using namespace std::string_literals;

using namespace broker;

namespace {

using str_set = std::set<std::string>;

struct connector_mock_base {
  connector_mock_base(caf::event_based_actor* self) : self_(self) {
    // nop
  }

  virtual ~connector_mock_base() {
    // nop
  }

  auto self() {
    return self_;
  }

  auto id() {
    return self_->node();
  }

  void start_peering(const endpoint_id& remote_id, const caf::actor& hdl,
                     caf::response_promise promise) {
    promise.deliver(remote_id, hdl);
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    return {std::move(fs)...};
  }

  template <class... Ts>
  bool dispatch_to(Ts&&...) {
    CAF_FAIL("connector_mock_base::" << __func__ << " called");
    return false;
  }

  template <class... Ts>
  void unpeer(Ts&&...) {
    CAF_FAIL("connector_mock_base::" << __func__ << " called");
  }

  template <class... Ts>
  void cannot_remove_peer(Ts&&...) {
    CAF_FAIL("connector_mock_base::" << __func__ << " called");
  }

  template <class... Ts>
  void peer_unavailable(const Ts&...) {
    // nop
  }

  virtual void peer_disconnected(const endpoint_id&, const caf::actor&,
                                 const error&) {
    // nop
  }

  virtual void peer_removed(const endpoint_id&, const caf::actor&) {
    // nop
  }

  virtual caf::behavior make_behavior() {
    return {};
  }

  caf::event_based_actor* self_;
};

caf::behavior dummy_peer() {
  return {
    [](atom::get, atom::id) { return "dummy"s; },
  };
}

struct dummy_mm_state {
  size_t i = 0;
  static inline const char* name = "dummy-mm";
};

caf::behavior dummy_mm(caf::stateful_actor<dummy_mm_state>* self, size_t n,
                       caf::actor peer) {
  return {
    [=](caf::connect_atom, const std::string&, uint16_t) -> caf::message {
      if (++self->state.i >= n)
        return caf::make_message(peer.node(),
                                 caf::actor_cast<caf::strong_actor_ptr>(peer),
                                 str_set{});
      return caf::make_message(make_error(caf::sec::cannot_connect_to_node));
    },
  };
}

struct aut_state : mixin::connector<connector_mock_base> {
  using super = mixin::connector<connector_mock_base>;

  aut_state(caf::event_based_actor* self) : super(self) {
    // nop
  }
};

using aut_type = caf::stateful_actor<aut_state>;

caf::behavior aut_impl(aut_type* self) {
  return self->state.make_behavior();
}

struct fixture : test_coordinator_fixture<> {
  fixture() {
    aut = sys.spawn(aut_impl);
    peer = sys.spawn(dummy_peer);
  }

  void set_mm(size_t tries_before_success) {
    mm = sys.spawn(dummy_mm, tries_before_success, peer);
    deref<aut_type>(aut).state.cache().mm(mm);
  }

  caf::actor aut;
  caf::actor mm;
  caf::actor peer;
};

} // namespace

FIXTURE_SCOPE(connector_tests, fixture)

TEST(the connector asks the middleman for actor handles) {
  set_mm(0);
  self->send(aut, atom::peer_v, network_info{"localhost", 8080});
  expect((atom::peer, network_info), from(self).to(aut));
  expect((atom::connect, std::string, uint16_t), from(aut).to(mm));
  expect((endpoint_id, caf::strong_actor_ptr, str_set), from(mm).to(aut));
}

FIXTURE_SCOPE_END()
