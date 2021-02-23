#define SUITE detail.peer_handshake

#include "broker/detail/peer_handshake.hh"

#include "test.hh"

using namespace broker;

namespace {

broker::endpoint_id make_peer_id(uint8_t num) {
  std::array<uint8_t, 20> host_id;
  host_id.fill(num);
  return caf::make_node_id(num, host_id);
}

class mock_transport : public peer, public detail::unipath_manager::observer {
public:
  mock_transport(caf::event_based_actor* self) : self(self), hs(this) {
    // nop
  }

  caf::event_based_actor* this_actor() noexcept override {
    return self;
  }

  endpoint_id this_endpoint() const override {
    return make_peer_id(1);
  }

  caf::actor remote_hdl() const override {
    return hdl;
  }

  detail::peer_handshake* handshake() noexcept override {
    return &hs;
  }

  void handshake_failed(error) override {
    log.emplace_back("failed");
  }

  bool finalize_handshake() override {
    log.emplace_back("finalized");
    return true;
  }

  friend void intrusive_ptr_add_ref(mock_transport* ptr) noexcept {
    ptr->ref();
  }

  friend void intrusive_ptr_release(mock_transport* ptr) noexcept {
    ptr->deref();
  }

  caf::event_based_actor* self;

  caf::actor hdl;

  detail::peer_handshake hs;

  bool has_inbound_path = false;

  bool has_outbound_path = false;

  std::vector<std::string> log;

private:
  bool make_path(bool& flag, const char* line) {
    if (!flag) {
      log.emplace_back(line);
      flag = true;
      return true;
    } else {
      return false;
    }
  }

  void add_transport_ref() noexcept override {
    ref();
  }

  void release_transport_ref() noexcept override {
    deref();
  }
};

using mock_transport_ptr = caf::intrusive_ptr<mock_transport>;

struct mock_transport_state {
  using message_type = int;

  mock_transport_state(caf::event_based_actor* self) : self_ptr(self) {
    transport = caf::make_counted<mock_transport>(self);
  }

  auto self() const noexcept {
    return self_ptr;
  }

  endpoint_id id() const {
    return make_peer_id(1);
  }

  auto& handshake() {
    return transport->hs;
  }

  caf::behavior make_behavior() {
    return {
      [this](atom::run) {
        if (!f)
          FAIL("received run but no function was defined by the fixture");
        f(*this);
        f = nullptr;
      },
    };
  }

  bool originator_start_peering(endpoint_id peer_id, caf::actor peer_hdl) {
    transport->hdl = peer_hdl;
    return handshake().originator_start_peering(peer_id, {});
  }

  bool responder_start_peering(endpoint_id peer_id, caf::actor peer_hdl) {
    transport->hdl = peer_hdl;
    return handshake().responder_start_peering(peer_id);
  }

  mock_transport_ptr transport;

  caf::event_based_actor* self_ptr;

  std::function<void(mock_transport_state&)> f;

  static inline const char* name = "actor-under-test";
};

class aut_type : public caf::stateful_actor<mock_transport_state> {
public:
  using super = caf::stateful_actor<mock_transport_state>;

  explicit aut_type(caf::actor_config& cfg) : super(cfg) {
    // nop
  }

  caf::behavior make_behavior() override {
    return state.make_behavior();
  }
};

using fsm = detail::peer_handshake::fsm;

struct fixture : time_aware_fixture<fixture, test_coordinator_fixture<>> {
  endpoint_id A = make_peer_id(1);
  endpoint_id B = make_peer_id(2);

  caf::actor aut;

  fixture() {
    aut = sys.spawn<aut_type>();
  }

  auto& aut_state() {
    return deref<aut_type>(aut).state;
  }

  // Utility to run some piece of code inside the AUT.
  void aut_exec(std::function<void(mock_transport_state&)> f) {
    aut_state().f = std::move(f);
    inject((atom::run), from(self).to(aut).with(atom::run_v));
  }

  auto& log() {
    return aut_state().transport->log;
  }

  bool log_includes(std::vector<std::string> lines) {
    auto in_log = [this](const std::string& line) {
      auto& log = aut_state().transport->log;
      return std::find(log.begin(), log.end(), line) != log.end();
    };
    if (std::all_of(lines.begin(), lines.end(), in_log)) {
      return true;
    } else {
      MESSAGE("log_includes check failed for log " << log());
      return false;
    }
  }

  bool log_excludes(std::vector<std::string> lines) {
    auto not_in_log = [this](const std::string& line) {
      return std::find(log().begin(), log().end(), line) == log().end();
    };
    if (std::all_of(lines.begin(), lines.end(), not_in_log)) {
      return true;
    } else {
      MESSAGE("log_excludes check failed for log " << log());
      return false;
    }
  }
};

caf::behavior dummy_peer() {
  return {
    [](atom::peer, atom::init, endpoint_id,
       caf::actor) -> caf::result<atom::peer, atom::ok, endpoint_id> {
      return {atom::peer_v, atom::ok_v, make_peer_id(2)};
    },
  };
}

} // namespace

#define AUT_EXEC(stmt)                                                         \
  aut_exec([&](mock_transport_state& state) {                                  \
    [[maybe_unused]] auto& handshake = state.handshake();                      \
    [[maybe_unused]] auto& transport = *state.transport;                       \
    stmt;                                                                      \
  })

FIXTURE_SCOPE(peer_handshake_tests, fixture)

TEST(calling start_peering on the originator twice fails the handshake) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK_EQUAL(handshake.state(), fsm::init_state));
  MESSAGE("start_peering transitions to 'started' and sends an init message");
  AUT_EXEC(CHECK(state.originator_start_peering(B, responder)));
  AUT_EXEC(CHECK_EQUAL(handshake.state(), fsm::started));
  expect((atom::peer, atom::init, endpoint_id, caf::actor),
         from(aut).to(responder).with(_, _, A, aut));
  expect((atom::peer, atom::ok, endpoint_id),
         from(responder).to(aut).with(_, _, B));
  AUT_EXEC(CHECK_EQUAL(handshake.state(), fsm::started));
  MESSAGE("calling start_peering again is an error");
  AUT_EXEC(CHECK(!state.originator_start_peering(B, responder)));
  AUT_EXEC(CHECK_EQUAL(handshake.state(), fsm::fail_state));
  AUT_EXEC(CHECK(handshake.failed()));
}

TEST(the originator creates both streams in handle_open_stream_msg) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.originator_start_peering(B, responder)));
  AUT_EXEC(CHECK(!transport.has_inbound_path));
  AUT_EXEC(CHECK(!transport.has_outbound_path));
  AUT_EXEC(CHECK(handshake.originator_handle_open_stream_msg()));
  AUT_EXEC(CHECK(transport.has_inbound_path));
  AUT_EXEC(CHECK(transport.has_outbound_path));
  CHECK(log_includes({"add input slot", "add output slot"}));
  CHECK(log_excludes({"finalized"}));
}

TEST(calling handle_open_stream_msg on the originator twice is an error) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.originator_start_peering(B, responder)));
  AUT_EXEC(CHECK(handshake.originator_handle_open_stream_msg()));
  AUT_EXEC(CHECK(!handshake.originator_handle_open_stream_msg()));
  CHECK(log_includes({"add input slot", "add output slot", "failed"}));
}

TEST(the originator triggers callbacks on success) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.originator_start_peering(B, responder)));
  AUT_EXEC(CHECK(handshake.originator_handle_open_stream_msg()));
  AUT_EXEC(CHECK(handshake.handle_ack_open_msg()));
  CHECK(log_includes({"add input slot", "add output slot", "finalized"}));
  CHECK(log_excludes({"failed"}));
}

TEST(calling start_peering on the responder twice fails the handshake) {
  auto originator = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK_EQUAL(handshake.state(), fsm::init_state));
  AUT_EXEC(CHECK(state.responder_start_peering(B, originator)));
  AUT_EXEC(CHECK_EQUAL(handshake.state(), fsm::started));
  AUT_EXEC(CHECK(!state.responder_start_peering(B, originator)));
  AUT_EXEC(CHECK_EQUAL(handshake.state(), fsm::fail_state));
  AUT_EXEC(CHECK(handshake.failed()));
  CHECK(log_includes({"failed"}));
}

TEST(the responder opens the output stream first) {
  auto originator = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.responder_start_peering(B, originator)));
  AUT_EXEC(CHECK(!transport.has_inbound_path));
  AUT_EXEC(CHECK(transport.has_outbound_path));
  AUT_EXEC(CHECK(handshake.responder_handle_open_stream_msg()));
  AUT_EXEC(CHECK(transport.has_inbound_path));
  AUT_EXEC(CHECK(transport.has_outbound_path));
  AUT_EXEC(CHECK(handshake.handle_ack_open_msg()));
  AUT_EXEC(CHECK(transport.has_inbound_path));
  AUT_EXEC(CHECK(transport.has_outbound_path));
  CHECK(log_includes({"add input slot", "add output slot", "finalized"}));
  CHECK(log_excludes({"failed"}));
}

TEST(the responder accepts messages from the originator in any order) {
  // Same test as above, but ack_open_msg and open_stream_msg are swapped.
  auto originator = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.responder_start_peering(B, originator)));
  AUT_EXEC(CHECK(!transport.has_inbound_path));
  AUT_EXEC(CHECK(transport.has_outbound_path));
  AUT_EXEC(CHECK(handshake.handle_ack_open_msg()));
  AUT_EXEC(CHECK(!transport.has_inbound_path));
  AUT_EXEC(CHECK(transport.has_outbound_path));
  AUT_EXEC(CHECK(handshake.responder_handle_open_stream_msg()));
  AUT_EXEC(CHECK(transport.has_inbound_path));
  AUT_EXEC(CHECK(transport.has_outbound_path));
  CHECK(log_includes({"add input slot", "add output slot", "finalized"}));
  CHECK(log_excludes({"failed"}));
}

FIXTURE_SCOPE_END()
