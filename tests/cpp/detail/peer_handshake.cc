#define SUITE detail.peer_handshake

#include "broker/detail/peer_handshake.hh"

#include "test.hh"

#include "broker/alm/lamport_timestamp.hh"
#include "broker/alm/routing_table.hh"

using namespace broker;

namespace {

struct mock_transport_state {
  using peer_id_type = std::string;

  using message_type = int;

  using handshake_type = detail::peer_handshake<mock_transport_state>;

  using handshake_ptr_type = detail::peer_handshake_ptr<mock_transport_state>;

  mock_transport_state(caf::event_based_actor* self) : self_ptr(self) {
    handshake = caf::make_counted<handshake_type>(this);
  }

  auto self() const noexcept {
    return self_ptr;
  }

  std::string local_id() const {
    return "A";
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

  caf::stream_slot make_inbound_path(handshake_type*) {
    log.emplace_back("add input slot");
    return caf::stream_slot{123};
  }

  caf::stream_slot make_outbound_path(handshake_type*) {
    log.emplace_back("add output slot");
    return caf::stream_slot{321};
  }

  bool finalize(handshake_type*) {
    log.emplace_back("finalize");
    return true;
  }

  template <class... Ts>
  void cleanup(Ts&&...) {
    log.emplace_back("cleanup");
  }

  template <class... Ts>
  void remove_input_path(Ts&&...) {
    log.emplace_back("remove input slot");
  }

  template <class... Ts>
  void remove_path(Ts&&...) {
    log.emplace_back("remove output slot");
  }

  auto& out() {
    return *this;
  }

  caf::event_based_actor* self_ptr;

  handshake_ptr_type handshake;

  std::function<void(mock_transport_state&)> f;

  std::vector<std::string> log;
};

using aut_type = caf::stateful_actor<mock_transport_state>;

using peer_id_type = std::string;

using fsm = detail::peer_handshake<mock_transport_state>::fsm;

struct fixture : time_aware_fixture<fixture, test_coordinator_fixture<>> {
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
    inject((atom::run), to(aut).with(atom::run_v));
  }

  auto& log() {
    return aut_state().log;
  }

  bool log_includes(std::vector<std::string> lines) {
    auto in_log = [this](const std::string& line) {
      auto& log = aut_state().log;
      return std::find(log.begin(), log.end(), line) != log.end();
    };
    if (std::all_of(lines.begin(), lines.end(), in_log)) {
      return true;
    } else {
      MESSAGE("log_includes check failed for log " << aut_state().log);
      return false;
    }
  }

  bool log_excludes(std::vector<std::string> lines) {
    auto not_in_log = [this](const std::string& line) {
      auto& log = aut_state().log;
      return std::find(log.begin(), log.end(), line) == log.end();
    };
    if (std::all_of(lines.begin(), lines.end(), not_in_log)) {
      return true;
    } else {
      MESSAGE("log_excludes check failed for log " << aut_state().log);
      return false;
    }
  }
};

caf::behavior dummy_peer() {
  return {
    [](atom::peer, atom::init, peer_id_type,
       caf::actor) -> caf::result<atom::peer, atom::ok, peer_id_type> {
      return {atom::peer_v, atom::ok_v, "B"};
    },
  };
}

} // namespace

#define AUT_EXEC(stmt) aut_exec([&](mock_transport_state& state) { stmt; })

FIXTURE_SCOPE(peer_handshake_tests, fixture)

TEST(calling start_peering on the originator twice fails the handshake) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK_EQUAL(state.handshake->state(), fsm::init_state));
  MESSAGE("start_peering transitions to 'started' and sends an init message");
  AUT_EXEC(CHECK(state.handshake->originator_start_peering("B", responder)));
  AUT_EXEC(CHECK_EQUAL(state.handshake->state(), fsm::started));
  expect((atom::peer, atom::init, peer_id_type, caf::actor),
         from(aut).to(responder).with(_, _, "A", aut));
  expect((atom::peer, atom::ok, peer_id_type),
         from(responder).to(aut).with(_, _, "B"));
  AUT_EXEC(CHECK_EQUAL(state.handshake->state(), fsm::started));
  MESSAGE("calling start_peering again is an error");
  AUT_EXEC(CHECK(!state.handshake->originator_start_peering("B", responder)));
  AUT_EXEC(CHECK_EQUAL(state.handshake->state(), fsm::fail_state));
  AUT_EXEC(CHECK(state.handshake->failed()));
}

TEST(the originator creates both streams in handle_open_stream_msg) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.handshake->originator_start_peering("B", responder)));
  AUT_EXEC(CHECK(!state.handshake->has_input_slot()));
  AUT_EXEC(CHECK(!state.handshake->has_output_slot()));
  AUT_EXEC(CHECK(state.handshake->originator_handle_open_stream_msg({}, {})));
  AUT_EXEC(CHECK(state.handshake->has_input_slot()));
  AUT_EXEC(CHECK(state.handshake->has_output_slot()));
  CHECK(log_includes({"add input slot", "add output slot"}));
  CHECK(log_excludes({"finalize"}));
}

TEST(the originator closes both streams on error) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.handshake->originator_start_peering("B", responder)));
  AUT_EXEC(CHECK(state.handshake->originator_handle_open_stream_msg({}, {})));
  AUT_EXEC(CHECK(!state.handshake->originator_handle_open_stream_msg({}, {})));
  CHECK(log_includes({"add input slot", "add output slot", "remove input slot",
                      "remove output slot"}));
}

TEST(the originator updates routing table and triggers callbacks on success) {
  auto responder = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.handshake->originator_start_peering("B", responder)));
  AUT_EXEC(CHECK(state.handshake->originator_handle_open_stream_msg({}, {})));
  AUT_EXEC(CHECK(state.handshake->handle_ack_open_msg()));
  CHECK(log_includes({"add input slot", "add output slot", "finalize"}));
  CHECK(log_excludes({"remove input slot", "remove output slot"}));
}

TEST(calling start_peering on the responder twice fails the handshake) {
  auto originator = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK_EQUAL(state.handshake->state(), fsm::init_state));
  AUT_EXEC(CHECK(state.handshake->responder_start_peering("B", originator)));
  AUT_EXEC(CHECK_EQUAL(state.handshake->state(), fsm::started));
  AUT_EXEC(CHECK(!state.handshake->responder_start_peering("B", originator)));
  AUT_EXEC(CHECK_EQUAL(state.handshake->state(), fsm::fail_state));
  AUT_EXEC(CHECK(state.handshake->failed()));
}

TEST(the responder opens the output stream first) {
  auto originator = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.handshake->responder_start_peering("B", originator)));
  AUT_EXEC(CHECK_EQUAL(state.handshake->in, caf::invalid_stream_slot));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->out, caf::invalid_stream_slot));
  AUT_EXEC(CHECK(state.handshake->responder_handle_open_stream_msg({}, {})));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->in, caf::invalid_stream_slot));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->out, caf::invalid_stream_slot));
  AUT_EXEC(CHECK(state.handshake->handle_ack_open_msg()));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->in, caf::invalid_stream_slot));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->out, caf::invalid_stream_slot));
  CHECK(log_includes({"add input slot", "add output slot", "finalize"}));
  CHECK(log_excludes({"remove input slot", "remove output slot"}));
}

TEST(the responder accepts messages from the originator in any order) {
  // Same test as above, but ack_open_msg and open_stream_msg are swapped.
  auto originator = sys.spawn(dummy_peer);
  AUT_EXEC(CHECK(state.handshake->responder_start_peering("B", originator)));
  AUT_EXEC(CHECK_EQUAL(state.handshake->in, caf::invalid_stream_slot));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->out, caf::invalid_stream_slot));
  AUT_EXEC(CHECK(state.handshake->handle_ack_open_msg()));
  AUT_EXEC(CHECK_EQUAL(state.handshake->in, caf::invalid_stream_slot));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->out, caf::invalid_stream_slot));
  AUT_EXEC(CHECK(state.handshake->responder_handle_open_stream_msg({}, {})));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->in, caf::invalid_stream_slot));
  AUT_EXEC(CHECK_NOT_EQUAL(state.handshake->out, caf::invalid_stream_slot));
  CHECK(log_includes({"add input slot", "add output slot", "finalize"}));
  CHECK(log_excludes({"remove input slot", "remove output slot"}));
}

FIXTURE_SCOPE_END()
