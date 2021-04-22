#define SUITE detail.unipath_manager

#include "broker/detail/unipath_manager.hh"

#include "test.hh"

#include "broker/alm/stream_transport.hh"
#include "broker/detail/peer_handshake.hh"

using namespace broker;

using fsm = detail::peer_handshake::fsm;

namespace {

broker::endpoint_id make_peer_id(uint8_t num) {
  std::array<uint8_t, 20> host_id;
  host_id.fill(num);
  return caf::make_node_id(num, host_id);
}

class testee_state : public alm::stream_transport {
public:
  using super = stream_transport;

  static inline const char* name = "testee";

  testee_state(caf::event_based_actor* self, endpoint_id id) : super(self) {
    super::id(std::move(id));
    mgr = detail::make_peer_manager(this, this);
  }

  void publish_locally(const data_message& msg) override {
    buf.emplace_back(msg);
    super::publish_locally(msg);
  }

  std::vector<endpoint_id> shortest_path(const endpoint_id& to) {
    if (auto ptr = alm::shortest_path(tbl(), to))
      return *ptr;
    return {};
  }

  caf::behavior make_behavior() override {
    return caf::message_handler{
      [this](atom::run) { f(*this); },
    }
      .or_else(super::make_behavior());
  }

  void make_pending(endpoint_id id) {
    pending_.emplace(id, mgr);
  }

  bool finalize_handshake(detail::peer_manager* ptr) override {
    handshake_callback_invoked = true;
    return super::finalize_handshake(ptr);
  }

  std::function<void(testee_state&)> f;

  detail::peer_manager_ptr mgr;

  std::vector<data_message> buf;

  bool handshake_callback_invoked = false;
};

using testee_actor = caf::stateful_actor<testee_state>;

struct fixture;

struct executor {
  fixture* fix;
  caf::actor hdl;
};

struct fixture : time_aware_fixture<fixture, test_coordinator_fixture<>> {
  endpoint_id orig_id;
  endpoint_id resp_id;
  caf::actor orig_hdl;
  caf::actor resp_hdl;

  fixture() {
    orig_id = make_peer_id(1);
    orig_hdl = sys.spawn<testee_actor>(orig_id);
    resp_id = make_peer_id(2);
    resp_hdl = sys.spawn<testee_actor>(resp_id);
    REQUIRE(orig_id < resp_id);
    sched.run();
    aut_exec(orig_hdl,
             [this](testee_state& state) { state.make_pending(resp_id); });
    aut_exec(resp_hdl,
             [this](testee_state& state) { state.make_pending(orig_id); });
  }

  ~fixture() {
    anon_send_exit(orig_hdl, caf::exit_reason::user_shutdown);
    anon_send_exit(resp_hdl, caf::exit_reason::user_shutdown);
  }

  auto& state(const caf::actor& hdl) {
    return deref<testee_actor>(hdl).state;
  }

  void aut_exec(caf::actor hdl, std::function<void(testee_state&)> f) {
    state(hdl).f = std::move(f);
    inject((atom::run), from(self).to(hdl).with(atom::run_v));
  }
};

template <class F>
void operator<<(executor exec, F fun) {
  auto g = [f{std::move(fun)}](testee_state& state) {
    f(state, state.mgr->handshake());
  };
  exec.fix->aut_exec(exec.hdl, std::move(g));
}

} // namespace

#define ORIG_EXEC executor{this, orig_hdl} << [&](auto& state, auto& handshake)

#define ORIG_CHECK(stmt)                                                       \
  ORIG_EXEC {                                                                  \
    CHECK(stmt);                                                               \
  }

#define ORIG_CHECK_EQ(lhs, rhs)                                                \
  ORIG_EXEC {                                                                  \
    CHECK_EQUAL(lhs, rhs);                                                     \
  }

#define RESP_EXEC executor{this, resp_hdl} << [&](auto& state, auto& handshake)

#define RESP_CHECK(stmt)                                                       \
  RESP_EXEC {                                                                  \
    CHECK(stmt);                                                               \
  }

#define RESP_CHECK_EQ(lhs, rhs)                                                \
  RESP_EXEC {                                                                  \
    CHECK_EQUAL(lhs, rhs);                                                     \
  }

FIXTURE_SCOPE(unipath_manager_tests, fixture)

TEST(calling start_peering on the originator twice fails the handshake) {
  ORIG_CHECK_EQ(handshake.state(), fsm::init_state);
  RESP_CHECK_EQ(handshake.state(), fsm::init_state);
  MESSAGE("start_peering transitions to 'started' and sends an init message");
  ORIG_CHECK(handshake.originator_start_peering(resp_id, resp_hdl, {}));
  ORIG_CHECK_EQ(handshake.state(), fsm::started);
  ORIG_CHECK_EQ(handshake.state(), fsm::started);
  MESSAGE("calling start_peering again is an error");
  ORIG_CHECK(!handshake.originator_start_peering(resp_id, resp_hdl, {}));
  ORIG_CHECK_EQ(handshake.state(), fsm::fail_state);
  ORIG_CHECK(handshake.failed());
}

TEST(the originator creates both streams in handle_open_stream_msg) {
  ORIG_CHECK(handshake.originator_start_peering(resp_id, resp_hdl, {}));
  ORIG_CHECK(!handshake.has_inbound_path());
  ORIG_CHECK(!handshake.has_outbound_path());
  expect((atom::peer, atom::init, endpoint_id, caf::actor),
         from(orig_hdl).to(resp_hdl).with(_, _, orig_id, orig_hdl));
  expect((caf::open_stream_msg), from(resp_hdl).to(orig_hdl).with(_));
  ORIG_CHECK(handshake.has_inbound_path());
  ORIG_CHECK(handshake.has_outbound_path());
}

TEST(peer managers trigger callbacks on success) {
  ORIG_CHECK(handshake.originator_start_peering(resp_id, resp_hdl, {}));
  expect((atom::peer, atom::init, endpoint_id, caf::actor),
         from(orig_hdl).to(resp_hdl).with(_, _, orig_id, orig_hdl));
  sched.run();
  ORIG_CHECK(handshake.done());
  ORIG_CHECK(state.handshake_callback_invoked);
  RESP_CHECK(handshake.done());
  RESP_CHECK(state.handshake_callback_invoked);
}

FIXTURE_SCOPE_END()
