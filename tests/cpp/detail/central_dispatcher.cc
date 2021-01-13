#define SUITE detail.central_dispatcher

#include "broker/detail/central_dispatcher.hh"

#include "test.hh"

using namespace broker;

namespace {

struct testee_state {
  detail::central_dispatcher dispatcher;
  detail::unipath_manager_ptr in;
  detail::unipath_manager_ptr out;
  detail::unipath_manager_ptr inout;

  testee_state(caf::scheduled_actor* self) : dispatcher(self) {
    // nop
  }
};

using testee_actor = caf::stateful_actor<testee_state>;

caf::behavior testee_impl(testee_actor* self){
  using namespace broker::detail;
  return {
    [self](caf::stream<data_message> handshake) -> caf::result<void> {
      auto& st = self->state;
      if (st.in == nullptr) {
        st.in = make_data_source(&st.dispatcher);
        CHECK(!st.in->blocks_inputs());
        st.in->add_unchecked_inbound_path(handshake);
        return caf::unit;
      } else {
        return make_error(caf::sec::runtime_error, "only one input allowed");
      }
    },
    [self](atom::join, caf::actor consumer) -> caf::result<void> {
      auto& st = self->state;
      if (st.out == nullptr) {
        st.out = make_peer_manager(&st.dispatcher, nullptr);
        st.out->filter({"test"});
        st.out->add_unchecked_outbound_path<node_message>(consumer);
        st.dispatcher.add(st.out);
        return caf::unit;
      } else {
        return make_error(caf::sec::runtime_error, "only one output allowed");
      }
    },
  };
}

struct consumer_state {
  std::vector<data> buf;
};

using consumer_actor = caf::stateful_actor<consumer_state>;

caf::behavior consumer_impl(consumer_actor* self, caf::actor testee) {
  self->send(testee, atom::join_v, self);
  return {
    [self](caf::stream<node_message> in) {
      caf::attach_stream_sink(
        self, in,
        // Initialization step.
        [](caf::unit_t&) {},
        // Processing step.
        [self](caf::unit_t&, node_message x) {
          auto& buf = self->state.buf;
          REQUIRE(is_data_message(x));
          buf.emplace_back(get_data(x));
          if (buf.size() % 100 == 0)
            MESSAGE("consumed " << buf.size() << " data message");
        });
    },
  };
}

void producer_impl(consumer_actor* self, caf::actor testee) {
  caf::attach_stream_source(
    self, testee, [self](size_t& pos) { pos = 0; },
    [self](size_t& pos, caf::downstream<data_message>& out, size_t hint) {
      auto n = std::min(hint, 500 - pos);
      MESSAGE("produce " << n << " more data message");
      for (size_t i = 0; i < n; ++i)
        out.push(make_data_message("test", data{static_cast<count>(pos++)}));
    },
    [](const size_t& pos) { return pos >= 500; });
}

struct config : caf::actor_system_config {
public:
  config() {
    configuration::add_message_types(*this);
    put(content, "broker.max-pending-inputs-per-source", 16);
  }
};

struct fixture : test_coordinator_fixture<config> {
  caf::actor aut;

  fixture() {
    aut = sys.spawn(testee_impl);
  }

  ~fixture() {
    anon_send_exit(aut, caf::exit_reason::user_shutdown);
  }
};

} // namespace

FIXTURE_SCOPE(central_dispatcher_tests, fixture)

TEST(producers and consumers are connected through the central dispatcher) {
  MESSAGE("spin up consumer and producer");
  auto consumer = sys.spawn(consumer_impl, aut);
  run();
  REQUIRE(deref<testee_actor>(aut).state.out != nullptr);
  auto producer = sys.spawn(producer_impl, aut);
  MESSAGE("run, but give the consumer a low priority to force credit shortage");
  do {
    while (sched.has_job()) {
      sched.prioritize(producer) || sched.prioritize(aut);
      sched.try_run_once();
    }
  } while (sched.trigger_timeout());
  run();
  CHECK_EQUAL(deref<consumer_actor>(consumer).state.buf.size(), 500u);
  anon_send_exit(consumer, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
