#define SUITE detail.central_dispatcher

#include "broker/detail/central_dispatcher.hh"

#include "test.hh"

#include "broker/detail/unipath_manager.hh"

using namespace broker;

namespace {

struct testee_state : public detail::central_dispatcher {
  static inline const char* name = "testee";

  caf::event_based_actor* self;
  detail::unipath_manager_ptr in;
  detail::unipath_data_sink_ptr out;

  testee_state(caf::event_based_actor* self) : self(self) {
    // nop
  }

  caf::behavior make_behavior() {
    using namespace broker::detail;
    return {
      [this](caf::stream<data_message> input) -> caf::result<void> {
        if (in == nullptr) {
          in = make_unipath_source(this, input);
          CHECK(!in->blocks_inputs());
          return caf::unit;
        } else {
          return make_error(caf::sec::runtime_error, "only one input allowed");
        }
      },
      [this](atom::join, caf::actor consumer) -> caf::result<void> {
        if (out == nullptr) {
          out = make_unipath_data_sink(this, {"test"});
          out->add_unchecked_outbound_path<data_message>(consumer);
          return caf::unit;
        } else {
          return make_error(caf::sec::runtime_error, "only one output allowed");
        }
      },
    };
  }

  auto receivers() {
    return std::vector<endpoint_id>{this_endpoint()};
  }

  void dispatch(const data_message& msg) override {
    if (out)
      out->enqueue(msg);
  }

  void dispatch(const command_message&) override {
    FAIL("expected a data_msg");
  }

  void dispatch(node_message&&) override {
    FAIL("expected a data_msg");
  }

  caf::event_based_actor* this_actor() noexcept override {
    return self;
  }

  endpoint_id this_endpoint() const override {
    return this_id;
  }

  filter_type local_filter() const override {
    return {};
  }

  alm::lamport_timestamp local_timestamp() const noexcept override {
    return {};
  }

  void flush() override {
    if (out)
      out->push();
  }

  endpoint_id this_id = endpoint_id::random(0x5EED);
};

using testee_actor = caf::stateful_actor<testee_state>;

struct consumer_state {
  static inline const char* name = "consumer";
  std::vector<data> buf;
};

using consumer_actor = caf::stateful_actor<consumer_state>;

caf::behavior consumer_impl(consumer_actor* self, caf::actor testee) {
  self->send(testee, atom::join_v, self);
  return {
    [self](caf::stream<data_message> in) {
      caf::attach_stream_sink(
        self, in,
        // Initialization step.
        [](caf::unit_t&) {},
        // Processing step.
        [self](caf::unit_t&, data_message x) {
          auto& buf = self->state.buf;
          buf.emplace_back(move_data(x));
          if (buf.size() % 100 == 0)
            MESSAGE("consumed " << buf.size() << " data message");
        });
    },
  };
}

struct producer_state {
  static inline const char* name = "producer";
};

using producer_actor = caf::stateful_actor<producer_state>;

void producer_impl(producer_actor* self, caf::actor testee) {
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
    aut = sys.spawn<testee_actor>();
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
