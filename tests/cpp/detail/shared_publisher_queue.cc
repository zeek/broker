#define SUITE detail.shared_publisher_queue

#include "broker/detail/shared_publisher_queue.hh"

#include "test.hh"

#include <caf/flow/merge.hpp>

#include "broker/detail/flow_controller.hh"
#include "broker/detail/flow_controller_callback.hh"

using namespace broker;

namespace {

constexpr size_t num_items = 5'000;

using queue_type = detail::shared_publisher_queue<data_message>;

using queue_ptr = detail::shared_publisher_queue_ptr<data_message>;

class dummy_state : public detail::flow_controller {
public:
  using result_type = std::vector<integer>;

  using result_promise_type = std::promise<std::vector<integer>>;

  caf::event_based_actor* self;

  result_type buf;

  result_promise_type result_promise;

  bool has_source = false;

  dummy_state(caf::event_based_actor* self, result_promise_type promise)
    : self(self), result_promise(std::move(promise)) {
    // nop
  }

  caf::behavior make_behavior() {
    return {
      [this](detail::flow_controller_callback_ptr& f) { (*f)(this); },
    };
  }

  caf::scheduled_actor* ctx() override {
    return self;
  }

  void add_source(caf::flow::observable<data_message> source) override {
    if (has_source)
      FAIL("add_source called twice");
    source.for_each(
      [this](const data_message& msg) {
        if (auto i = get_if<integer>(get_data(msg)))
          buf.emplace_back(*i);
      },
      [](const caf::error& err) { FAIL("source failed: " << err); },
      [this] { result_promise.set_value(buf); });
    has_source = true;
  }

  void add_source(caf::flow::observable<command_message>) override {
    FAIL("unexpected function call");
  }

  void add_sink(caf::flow::observer<data_message>) override {
    FAIL("unexpected function call");
  }

  void add_sink(caf::flow::observer<command_message>) override {
    FAIL("unexpected function call");
  }

  caf::async::publisher<data_message>
  select_local_data(const filter_type&) override {
    FAIL("unexpected function call");
  }

  caf::async::publisher<command_message>
  select_local_commands(const filter_type&) override {
    FAIL("unexpected function call");
  }

  void add_filter(const filter_type&) override {
    // nop
  }
};

using dummy_actor = caf::stateful_actor<dummy_state>;

struct fixture : base_fixture {
  caf::actor testee;
  std::future<std::vector<integer>> f_result;

  fixture() {
    std::promise<std::vector<integer>> p;
    f_result = p.get_future();
    testee = sys.spawn<dummy_actor, caf::detached>(std::move(p));
  }

  ~fixture() {
    caf::anon_send_exit(testee, caf::exit_reason::user_shutdown);
  }
};

} // namespace

FIXTURE_SCOPE(shared_publisher_queue_tests, fixture)

TEST(a shared publisher queue connects a producer to a flow controller) {
  using promise_type = std::promise<queue_ptr>;
  auto qpromise = std::make_shared<promise_type>();
  auto f = qpromise->get_future();
  auto init = detail::make_flow_controller_callback(
    [pptr{std::move(qpromise)}](detail::flow_controller* ctrl) mutable {
      ctrl->connect(std::move(pptr));
    });
  caf::anon_send(testee, std::move(init));
  auto q = f.get();
  std::vector<integer> expected_result;
  expected_result.reserve(num_items);
  for (integer i = 0; i < static_cast<integer>(num_items); ++i) {
    q->produce("foo/bar", i);
    expected_result.emplace_back(i);
  }
  q->close();
  auto res = f_result.get();
  CHECK_EQUAL(res.size(), num_items);
  CHECK_EQUAL(res, expected_result);
}

FIXTURE_SCOPE_END()
