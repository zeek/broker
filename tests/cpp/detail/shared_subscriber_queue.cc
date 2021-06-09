#define SUITE detail.shared_subscriber_queue

#include "broker/detail/shared_subscriber_queue.hh"

#include "test.hh"

#include <caf/flow/observable_builder.hpp>

#include "broker/detail/flow_controller.hh"
#include "broker/detail/flow_controller_callback.hh"

using namespace broker;

namespace {

constexpr size_t num_items = 5'000;

using queue_type = detail::shared_subscriber_queue<data_message>;

using queue_ptr = detail::shared_subscriber_queue_ptr<data_message>;

class dummy_state : public detail::flow_controller {
public:
  using result_type = std::vector<integer>;

  using result_promise_type = std::promise<std::vector<integer>>;

  caf::event_based_actor* self;

  result_type buf;

  result_promise_type result_promise;

  bool has_sink = false;

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

  void add_source(caf::flow::observable<data_message>) override {
    FAIL("unexpected function call");
  }

  void add_source(caf::flow::observable<command_message>) override {
    FAIL("unexpected function call");
  }

  void add_sink(caf::flow::observer<data_message> sink) override {
    if (has_sink)
      FAIL("add_sink called twice");
    self->make_observable()
      .from_callable([i{integer{0}}]() mutable { //
        return make_data_message("foo/bar", i++);
      })
      .take(num_items)
      .attach(sink);
    has_sink = true;
  }

  void add_sink(caf::flow::observer<command_message>) override {
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

FIXTURE_SCOPE(shared_subscriber_queue_tests, fixture)

TEST(a shared subscriber queue connects a consumer to a flow controller) {
  using promise_type = std::promise<queue_ptr>;
  auto qpromise = std::make_shared<promise_type>();
  auto f = qpromise->get_future();
  auto init = detail::make_flow_controller_callback(
    [pptr{std::move(qpromise)}](detail::flow_controller* ctrl) mutable {
      filter_type filter{"foo/bar"};
      ctrl->connect(std::move(pptr), filter);
    });
  caf::anon_send(testee, std::move(init));
  auto queue = f.get();
  std::vector<data_message> expected_result;
  expected_result.reserve(num_items);
  for (integer i = 0; i < static_cast<integer>(num_items); ++i)
    expected_result.emplace_back(make_data_message("foo/bar", i));
  std::vector<data_message> result;
  result.reserve(num_items);
  while (result.size() < num_items) {
    queue->await_non_empty();
    auto chunk = queue->consume_all();
    result.insert(result.end(), chunk.begin(), chunk.end());
  }
  CHECK_EQUAL(result.size(), num_items);
  CHECK_EQUAL(result, expected_result);
  try {
    queue->await_non_empty();
    auto chunk = queue->consume_all();
    FAIL("expected an exception, got: " << chunk);
  } catch (std::out_of_range& ex) {
    MESSAGE("what: " << ex.what());
  }
}

FIXTURE_SCOPE_END()
