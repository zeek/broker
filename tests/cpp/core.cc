#define CAF_SUITE core
#include <caf/test/dsl.hpp>

#include "broker/broker.hh"
#include "broker/detail/aliases.hh"
#include "broker/detail/core_actor.hh"

using std::cout;
using std::endl;
using std::string;

using namespace caf;
using namespace broker;
using namespace broker::detail;

namespace {

void driver(event_based_actor* self, const actor& sink) {
  using buf_type = std::vector<element_type>;
  self->new_stream(
    // Destination.
    sink,
    // Initialize send buffer with 10 elements.
    [](buf_type& xs) {
      xs = buf_type{{"a", 0}, {"b", true}, {"a", 1}, {"a", 2}, {"b", false},
                    {"b", true}, {"a", 3}, {"b", false}, {"a", 4}, {"a", 5}};
    },
    // Get next element.
    [](buf_type& xs, downstream<element_type>& out, size_t num) {
      auto n = std::min(num, xs.size());
      for (size_t i = 0u; i < n; ++i)
        out.push(xs[i]);
      xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
    },
    // Did we reach the end?.
    [](const buf_type& xs) {
      return xs.empty();
    },
    // Handle result of the stream.
    [](expected<void>) {
      // nop
    }
  );
}

void consumer(event_based_actor* self, filter_type ts, const actor& src) {
  self->send(self * src, join_atom::value, std::move(ts));
  self->become(
    [=](const stream_type& in) {
      self->add_sink(
        // Input stream.
        in,
        // Initialize state.
        [](unit_t&) {
          // nop
        },
        // Process single element.
        [](unit_t&, element_type) {
          // nop
        },
        // Cleanup.
        [](unit_t&) {
          // nop
        }
      );
    }
  );
}

struct config : actor_system_config {
public:
  config() {
    add_message_type<element_type>("element");
  }
};

using fixture = test_coordinator_fixture<config>;

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(manual_stream_management, fixture)

CAF_TEST(two_peers) {
  // Spawn core actors.
  auto core1 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core2 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  sched.run();
  // Connect a consumer (leaf) to core2.
  auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
  sched.run_once();
  expect((atom_value, filter_type),
         from(leaf).to(core2).with(join_atom::value, filter_type{"b"}));
  expect((stream_msg::open), from(_).to(leaf).with(_, core2, _, _, false));
  expect((stream_msg::ack_open), from(leaf).to(core2).with(_, 5, _, false));
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer::value, actor_cast<strong_actor_ptr>(core2));
  expect((atom::peer, strong_actor_ptr), from(self).to(core1).with(_, core2));
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  expect((atom::peer, filter_type),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  expect((stream_msg::open),
         from(_).to(core1).with(
           std::make_tuple(_, filter_type{"a", "b", "c"}), core2, _, _,
           false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core2).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core2).to(core1).with(_, 5, _, false));
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1);
  sched.run_once();
  expect((stream_msg::open), from(_).to(core1).with(_, d1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(d1).with(_, 5, _, false));
  // Data flows from driver to core1 to core2 and finally to leaf.
  using buf = std::vector<element_type>;
  expect((stream_msg::batch),
         from(d1).to(core1)
         .with(5, buf{{"a", 0}, {"b", true}, {"a", 1}, {"a", 2}, {"b", false}},
               0));
  expect((stream_msg::batch),
         from(core1).to(core2)
         .with(5, buf{{"a", 0}, {"b", true}, {"a", 1}, {"a", 2}, {"b", false}},
               0));
  expect((stream_msg::batch),
         from(core2).to(leaf)
         .with(2, buf{{"b", true}, {"b", false}}, 0));
  expect((stream_msg::ack_batch), from(core2).to(core1).with(5, 0));
  expect((stream_msg::ack_batch), from(core1).to(d1).with(5, 0));
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(leaf, exit_reason::user_shutdown);
  sched.run();
}

CAF_TEST_FIXTURE_SCOPE_END()
