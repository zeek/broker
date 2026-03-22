#include "broker/internal/core_actor.hh"

#include "broker/broker-test.test.hh"
#include "broker/internal/metric_factory.hh"
#include "broker/internal/wire_format.hh"

#include "broker/detail/latch.hh"
#include "broker/event_observer.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"

#include <caf/async/blocking_consumer.hpp>
#include <caf/async/blocking_producer.hpp>
#include <caf/async/policy.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/chunk.hpp>

#include <condition_variable>
#include <deque>
#include <mutex>

namespace atom = broker::internal::atom;

using namespace broker;
using namespace std::literals;

namespace {

template <class T>
class synchronized_deque {
public:
  void push_back(T item) {
    std::unique_lock guard{mtx_};
    deq_.emplace_back(std::move(item));
    cv_.notify_all();
  }

  template <class Rep, class Period>
  std::optional<T> try_pop_front(std::chrono::duration<Rep, Period> timeout) {
    std::unique_lock guard{mtx_};
    while (deq_.empty()) {
      if (cv_.wait_for(guard, timeout) == std::cv_status::timeout) {
        return std::nullopt;
      }
    }
    auto item = std::move(deq_.front());
    deq_.pop_front();
    return item;
  }

private:
  std::mutex mtx_;
  std::condition_variable cv_;
  std::deque<T> deq_;
};

class test_event_observer : public event_observer {
public:
  void on_peer_connect(const endpoint_id& peer,
                       const network_info& info) override {
    connects_.push_back(peer);
  }

  template <class Rep, class Period>
  auto wait_for_connect(std::chrono::duration<Rep, Period> timeout) {
    return connects_.try_pop_front(timeout);
  }

  void on_peer_buffer_overflow(const endpoint_id& peer,
                               overflow_policy) override {
    overflows_.push_back(peer);
  }

  template <class Rep, class Period>
  auto wait_for_overflow(std::chrono::duration<Rep, Period> timeout) {
    return overflows_.try_pop_front(timeout);
  }

  void on_peer_disconnect(const endpoint_id& peer, const error&) override {
    disconnects_.push_back(peer);
  }

  template <class Rep, class Period>
  auto wait_for_disconnect(std::chrono::duration<Rep, Period> timeout) {
    return disconnects_.try_pop_front(timeout);
  }

  void observe(event_ptr) override {
    // nop
  }

  bool accepts(event::severity_level, event::component_type) const override {
    return false;
  }

private:
  synchronized_deque<endpoint_id> overflows_;

  synchronized_deque<endpoint_id> connects_;

  synchronized_deque<endpoint_id> disconnects_;
};

broker::integer deserialize_value(const caf::chunk& chunk) {
  envelope_ptr msg;
  broker::internal::wire_format::v1::trait trait;
  if (!trait.convert(chunk.bytes(), msg)) {
    throw std::logic_error("failed to deserialize chunk");
  }
  if (msg->type() != envelope_type::data) {
    throw std::logic_error("expected data envelope");
  }
  return msg->as_data()->value().to_integer();
}

} // namespace

// -----------------------------------------------------------------------------
// Test setup 1.
//
// Mimic a slow peer by passing an SPSC buffer to the core actor that has no
// consumer attached to it. Then, we publish messages until the SPSC buffer and
// the internal buffer in the core actor overflows.
//
// What happens when an overflow occurs depends on the overflow policy. Hence,
// we implement one test for each of the three overflow policies.
// -----------------------------------------------------------------------------

TEST(setting the disconnect overflow policy disconnects slow peers) {
  auto observer = std::make_shared<test_event_observer>();
  logger(observer);
  // Configuration for the test.
  auto opts = broker_options{};
  opts.peer_overflow_policy = overflow_policy::disconnect;
  opts.peer_buffer_size = 8;
  opts.ignore_broker_conf = true;
  // Boot up the endpoint.
  auto ep = endpoint{configuration{opts}};
  auto hdl = internal::native(ep.core());
  auto self = caf::scoped_actor{hdl->home_system()};
  // "Connect" our dummy peer.
  auto peer_id = endpoint_id::random(0x12345678);
  auto addr = network_info{"127.0.0.1", 12345};
  auto resources1 = caf::async::make_spsc_buffer_resource<caf::chunk>(2, 1);
  auto& [rd_1, wr_1] = resources1;
  auto resources2 = caf::async::make_spsc_buffer_resource<caf::chunk>(2, 1);
  auto& [rd_2, wr_2] = resources2;
  self
    ->request(hdl, caf::infinite, atom::peer_v, peer_id, addr,
              filter_type{"foo"_t}, rd_1, wr_2)
    .receive([] { MESSAGE("peer has been connected"); },
             [](const caf::error& what) {
               FAIL("failed to connect peer: " << what);
             });
  // Get blocking access to the SPSC buffers.
  auto maybe_consumer = caf::async::make_blocking_consumer(rd_2);
  REQUIRE(maybe_consumer);
  auto& consumer = *maybe_consumer;
  auto maybe_producer = caf::async::make_blocking_producer(wr_1);
  REQUIRE(maybe_producer);
  auto& producer = *maybe_producer;
  // Wait for the connect event.
  if (auto eid = observer->wait_for_connect(1s)) {
    REQUIRE_EQUAL(*eid, peer_id);
  } else {
    FAIL("expected connect event, but got none");
  }
  // Push 11 messages to the topic "foo" to trigger an overflow:
  // - 2 messages are buffered in the SPSC buffer
  // - 8 messages are buffered at the handler in the core actor
  for (int i = 0; i < 11; ++i) {
    ep.publish("foo"_t, data{i});
  }
  // Wait for the overflow event.
  if (auto eid = observer->wait_for_overflow(1s)) {
    REQUIRE_EQUAL(*eid, peer_id);
  } else {
    FAIL("expected overflow event, but got none");
  }
  // Wait for the disconnect event.
  if (auto eid = observer->wait_for_disconnect(1s)) {
    REQUIRE_EQUAL(*eid, peer_id);
  } else {
    FAIL("expected disconnect event, but got none");
  }
  // The consumer has been closed after the first two messages.
  auto msg = caf::chunk{};
  CHECK_EQUAL(consumer.pull(caf::async::delay_errors, msg),
              caf::async::read_result::ok);
  CHECK_EQUAL(deserialize_value(msg), 0);
  CHECK_EQUAL(consumer.pull(caf::async::delay_errors, msg),
              caf::async::read_result::ok);
  CHECK_EQUAL(deserialize_value(msg), 1);
  CHECK_EQUAL(consumer.pull(caf::async::delay_errors, msg),
              caf::async::read_result::abort);
}

TEST(setting the drop_newest overflow policy overrides the newest message) {
  auto observer = std::make_shared<test_event_observer>();
  logger(observer);
  // Configuration for the test.
  auto opts = broker_options{};
  opts.peer_overflow_policy = overflow_policy::drop_newest;
  opts.peer_buffer_size = 8;
  opts.ignore_broker_conf = true;
  // Boot up the endpoint.
  auto ep = endpoint{configuration{opts}};
  auto hdl = internal::native(ep.core());
  auto self = caf::scoped_actor{hdl->home_system()};
  // "Connect" our dummy peer.
  auto peer_id = endpoint_id::random(0x12345678);
  auto addr = network_info{"127.0.0.1", 12345};
  auto resources1 = caf::async::make_spsc_buffer_resource<caf::chunk>(2, 1);
  auto& [rd_1, wr_1] = resources1;
  auto resources2 = caf::async::make_spsc_buffer_resource<caf::chunk>(2, 1);
  auto& [rd_2, wr_2] = resources2;
  self
    ->request(hdl, caf::infinite, atom::peer_v, peer_id, addr,
              filter_type{"foo"_t}, rd_1, wr_2)
    .receive([] { MESSAGE("peer has been connected"); },
             [](const caf::error& what) {
               FAIL("failed to connect peer: " << what);
             });
  // Get blocking access to the SPSC buffers.
  auto maybe_consumer = caf::async::make_blocking_consumer(rd_2);
  REQUIRE(maybe_consumer);
  auto& consumer = *maybe_consumer;
  auto maybe_producer = caf::async::make_blocking_producer(wr_1);
  REQUIRE(maybe_producer);
  auto& producer = *maybe_producer;
  // Wait for the connect event.
  if (auto eid = observer->wait_for_connect(1s)) {
    REQUIRE_EQUAL(*eid, peer_id);
  } else {
    FAIL("expected connect event, but got none");
  }
  // Push 11 messages to the topic "foo" to trigger an overflow:
  // - 2 messages are buffered in the SPSC buffer
  // - 8 messages are buffered at the handler in the core actor
  for (int i = 0; i < 11; ++i) {
    ep.publish("foo"_t, data{i});
  }
  // Wait for the overflow event.
  if (auto eid = observer->wait_for_overflow(1s)) {
    REQUIRE_EQUAL(*eid, peer_id);
  } else {
    FAIL("expected overflow event, but got none");
  }
  // Drain the buffer: messages should be 0 to 8 and then 10.
  auto msg = caf::chunk{};
  for (int i = 0; i < 9; ++i) {
    CHECK_EQUAL(consumer.pull(caf::async::delay_errors, msg),
                caf::async::read_result::ok);
    CHECK_EQUAL(deserialize_value(msg), i);
  }
  CHECK_EQUAL(consumer.pull(caf::async::delay_errors, msg),
              caf::async::read_result::ok);
  CHECK_EQUAL(deserialize_value(msg), 10);
}

TEST(setting the drop_oldest overflow policy overrides the oldest message) {
  auto observer = std::make_shared<test_event_observer>();
  logger(observer);
  // Configuration for the test.
  auto opts = broker_options{};
  opts.peer_overflow_policy = overflow_policy::drop_oldest;
  opts.peer_buffer_size = 8;
  opts.ignore_broker_conf = true;
  // Boot up the endpoint.
  auto ep = endpoint{configuration{opts}};
  auto hdl = internal::native(ep.core());
  auto self = caf::scoped_actor{hdl->home_system()};
  // "Connect" our dummy peer.
  auto peer_id = endpoint_id::random(0x12345678);
  auto addr = network_info{"127.0.0.1", 12345};
  auto resources1 = caf::async::make_spsc_buffer_resource<caf::chunk>(2, 1);
  auto& [rd_1, wr_1] = resources1;
  auto resources2 = caf::async::make_spsc_buffer_resource<caf::chunk>(2, 1);
  auto& [rd_2, wr_2] = resources2;
  self
    ->request(hdl, caf::infinite, atom::peer_v, peer_id, addr,
              filter_type{"foo"_t}, rd_1, wr_2)
    .receive([] { MESSAGE("peer has been connected"); },
             [](const caf::error& what) {
               FAIL("failed to connect peer: " << what);
             });
  // Get blocking access to the SPSC buffers.
  auto maybe_consumer = caf::async::make_blocking_consumer(rd_2);
  REQUIRE(maybe_consumer);
  auto& consumer = *maybe_consumer;
  auto maybe_producer = caf::async::make_blocking_producer(wr_1);
  REQUIRE(maybe_producer);
  auto& producer = *maybe_producer;
  // Wait for the connect event.
  if (auto eid = observer->wait_for_connect(1s)) {
    REQUIRE_EQUAL(*eid, peer_id);
  } else {
    FAIL("expected connect event, but got none");
  }
  // Push 11 messages to the topic "foo" to trigger an overflow:
  // - 2 messages are buffered in the SPSC buffer
  // - 8 messages are buffered at the handler in the core actor
  for (int i = 0; i < 11; ++i) {
    ep.publish("foo"_t, data{i});
  }
  // Wait for the overflow event.
  if (auto eid = observer->wait_for_overflow(1s)) {
    REQUIRE_EQUAL(*eid, peer_id);
  } else {
    FAIL("expected overflow event, but got none");
  }
  // Drain the buffer: messages should be [0, 1] and then 3 to 10.
  auto msg = caf::chunk{};
  for (int i = 0; i < 2; ++i) {
    CHECK_EQUAL(consumer.pull(caf::async::delay_errors, msg),
                caf::async::read_result::ok);
    CHECK_EQUAL(deserialize_value(msg), i);
  }
  // 2 was the oldest message in the buffer and got dropped.
  for (int i = 3; i < 11; ++i) {
    CHECK_EQUAL(consumer.pull(caf::async::delay_errors, msg),
                caf::async::read_result::ok);
    CHECK_EQUAL(deserialize_value(msg), i);
  }
}

// -----------------------------------------------------------------------------
// Test setup 2.
//
// Subscribe to a topic with a subscriber that never pulls any messages, causing
// backpressure to the connected peer. Ultimately, the fast publisher should
// disconnect from the slow subscriber as a result.
//
// The setup involves two endpoints: one that publishes messages and one that
// has the subscriber attached to it. We only test with the disconnect overflow
// policy and detect the disconnect event by looking at the
// broker_connections metric (waiting for it to drop from 1 to 0). Once the
// publisher reports a disconnect, we stop.
//
// Note: the subscriber will detect the disconnect only after some delay once
// the subscriber starts pulling messages again. Hence, we only wait for the
// disconnect event in the publisher thread to not stall the test more than
// necessary.
// -----------------------------------------------------------------------------

template <class T>
using promise_ptr = std::shared_ptr<std::promise<T>>;

using latch_ptr = std::shared_ptr<detail::latch>;

bool await_connected(prometheus::Gauge* connections) {
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (connections->Value() < 1.0) {
    std::this_thread::sleep_for(1ms);
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
  }
  return true;
}

auto* get_connections(prometheus::Registry& registry) {
  return internal::metric_factory::core_t(registry)
    .connections_instances()
    .native;
}

bool fast_publisher(prometheus::Registry& registry, detail::latch& sync_start,
                    detail::latch& sync_stop, endpoint& ep) {
  auto guard = caf::detail::make_scope_guard([&sync_stop]() noexcept {
    // Tell the subscriber thread to stop as well if this thread stops.
    sync_stop.count_down();
  });
  auto* connections = get_connections(registry);
  if (!await_connected(connections)) {
    fprintf(stderr, "publisher timed out while waiting for the peering\n");
    abort();
  }
  sync_start.arrive_and_wait();
  auto deadline = std::chrono::steady_clock::now() + 10s;
  for (;;) {
    for (auto i = 0; i < 500; ++i) {
      ep.publish("foo"_t, data{i});
    }
    if (connections->Value() < 1.0) {
      return true;
    }
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
  }
}

void run_fast_publisher(const promise_ptr<bool>& detected_overflow,
                        uint16_t port, const latch_ptr& sync_start,
                        const latch_ptr& sync_stop) {
  auto registry = std::make_shared<prometheus::Registry>();
  configuration cfg;
  cfg.set("caf.scheduler.max-threads", 2);
  endpoint ep{std::move(cfg), registry};
  if (!ep.peer("127.0.0.1", port)) {
    fprintf(stderr, "publisher failed to peer with the subscriber\n");
    abort();
  }
  detected_overflow->set_value(
    fast_publisher(*registry, *sync_start, *sync_stop, ep));
}

void slow_subscriber(prometheus::Registry& registry, detail::latch& sync_start,
                     detail::latch& sync_stop, endpoint& ep, subscriber& sub) {
  auto* connections = get_connections(registry);
  if (!await_connected(connections)) {
    fprintf(stderr, "subscriber timed out while waiting for the peering\n");
    abort();
  }
  sync_start.arrive_and_wait();
  sync_stop.arrive_and_wait();
}

void run_slow_subscriber(const promise_ptr<uint16_t>& port,
                         const latch_ptr& sync_start,
                         const latch_ptr& sync_stop) {
  auto registry = std::make_shared<prometheus::Registry>();
  configuration cfg;
  cfg.set("caf.scheduler.max-threads", 2);
  endpoint ep{std::move(cfg), registry};
  auto sub = ep.make_subscriber({"foo"_t});
  auto listen_result = ep.listen("127.0.0.1", 0);
  if (listen_result == 0) {
    fprintf(stderr, "endpoint failed to open a port for peering\n");
    abort();
  }
  port->set_value(listen_result);
  slow_subscriber(*registry, *sync_start, *sync_stop, ep, sub);
}

TEST(slow local subscribers apply backpressure) {
  // Synchronizes both threads to start only after both endpoints see an open
  // connection.
  auto sync_start = std::make_shared<detail::latch>(2);
  // Synchronizes shutting down of the threads. The subscriber simply waits for
  // the publisher.
  auto sync_stop = std::make_shared<detail::latch>(2);
  // Captures the result of `fast_publisher`.
  auto detected_overflow = std::make_shared<std::promise<bool>>();
  // Launch the threads.
  auto port = std::make_shared<std::promise<uint16_t>>();
  auto t1 = std::thread{run_slow_subscriber, port, sync_start, sync_stop};
  auto t2 = std::thread{run_fast_publisher, detected_overflow,
                        port->get_future().get(), sync_start, sync_stop};
  // Wait for both to terminate.
  t1.join();
  t2.join();
  // Check whether the publisher detected a disconnect due to overflow.
  CHECK(detected_overflow->get_future().get());
}
