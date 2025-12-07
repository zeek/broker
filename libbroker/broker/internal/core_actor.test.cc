#include "broker/internal/core_actor.hh"

#include "broker/broker-test.test.hh"
#include "broker/internal/wire_format.hh"

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
