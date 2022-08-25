#pragma once

#ifdef SUITE
#  define CAF_SUITE SUITE
#endif

#include <caf/test/bdd_dsl.hpp>

#include <caf/actor_system.hpp>
#include <caf/scheduler/test_coordinator.hpp>
#include <caf/scoped_actor.hpp>

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal/channel.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

#include <cassert>
#include <ciso646>
#include <functional>

// -- test setup macros --------------------------------------------------------

#define TEST CAF_TEST
#define FIXTURE_SCOPE CAF_TEST_FIXTURE_SCOPE
#define FIXTURE_SCOPE_END CAF_TEST_FIXTURE_SCOPE_END

// -- logging macros -----------------------------------------------------------

#define ERROR CAF_TEST_PRINT_ERROR
#define INFO CAF_TEST_PRINT_INFO
#define VERBOSE CAF_TEST_PRINT_VERBOSE

// -- macros for checking results ---------------------------------------------

#define REQUIRE_EQUAL CAF_REQUIRE_EQUAL
#define REQUIRE_NOT_EQUAL CAF_REQUIRE_NOT_EQUAL
#define REQUIRE_LESS CAF_REQUIRE_LESS
#define REQUIRE_LESS_EQUAL CAF_REQUIRE_LESS_EQUAL
#define REQUIRE_GREATER CAF_REQUIRE_GREATER
#define REQUIRE_GREATER_EQUAL CAF_REQUIRE_GREATER_EQUAL
#define CHECK_EQUAL CAF_CHECK_EQUAL
#define CHECK_NOT_EQUAL CAF_CHECK_NOT_EQUAL
#define CHECK_LESS CAF_CHECK_LESS
#define CHECK_LESS_EQUAL CAF_CHECK_LESS_OR_EQUAL
#define CHECK_GREATER CAF_CHECK_GREATER
#define CHECK_GREATER_EQUAL CAF_CHECK_GREATER_OR_EQUAL
#define CHECK_FAIL CAF_CHECK_FAIL

// -- custom message types for channel.cc --------------------------------------

using string_channel = broker::internal::channel<std::string, std::string>;

struct producer_msg {
  std::string source;
  string_channel::producer_message content;
};

struct consumer_msg {
  std::string source;
  string_channel::consumer_message content;
};

// -- ID block for all message types in test suites ----------------------------

CAF_BEGIN_TYPE_ID_BLOCK(broker_test, caf::id_block::broker_internal::end)

  CAF_ADD_TYPE_ID(broker_test, (consumer_msg))
  CAF_ADD_TYPE_ID(broker_test, (producer_msg))
  CAF_ADD_TYPE_ID(broker_test, (std::vector<std::string>) )
  CAF_ADD_TYPE_ID(broker_test, (string_channel::consumer_message))
  CAF_ADD_TYPE_ID(broker_test, (string_channel::cumulative_ack))
  CAF_ADD_TYPE_ID(broker_test, (string_channel::event))
  CAF_ADD_TYPE_ID(broker_test, (string_channel::handshake))
  CAF_ADD_TYPE_ID(broker_test, (string_channel::heartbeat))
  CAF_ADD_TYPE_ID(broker_test, (string_channel::nack))
  CAF_ADD_TYPE_ID(broker_test, (string_channel::producer_message))
  CAF_ADD_TYPE_ID(broker_test, (string_channel::retransmit_failed))

CAF_END_TYPE_ID_BLOCK(broker_test)

// -- inspection support -------------------------------------------------------

template <class Inspector>
bool inspect(Inspector& f, producer_msg& x) {
  return f.object(x).fields(f.field("source", x.source),
                            f.field("content", x.content));
}

template <class Inspector>
bool inspect(Inspector& f, consumer_msg& x) {
  return f.object(x).fields(f.field("source", x.source),
                            f.field("content", x.content));
}

// -- synchronization ----------------------------------------------------------

// Drop-in replacement for std::barrier (based on the TS API as of 2020).
class barrier {
public:
  explicit barrier(ptrdiff_t num_threads);

  void arrive_and_wait();

private:
  size_t num_threads_;
  std::mutex mx_;
  std::atomic<size_t> count_;
  std::condition_variable cv_;
};

// Allows threads to wait on a boolean condition. Unlike promise<bool>, allows
// calling `set_true` multiple times without side effects.
class beacon {
public:
  beacon();

  void set_true();

  void wait();

private:
  std::mutex mx_;
  std::atomic<bool> value_;
  std::condition_variable cv_;
};

// -- data processing ----------------------------------------------------------

std::vector<std::string>
normalize_status_log(const std::vector<broker::data_message>& xs,
                     bool include_endpoint_id = false);

// -- fixtures -----------------------------------------------------------------

struct empty_fixture_base {};

template <class Derived, class Base = empty_fixture_base>
class time_aware_fixture : public Base {
public:
  void run(caf::timespan t) {
    auto& sched = dref().sched;
    for (;;) {
      sched.run();
      auto& clk = sched.clock();
      if (!clk.has_pending_timeout()) {
        sched.advance_time(t);
        sched.run();
        return;
      } else {
        auto next_timeout = clk.next_timeout();
        auto delta = next_timeout - clk.now();
        if (delta >= t) {
          sched.advance_time(t);
          sched.run();
          return;
        } else {
          sched.advance_time(delta);
          t -= delta;
        }
      }
    }
  }

  template <class Rep, class Period>
  void run(std::chrono::duration<Rep, Period> t) {
    run(std::chrono::duration_cast<caf::timespan>(t));
  }

private:
  Derived& dref() {
    return *static_cast<Derived*>(this);
  }
};

/// A fixture that hosts an endpoint configured with `test_coordinator` as
/// scheduler as well as a `scoped_actor`.
class base_fixture : public time_aware_fixture<base_fixture> {
public:
  struct endpoint_state {
    broker::endpoint_id id;
    broker::filter_type filter;
    caf::actor hdl;
  };

  using super = time_aware_fixture<base_fixture>;

  using scheduler_type = caf::scheduler::test_coordinator;

  base_fixture();

  virtual ~base_fixture();

  broker::endpoint ep;
  caf::actor_system& sys;
  caf::scoped_actor self;
  scheduler_type& sched;

  // A couple of predefined endpoint IDs for testing purposes. Filled from A-Z.
  std::map<char, broker::endpoint_id> ids;

  // String representation of all `ids`.
  std::map<char, std::string> str_ids;

  using super::run;

  void run();

  void consume_message();

  char id_by_value(const broker::endpoint_id& value);

  /// Dereferences `hdl` and downcasts it to `T`.
  template <class T = caf::scheduled_actor, class Handle = caf::actor>
  static T& deref(const Handle& hdl) {
    auto ptr = caf::actor_cast<caf::abstract_actor*>(hdl);
    if (ptr == nullptr)
      CAF_FAIL("unable to cast handle to abstract_actor*");
    return dynamic_cast<T&>(*ptr);
  }

  static endpoint_state ep_state(caf::actor core);

  static broker::configuration make_config();

  /// Establishes a peering relation between `left` and `right`.
  static caf::actor bridge(const endpoint_state& left,
                           const endpoint_state& right);

  /// Establishes a peering relation between `left` and `right`.
  static caf::actor bridge(const broker::endpoint& left,
                           const broker::endpoint& right);

  static caf::actor bridge(caf::actor left_core, caf::actor right_core);

  /// Collect data directly at a Broker core without using a
  /// `broker::subscriber` or other public API.
  static std::shared_ptr<std::vector<broker::data_message>>
  collect_data(caf::actor core, broker::filter_type filter);

  static void push_data(caf::actor core, std::vector<broker::data_message> xs);
};

template <class Fixture>
class net_fixture {
public:
  using planet_type = Fixture;

  planet_type earth;
  planet_type mars;

  auto bridge(planet_type& left, planet_type& right) {
    return planet_type::bridge(left.ep, right.ep);
  }

  void run() {
    while (earth.sched.has_job() || earth.sched.has_pending_timeout()
           || mars.sched.has_job() || mars.sched.has_pending_timeout()) {
      earth.sched.run();
      earth.sched.trigger_timeouts();
      mars.sched.run();
      mars.sched.trigger_timeouts();
    }
  }

  void run(caf::timespan t) {
    auto& n1 = this->earth;
    auto& n2 = this->mars;
    assert(n1.sched.clock().now() == n2.sched.clock().now());
    auto advance = [](auto& n) {
      return n.sched.try_run_once() || n.mpx.try_exec_runnable()
             || n.mpx.read_data();
    };
    auto exhaust = [&] {
      while (advance(n1) || advance(n2))
        ; // repeat
    };
    auto get_next_timeout = [](auto& result, auto& node) {
      if (node.sched.has_pending_timeout()) {
        auto t = node.sched.clock().schedule().begin()->first;
        if (result)
          result = std::min(*result, t);
        else
          result = t;
      }
    };
    for (;;) {
      exhaust();
      caf::optional<caf::actor_clock::time_point> next_timeout;
      get_next_timeout(next_timeout, n1);
      get_next_timeout(next_timeout, n2);
      if (!next_timeout) {
        n1.sched.advance_time(t);
        n2.sched.advance_time(t);
        exhaust();
        return;
      }
      auto delta = *next_timeout - n1.sched.clock().now();
      if (delta >= t) {
        n1.sched.advance_time(t);
        n2.sched.advance_time(t);
        exhaust();
        return;
      }
      n1.sched.advance_time(delta);
      n2.sched.advance_time(delta);
      t -= delta;
    }
  }

  template <class Rep, class Period>
  void run(std::chrono::duration<Rep, Period> t) {
    run(std::chrono::duration_cast<caf::timespan>(t));
  }
};

// -- utility ------------------------------------------------------------------

template <class T>
T unbox(broker::expected<T> x) {
  if (!x)
    FAIL(to_string(x.error()));
  else
    return std::move(*x);
}

inline broker::data value_of(broker::expected<broker::data> x) {
  if (!x) {
    FAIL("cannot unbox expected<data>: " << to_string(x.error()));
  }
  return std::move(*x);
}

inline broker::error error_of(broker::expected<broker::data> x) {
  if (x) {
    FAIL(
      "cannot get error of expected<data>, contains value: " << to_string(*x));
  }
  return std::move(x.error());
}

/// Convenience function for creating a vector of events from topic and data
/// pairs.
inline std::vector<broker::data_message>
data_msgs(std::initializer_list<std::pair<broker::topic, broker::data>> xs) {
  std::vector<broker::data_message> result;
  for (auto& x : xs)
    result.emplace_back(x.first, x.second);
  return result;
}
