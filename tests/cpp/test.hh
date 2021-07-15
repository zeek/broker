#pragma once

#ifdef SUITE
#define CAF_SUITE SUITE
#endif

#include <caf/test/dsl.hpp>

#include <caf/actor_system.hpp>
#include <caf/scheduler/test_coordinator.hpp>
#include <caf/scoped_actor.hpp>

#include "broker/configuration.hh"
#include "broker/detail/channel.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"

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
#define MESSAGE CAF_MESSAGE

// -- macros for checking results ---------------------------------------------

#define REQUIRE CAF_REQUIRE
#define REQUIRE_EQUAL CAF_REQUIRE_EQUAL
#define REQUIRE_NOT_EQUAL CAF_REQUIRE_NOT_EQUAL
#define REQUIRE_LESS CAF_REQUIRE_LESS
#define REQUIRE_LESS_EQUAL CAF_REQUIRE_LESS_EQUAL
#define REQUIRE_GREATER CAF_REQUIRE_GREATER
#define REQUIRE_GREATER_EQUAL CAF_REQUIRE_GREATER_EQUAL
#define CHECK CAF_CHECK
#define CHECK_EQUAL CAF_CHECK_EQUAL
#define CHECK_NOT_EQUAL CAF_CHECK_NOT_EQUAL
#define CHECK_LESS CAF_CHECK_LESS
#define CHECK_LESS_EQUAL CAF_CHECK_LESS_EQUAL
#define CHECK_GREATER CAF_CHECK_GREATER
#define CHECK_GREATER_EQUAL CAF_CHECK_GREATER_EQUAL
#define CHECK_FAIL CAF_CHECK_FAIL
#define FAIL CAF_FAIL

// -- custom message types for channel.cc --------------------------------------

using string_channel = broker::detail::channel<std::string, std::string>;

struct producer_msg {
  std::string source;
  string_channel::producer_message content;
};

struct consumer_msg {
  std::string source;
  string_channel::consumer_message content;
};

// -- ID block for all message types in test suites ----------------------------

CAF_BEGIN_TYPE_ID_BLOCK(broker_test, caf::id_block::broker::end)

  CAF_ADD_TYPE_ID(broker_test, (consumer_msg))
  CAF_ADD_TYPE_ID(broker_test, (producer_msg))
  CAF_ADD_TYPE_ID(broker_test, (std::vector<std::string>))
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

// -- fixtures -----------------------------------------------------------------

struct empty_fixture_base {};

template <class Derived, class Base = empty_fixture_base>
class time_aware_fixture : public Base {
public:
  void run(caf::timespan t) {
    auto& sched = dref().sched;
    for (;;) {
      sched.run();
      if (!sched.has_pending_timeout()) {
        sched.advance_time(t);
        sched.run();
        return;
      } else {
        auto& clk = sched.clock();
        auto next_timeout = clk.schedule().begin()->first;
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
    broker::alm::lamport_timestamp ts;
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
  std::map<char, broker::endpoint_id> ids;

  using super::run;

  void run();

  void consume_message();

  static void init_socket_api();

  static void deinit_socket_api();

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

inline broker::data value_of(caf::expected<broker::data> x) {
  if (!x) {
    FAIL("cannot unbox expected<data>: " << to_string(x.error()));
  }
  return std::move(*x);
}

inline caf::error error_of(caf::expected<broker::data> x) {
  if (x) {
    FAIL("cannot get error of expected<data>, contains value: "
         << to_string(*x));
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
