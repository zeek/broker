#pragma once

#ifdef SUITE
#define CAF_SUITE SUITE
#endif

#include <caf/test/dsl.hpp>
#include <caf/test/io_dsl.hpp>

#include <caf/actor_system.hpp>
#include <caf/scheduler/test_coordinator.hpp>
#include <caf/scoped_actor.hpp>

#include <caf/io/network/test_multiplexer.hpp>

#include "broker/configuration.hh"
#include "broker/endpoint.hh"

#include <cassert>

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

/// A fixture that offes a `context` configured with `test_coordinator` as
/// scheduler as well as a `scoped_actor`.
class base_fixture : public time_aware_fixture<base_fixture> {
public:
  using super = time_aware_fixture<base_fixture>;

  using scheduler_type = caf::scheduler::test_coordinator;

  base_fixture();

  virtual ~base_fixture();

  broker::endpoint ep;
  caf::actor_system& sys;
  caf::scoped_actor self;
  scheduler_type& sched;

  using super::run;

  void run();

  void consume_message();

  static void init_socket_api();

  static void deinit_socket_api();

  /// Dereferences `hdl` and downcasts it to `T`.
  template <class T = caf::scheduled_actor, class Handle = caf::actor>
  T& deref(const Handle& hdl) {
    auto ptr = caf::actor_cast<caf::abstract_actor*>(hdl);
    if (ptr == nullptr)
      CAF_FAIL("unable to cast handle to abstract_actor*");
    return dynamic_cast<T&>(*ptr);
  }

private:
  static broker::configuration make_config();
};

template <class Fixture>
class net_fixture : public point_to_point_fixture<Fixture> {
public:
  static_assert(std::is_base_of<base_fixture, Fixture>::value);

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
