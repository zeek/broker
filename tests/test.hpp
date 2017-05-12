#ifndef TEST_HPP
#define TEST_HPP

#ifdef SUITE
#define CAF_SUITE SUITE
#endif

#include <caf/test/unit_test.hpp>

#include <caf/actor_system.hpp>
#include <caf/scheduler/test_coordinator.hpp>
#include <caf/scoped_actor.hpp>

#include <caf/io/network/test_multiplexer.hpp>

#include "broker/configuration.hh"
#include "broker/context.hh"

// -- test setup macros --------------------------------------------------------

#define TEST CAF_TEST
#define FIXTURE_SCOPE CAF_TEST_FIXTURE_SCOPE
#define FIXTURE_SCOPE_END CAF_TEST_FIXTURE_SCOPE_END

// -- logging macros -----------------------------------------------------------

#define ERROR CAF_TEST_PRINT_ERROR
#define INFO CAF_TEST_PRINT_INFO
#define VERBOSE CAF_TEST_PRINT_VERBOSE
#define MESSAGE CAF_TEST_PRINT_VERBOSE

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

/// A fixture that offes a `context` configured with `test_coordinator` as
/// scheduler as well as a `scoped_actor`.
class base_fixture {
public:
  using scheduler_type = caf::scheduler::test_coordinator;

  base_fixture(bool fake_network = false);

  broker::context ctx;
  caf::actor_system& sys;
  caf::scoped_actor self;
  scheduler_type& sched;

private:
  static broker::configuration make_config(bool fake_network);
};

/// Extends the base fixture with fake networking.
class fake_network_fixture : public base_fixture {
public:
  fake_network_fixture();
};

#endif
