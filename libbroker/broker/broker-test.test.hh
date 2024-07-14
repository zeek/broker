#pragma once

#ifdef SUITE
#  define CAF_SUITE SUITE
#endif

#include <caf/test/bdd_dsl.hpp>

#include <caf/actor_system.hpp>
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

/// Adds broker endpoint IDs with keys A-Z to the test environment.
class ids_fixture {
public:
  ids_fixture();

  virtual ~ids_fixture();

  char id_by_value(const broker::endpoint_id& value);

  // A couple of predefined endpoint IDs for testing purposes. Filled from A-Z.
  std::map<char, broker::endpoint_id> ids;

  // String representation of all `ids`.
  std::map<char, std::string> str_ids;
};

// -- utility ------------------------------------------------------------------

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
