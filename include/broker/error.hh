#pragma once

#include <cstdint>
#include <utility>

#include <caf/atom.hpp>
#include <caf/detail/pp.hpp>
#include <caf/error.hpp>
#include <caf/make_message.hpp>

#include "broker/fwd.hh"

namespace broker {

using caf::error;

/// Broker's error codes.
// -- ec-enum-start
enum class ec : uint8_t {
  /// The unspecified default error code.
  unspecified = 1,
  /// Version incompatibility.
  peer_incompatible,
  /// Referenced peer does not exist.
  peer_invalid,
  /// Remote peer not listening.
  peer_unavailable,
  /// An peering request timed out.
  peer_timeout,
  /// Master with given name already exist.
  master_exists,
  /// Master with given name does not exist.
  no_such_master,
  /// The given data store key does not exist.
  no_such_key,
  /// The store operation timed out.
  request_timeout,
  /// The operation expected a different type than provided
  type_clash,
  /// The data value cannot be used to carry out the desired operation.
  invalid_data,
  /// The storage backend failed to execute the operation.
  backend_failure,
  /// The clone store has not yet synchronized with its master, or it has
  /// been disconnected for too long.
  stale_data,
  /// Opening a file failed.
  cannot_open_file,
  /// Writing to an open file failed.
  cannot_write_file,
  /// Received an unknown key for a topic.
  invalid_topic_key,
  /// Reached the end of an input file.
  end_of_file,
  /// Received an unknown type tag value.
  invalid_tag,
};
// -- ec-enum-end

/// @relates ec
const char* to_string(ec code);

/// @relates ec
bool convert(const std::string& str, ec& code);

/// @relates ec
template <class... Ts>
error make_error(ec x, Ts&&... xs) {
  return {static_cast<uint8_t>(x), caf::atom("broker"),
          caf::make_message(std::forward<Ts>(xs)...)};
}

/// @relates data
bool convert(const error& src, data& dst);

/// @relates data
bool convert(const data& src, error& dst);

#define BROKER_TRY_IMPL(statement)                                             \
  if (auto err = statement)                                                    \
  return err

#define BROKER_TRY_1(x1) BROKER_TRY_IMPL(x1)

#define BROKER_TRY_2(x1, x2)                                                   \
  BROKER_TRY_1(x1);                                                            \
  BROKER_TRY_IMPL(x2)

#define BROKER_TRY_3(x1, x2, x3)                                               \
  BROKER_TRY_2(x1, x2);                                                        \
  BROKER_TRY_IMPL(x3)

#define BROKER_TRY_4(x1, x2, x3, x4)                                           \
  BROKER_TRY_3(x1, x2, x3);                                                    \
  BROKER_TRY_IMPL(x4)

#define BROKER_TRY_5(x1, x2, x3, x4, x5)                                       \
  BROKER_TRY_4(x1, x2, x3, x4);                                                \
  BROKER_TRY_IMPL(x5)

#define BROKER_TRY_6(x1, x2, x3, x4, x5, x6)                                   \
  BROKER_TRY_5(x1, x2, x3, x4, x5);                                            \
  BROKER_TRY_IMPL(x6)

#define BROKER_TRY_7(x1, x2, x3, x4, x5, x6, x7)                               \
  BROKER_TRY_6(x1, x2, x3, x4, x5, x6);                                        \
  BROKER_TRY_IMPL(x7)

#define BROKER_TRY_8(x1, x2, x3, x4, x5, x6, x7, x8)                           \
  BROKER_TRY_7(x1, x2, x3, x4, x5, x6, x7);                                    \
  BROKER_TRY_IMPL(x8)

#define BROKER_TRY_9(x1, x2, x3, x4, x5, x6, x7, x8, x9)                       \
  BROKER_TRY_8(x1, x2, x3, x4, x5, x6, x7, x8);                                \
  BROKER_TRY_IMPL(x9)

#define BROKER_TRY(...) CAF_PP_OVERLOAD(BROKER_TRY_, __VA_ARGS__)(__VA_ARGS__)

} // namespace broker
