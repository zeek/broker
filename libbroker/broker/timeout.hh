#pragma once

#include <chrono>
#include <cstdint>
#include <ratio>

namespace broker::timeout {

/// Represents a timeout in milliseconds with a fixed integer type (unlike
/// std::chrono::milliseconds).
using milliseconds = std::chrono::duration<int64_t, std::milli>;

/// Represents a timeout in seconds with a fixed integer type (unlike
/// std::chrono::seconds).
using seconds = std::chrono::duration<int64_t>;

/// Timeout when peering between two brokers.
constexpr auto peer = seconds{10};

/// Timeout when subscribing to a topic.
constexpr auto subscribe = seconds{5};

/// Timeout for interacting with a data store frontend
constexpr auto frontend = seconds{10};

} // namespace broker::timeout
