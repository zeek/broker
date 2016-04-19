#ifndef BROKER_TIMEOUT_HH
#define BROKER_TIMEOUT_HH

#include <chrono>

#include <caf/atom.hpp>

namespace broker {
namespace timeout {

using std::chrono::milliseconds;
using std::chrono::seconds;

/// Timeout when peering between two brokers.
constexpr auto peer = seconds(10);

/// Timeout when subscribing to a topic.
constexpr auto subscribe = seconds(5);

} // namespace timeout
} // namespace broker

#endif // BROKER_TIMEOUT_HH
