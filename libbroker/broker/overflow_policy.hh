#pragma once

#include <cstdint>
#include <string>

namespace broker {

/// Configures how Broker handles message overflow in its peer buffers.
enum class overflow_policy {
  /// Drops the newest item when the buffer is full.
  drop_newest,
  /// Drops the oldest item when the buffer is full.
  drop_oldest,
  /// Disconnects the peer when its buffer is full.
  disconnect,
};

void convert(overflow_policy src, std::string& dst);

bool convert(const std::string& src, overflow_policy& dst);

} // namespace broker
