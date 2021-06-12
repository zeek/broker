#pragma once

#include <caf/actor.hpp>

#include "broker/fwd.hh"

namespace broker {

/// Represents a handle to background work performed by Broker. This may be a
/// scheduler or publisher that runs asynchronously.
class activity {
public:
  friend class endpoint;

  activity() = default;
  activity(activity&&) = default;
  activity(const activity&) = default;
  activity& operator=(activity&&) = default;
  activity& operator=(const activity&) = default;

  explicit operator bool() const noexcept {
    return hdl_ != nullptr;
  }

  /// Cancels the activity.
  void cancel();

  /// Blocks the current thread until the background activity completed.
  void wait();

private:
  explicit activity(caf::actor hdl);

  caf::actor hdl_;
};

} // namespace broker
