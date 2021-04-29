#pragma once

#include "broker/detail/assert.hh"

namespace broker::detail {

/// Computes the next tick based on an initial time, the current time and a
/// fixed interval.
template <class TimePoint, class Duration>
auto next_tick(TimePoint init, TimePoint now, Duration interval) {
  BROKER_ASSERT(now >= init);
  BROKER_ASSERT(interval.count() != 0);
  auto dt = now - init;
  auto passed = dt / interval;
  static_assert(std::is_integral_v<decltype(passed)>);
  return init + ((passed + 1) * interval);
}

} // namespace broker::detail
