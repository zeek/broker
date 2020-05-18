#pragma once

#include <cstdint>
#include <vector>

namespace broker::alm {

/// A logical clock using a 64-bit counter.
struct lamport_timestamp {
  uint64_t value = 1;

  lamport_timestamp& operator++() {
    ++value;
    return *this;
  }
};

/// @relates lamport_timestamp
constexpr bool operator<(lamport_timestamp x, lamport_timestamp y) {
  return x.value < y.value;
}

/// @relates lamport_timestamp
constexpr bool operator<=(lamport_timestamp x, lamport_timestamp y) {
  return x.value <= y.value;
}

/// @relates lamport_timestamp
constexpr bool operator>(lamport_timestamp x, lamport_timestamp y) {
  return x.value > y.value;
}

/// @relates lamport_timestamp
constexpr bool operator>=(lamport_timestamp x, lamport_timestamp y) {
  return x.value >= y.value;
}

/// @relates lamport_timestamp
constexpr bool operator==(lamport_timestamp x, lamport_timestamp y) {
  return x.value == y.value;
}

/// @relates lamport_timestamp
constexpr bool operator!=(lamport_timestamp x, lamport_timestamp y) {
  return x.value != y.value;
}

/// @relates lamport_timestamp
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, lamport_timestamp& x) {
  return f(x.value);
}

/// @relates lamport_timestamp
using vector_timestamp = std::vector<lamport_timestamp>;

} // namespace broker::alm

namespace broker::literals {

constexpr auto operator""_lt(unsigned long long value) noexcept {
  return alm::lamport_timestamp{static_cast<uint64_t>(value)};
}

} // namespace broker::literals
