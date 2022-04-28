#pragma once

#include <cstdint>
#include <vector>

namespace broker {

/// A logical clock using a 64-bit counter.
struct lamport_timestamp {
  uint64_t value = 1;

  lamport_timestamp& operator++() {
    ++value;
    return *this;
  }
};

template <class Inspector>
bool inspect(Inspector& f, lamport_timestamp& x) {
  return f.apply(x.value);
}

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
constexpr lamport_timestamp operator+(lamport_timestamp x, uint64_t y) {
  return lamport_timestamp{x.value + y};
}

/// @relates lamport_timestamp
constexpr lamport_timestamp operator+(uint64_t x, lamport_timestamp y) {
  return lamport_timestamp{x + y.value};
}

/// @relates lamport_timestamp
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, lamport_timestamp& x) {
  return f.apply(x.value);
}

/// @relates lamport_timestamp
using vector_timestamp = std::vector<lamport_timestamp>;

} // namespace broker

namespace broker::literals {

constexpr auto operator""_lt(unsigned long long value) noexcept {
  return lamport_timestamp{static_cast<uint64_t>(value)};
}

} // namespace broker::literals
