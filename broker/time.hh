#ifndef BROKER_TIME_HH
#define BROKER_TIME_HH

#include <chrono>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <string>

#include "broker/detail/hash.hh"
#include "broker/detail/operators.hh"

#include "broker/variant.hh"

namespace broker {
namespace time {

/// A fractional timestamp.
using double_seconds = std::chrono::duration<double, std::ratio<1>>;

/// SI time units.
enum class unit : uint8_t {
  invalid = 0,
  nanoseconds,
  microseconds,
  milliseconds,
  seconds,
};

/// Converts a ratio of numerator and denominator (as in `std::ratio`) to a
/// time::unit.
/// @relates unit
template <intmax_t Num, intmax_t Denom>
struct to_unit {
  static constexpr unit value = unit::invalid;
};

template <>
struct to_unit<1, 1000000000> {
  static constexpr unit value = unit::nanoseconds;
};

template <>
struct to_unit<1, 1000000> {
  static constexpr unit value = unit::microseconds;
};

template <>
struct to_unit<1, 1000> {
  static constexpr unit value = unit::milliseconds;
};

template <>
struct to_unit<1, 1> {
  static constexpr unit value = unit::seconds;
};

template <>
struct to_unit<60, 1> {
  static constexpr unit value = unit::seconds;
};

template <>
struct to_unit<60 * 60, 1> {
  static constexpr unit value = unit::seconds;
};

/// A multi-resolution time duration.
struct duration : detail::equality_comparable<duration> {
  using count_type = int64_t;

  /// Default-constructs a zero-valued duration with nanosecond granularity.
  constexpr duration() = default;

  /// Constructs a duration from a given unit and value.
  /// @param u The time unit.
  /// @param c The value in unit *u*.
  constexpr duration(time::unit u, count_type c) : unit{u}, count{c} {
    // nop
  }

  /// Constructs a duration from a `std::chrono::duration`.
  /// @param d The duration to convert from.
  template <class Rep, class Period>
  duration(std::chrono::duration<Rep, Period> d)
    : unit{to_unit<Period::num, Period::den>::value},
      count{static_cast<count_type>(d.count())
            * static_cast<count_type>(Period::num)} {
    static_assert(to_unit<Period::num, Period::den>::value != unit::invalid,
                  "only hours, minutes, seconds, milliseconds, microseconds, "
                  "and nanoseconds supported");
  }

  time::unit unit = time::unit::invalid;
  count_type count = 0;
};

/// @relates duration
bool operator==(const duration& lhs, const duration& rhs);

/// @relates duration
template <class Processor>
void serialize(Processor& proc, duration& d) {
  proc & d.unit;
  proc & d.count;
}

/// @relates duration
bool convert(const duration& d, std::string& str);

/// @relates duration
bool convert(const duration& d, double& secs);

/// @relates duration
template <class Rep, class Period>
bool convert(const duration& d, std::chrono::nanoseconds& ns);

/// @relates duration
bool convert(const duration& d, std::chrono::microseconds& us);

/// @relates duration
bool convert(const duration& d, std::chrono::milliseconds& ms);

/// @relates duration
bool convert(const duration& d, std::chrono::seconds& s);

/// @relates duration
template <class Clock, class Duration>
std::chrono::time_point<Clock, Duration>&
operator+=(std::chrono::time_point<Clock, Duration>& lhs, const duration& rhs) {
  switch (rhs.unit) {
    case unit::invalid:
      break;
    case unit::seconds:
      lhs += std::chrono::seconds(rhs.count);
      break;
    case unit::milliseconds:
      lhs += std::chrono::milliseconds(rhs.count);
      break;
    case unit::microseconds:
      lhs += std::chrono::microseconds(rhs.count);
      break;
    case unit::nanoseconds:
      lhs += std::chrono::nanoseconds(rhs.count);
      break;
  }
  return lhs;
}

/// @relates duration
template <class Clock, class Duration>
std::chrono::time_point<Clock, Duration>&
operator-=(std::chrono::time_point<Clock, Duration>& lhs, const duration& rhs) {
  switch (rhs.unit) {
    case unit::invalid:
      break;
    case unit::seconds:
      lhs -= std::chrono::seconds(rhs.count);
      break;
    case unit::milliseconds:
      lhs -= std::chrono::milliseconds(rhs.count);
      break;
    case unit::microseconds:
      lhs -= std::chrono::microseconds(rhs.count);
      break;
    case unit::nanoseconds:
      lhs -= std::chrono::nanoseconds(rhs.count);
      break;
  }
  return lhs;
}

/// A fixed point in time relative to the UNIX epoch.
struct point : detail::equality_comparable<point> {
  /// Constructs a time point from a duration.
  point(duration d = {}) : value{d} {
  }

  template <class Clock, class Duration>
  point(std::chrono::time_point<Clock, Duration> tp)
    : value{tp.time_since_epoch()} {
    // nop
  }

  duration value;
};

/// @relates point
bool operator==(const point& lhs, const point& rhs);

/// @relates point
template <class Processor>
void serialize(Processor& proc, point& p) {
  proc & p.value;
}

/// @returns the current point in time.
static point now();

} // namespace time
} // namespace broker

namespace std {

/// @relates broker::time::duration
template <>
struct hash<broker::time::duration> {
  size_t operator()(const broker::time::duration& d) const {
    using underlying_type = std::underlying_type<broker::time::unit>;
    size_t result;
    auto underlying = static_cast<typename underlying_type::type>(d.unit);
    broker::detail::hash_combine(result, underlying);
    broker::detail::hash_combine(result, d.count);
    return result;
  }
};

/// @relates broker::time::point
template <>
struct hash<broker::time::point> {
  size_t operator()(const broker::time::point& tp) const {
    return std::hash<broker::time::duration>{}(tp.value);
  }
};

} // namespace std;

#endif // BROKER_TIME_HH
