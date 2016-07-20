#ifndef BROKER_TIME_HH
#define BROKER_TIME_HH

#include <chrono>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <string>

#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

#include "broker/detail/hash.hh"
#include "broker/detail/operators.hh"

namespace broker {
namespace time {

/// A fractional timestamp represented in IEEE754 double-precision floating
/// point.
using fractional_seconds = std::chrono::duration<double, std::ratio<1>>;

/// The clock type.
using clock = std::chrono::system_clock;

/// A fractional timestamp with nanosecond precision..
using duration = std::chrono::duration<int64_t, std::nano>;

/// A point in time anchored at the UNIX epoch: January 1, 1970.
using point = std::chrono::time_point<clock, duration>;

/// @relates duration
bool convert(const duration& d, fractional_seconds& secs);

/// @relates duration
bool convert(const duration& d, double& secs);

/// @relates duration
bool convert(const duration& p, std::string& str);

/// @relates point
bool convert(const point& p, std::string& str);

/// @returns the current point in time.
point now();

} // namespace time
} // namespace broker

// Because we only use type aliases and CAF's serialization framework uses ADL
// to find the serialize() overloads, we must define them in the corresponding
// namespace.
namespace caf {

template <class Rep, class Period>
void serialize(serializer& sink, std::chrono::duration<Rep, Period> d) {
  sink << d.count();
}

template <class Rep, class Period>
void serialize(deserializer& source, std::chrono::duration<Rep, Period>& d) {
  Rep r;
  source >> r;
  d = r;
}

template <class Clock, class Duration>
void serialize(serializer& sink, std::chrono::time_point<Clock, Duration> t) {
  sink << t.time_since_epoch();
}

template <class Clock, class Duration>
void serialize(deserializer& source,
                 std::chrono::time_point<Clock, Duration>& t) {
  Duration since_epoch;
  source >> since_epoch;
  t = std::chrono::time_point<Clock, Duration>(since_epoch);
}

} // namespace caf

namespace std {

/// @relates broker::time::duration
template <>
struct hash<broker::time::duration> {
  size_t operator()(const broker::time::duration& d) const {
    return hash<broker::time::duration::rep>{}(d.count());
  }
};

/// @relates broker::time::point
template <>
struct hash<broker::time::point> {
  size_t operator()(const broker::time::point& p) const {
    return hash<broker::time::duration>{}(p.time_since_epoch());
  }
};

} // namespace std

#endif // BROKER_TIME_HH
