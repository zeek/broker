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

/// A fractional timestamp represented in IEEE754 double-precision floating
/// point.
using fractional_seconds = std::chrono::duration<double, std::ratio<1>>;

/// The clock type.
using clock = std::chrono::system_clock;

/// A fractional timestamp with nanosecond precision.
using interval = std::chrono::duration<int64_t, std::nano>;

/// A point in time anchored at the UNIX epoch: January 1, 1970.
using timestamp = std::chrono::time_point<clock, interval>;

/// @relates interval
bool convert(interval i, fractional_seconds& secs);

/// @relates interval
bool convert(interval i, double& secs);

/// @relates interval
bool convert(interval i, std::string& str);

/// @relates timestamp
bool convert(timestamp t, std::string& str);

/// @relates interval
bool convert(double secs, interval& i);

/// @relates interval
bool convert(double secs, timestamp& ts);

/// @returns the current point in time.
timestamp now();

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
void serialize(serializer& sink, std::chrono::time_point<Clock, Duration> tp) {
  sink << tp.time_since_epoch();
}

template <class Clock, class Duration>
void serialize(deserializer& source,
               std::chrono::time_point<Clock, Duration>& tp) {
  Duration since_epoch;
  source >> since_epoch;
  tp = std::chrono::time_point<Clock, Duration>(since_epoch);
}

} // namespace caf

namespace std {

/// @relates broker::interval
template <>
struct hash<broker::interval> {
  size_t operator()(const broker::interval& d) const {
    return hash<broker::interval::rep>{}(d.count());
  }
};

/// @relates broker::timestamp
template <>
struct hash<broker::timestamp> {
  size_t operator()(const broker::timestamp& t) const {
    return hash<broker::interval>{}(t.time_since_epoch());
  }
};

} // namespace std

#endif // BROKER_TIME_HH
