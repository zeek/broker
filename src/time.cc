#include "broker/time.hh"

namespace broker {
namespace time {

bool operator==(const duration& lhs, const duration& rhs) {
  return lhs.unit == rhs.unit && lhs.count == rhs.count;
}

bool convert(const duration& d, std::string& str) {
  str = std::to_string(d.count);
  switch (d.unit) {
    case unit::invalid:
      return false;
    case unit::nanoseconds:
      str += "ns";
      break;
    case unit::microseconds:
      str += "us";
      break;
    case unit::milliseconds:
      str += "ms";
      break;
    case unit::seconds:
      str += 's';
      break;
  }
  return true;
}

namespace {

template <class To>
To cast(const duration& d) {
  namespace chrono = std::chrono;
  switch (d.unit) {
    case unit::invalid:
      return {};
    case unit::nanoseconds:
      return chrono::duration_cast<To>(chrono::nanoseconds(d.count));
    case unit::microseconds:
      return chrono::duration_cast<To>(chrono::microseconds(d.count));
    case unit::milliseconds:
      return chrono::duration_cast<To>(chrono::milliseconds(d.count));
    case unit::seconds:
      return chrono::duration_cast<To>(chrono::seconds(d.count));
  }
}

template <class To>
void cast(const duration& d, To& to) {
  to = cast<To>(d);
}

} // namespace <anonymous>

bool convert(const duration& d, double& secs) {
  secs = cast<double_seconds>(d).count();
  return true;
}

bool convert(const duration& d, std::chrono::nanoseconds& ns) {
  cast(d, ns);
  return true;
}

bool convert(const duration& d, std::chrono::microseconds& us) {
  cast(d, us);
  return true;
}

bool convert(const duration& d, std::chrono::milliseconds& ms) {
  cast(d, ms);
  return true;
}

bool convert(const duration& d, std::chrono::seconds& s) {
  cast(d, s);
  return true;
}

bool operator==(const point& lhs, const point& rhs) {
  return lhs.value == rhs.value;
}

point now() {
  return std::chrono::system_clock::now();
};

} // namespace time
} // namespace broker
