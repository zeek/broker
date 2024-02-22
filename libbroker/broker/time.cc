#include "broker/time.hh"

#include "caf/timestamp.hpp"

namespace broker {

void convert(timespan s, std::string& str) {
  using std::to_string;
  str = to_string(s.count());
  str += "ns";
}

void convert(timespan s, fractional_seconds& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(s);
}

void convert(timespan s, double& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(s).count();
}

void convert(timestamp t, std::string& str) {
  caf::append_timestamp_to_string(str, t);
}

void convert(timestamp t, double& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(t.time_since_epoch())
           .count();
}

bool convert(double secs, timespan& s) {
  s = std::chrono::duration_cast<timespan>(fractional_seconds{secs});
  return true;
}

void convert(double secs, timestamp& ts) {
  auto d = std::chrono::duration_cast<timespan>(fractional_seconds{secs});
  ts = timestamp{d};
}

timestamp now() {
  return clock::now();
}

} // namespace broker
