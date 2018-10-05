#include "broker/time.hh"

namespace broker {

bool convert(timespan s, std::string& str) {
  using std::to_string;
  str = to_string(s.count());
  str += "ns";
  return true;
}

bool convert(timespan s, fractional_seconds& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(s);
  return true;
}

bool convert(timespan s, double& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(s).count();
  return true;
}

bool convert(timestamp t, std::string& str) {
  return convert(t.time_since_epoch(), str);
}

bool convert(timestamp t, double& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(t.time_since_epoch()).count();
  return true;
}

bool convert(double secs, timespan& s) {
  s = std::chrono::duration_cast<timespan>(fractional_seconds{secs});
  return true;
}

bool convert(double secs, timestamp& ts) {
  auto d = std::chrono::duration_cast<timespan>(fractional_seconds{secs});
  ts = timestamp{d};
  return true;
}

timestamp now() {
  return clock::now();
}

} // namespace broker
