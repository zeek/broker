#include "broker/time.hh"

namespace broker {

bool convert(interval i, std::string& str) {
  using std::to_string;
  str = to_string(i.count());
  str += "ns";
  return true;
}

bool convert(interval i, fractional_seconds& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(i);
  return true;
}

bool convert(interval i, double& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(i).count();
  return true;
}

bool convert(timestamp t, std::string& str) {
  return convert(t.time_since_epoch(), str);
}

bool convert(double secs, interval& i) {
  i = std::chrono::duration_cast<interval>(fractional_seconds{secs});
  return true;
}

bool convert(double secs, timestamp& ts) {
  auto d = std::chrono::duration_cast<interval>(fractional_seconds{secs});
  ts = timestamp{d};
  return true;
}

timestamp now() {
  return clock::now();
};

} // namespace broker
