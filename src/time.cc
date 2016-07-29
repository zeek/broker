#include "broker/time.hh"

namespace broker {

bool convert(const interval& i, std::string& str) {
  using std::to_string;
  str = to_string(i.count());
  str += "ns";
  return true;
}

bool convert(const interval& i, fractional_seconds& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(i);
  return true;
}

bool convert(const interval& i, double& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(i).count();
  return true;
}

bool convert(const timestamp& t, std::string& str) {
  return convert(t.time_since_epoch(), str);
}

timestamp now() {
  return clock::now();
};

} // namespace broker
