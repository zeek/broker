#include "broker/time.hh"

namespace broker {
namespace time {

bool convert(const duration& d, std::string& str) {
  using std::to_string;
  str = to_string(d.count());
  str += "ns";
  return true;
}

bool convert(const duration& d, fractional_seconds& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(d);
  return true;
}

bool convert(const duration& d, double& secs) {
  secs = std::chrono::duration_cast<fractional_seconds>(d).count();
  return true;
}

bool convert(const point& p, std::string& str) {
  return convert(p.time_since_epoch(), str);
}

point now() {
  return clock::now();
};

} // namespace time
} // namespace broker
