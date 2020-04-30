#include "broker/publisher_id.hh"

namespace broker {

std::string to_string(const publisher_id& x) {
  using std::to_string;
  std::string result;
  if (x) {
    result = to_string(x.object);
    result += "@";
    result += to_string(x.endpoint);
  } else {
    result = "none";
  }
  return result;
}

} // namespace broker
