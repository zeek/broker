#include "broker/entity_id.hh"

#include <caf/hash/fnv.hpp>

namespace broker {

size_t entity_id ::hash() const noexcept {
  return caf::hash::fnv<size_t>::compute(*this);
}

std::string to_string(const entity_id& x) {
  using std::to_string;
  std::string result;
  if (x.object != 0 || x.endpoint) {
    result = to_string(x.object);
    result += "@";
    result += to_string(x.endpoint);
  } else {
    result = "none";
  }
  return result;
}

} // namespace broker
