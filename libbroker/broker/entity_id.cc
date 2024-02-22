#include "broker/entity_id.hh"

#include <caf/hash/fnv.hpp>

#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

namespace broker {

size_t entity_id ::hash() const noexcept {
  return caf::hash::fnv<size_t>::compute(*this);
}

void convert(const entity_id& in, std::string& out) {
  using std::to_string;
  if (in.object != 0 || in.endpoint) {
    out = to_string(in.object);
    out += "@";
    out += to_string(in.endpoint);
  } else {
    out = "none";
  }
}

} // namespace broker
