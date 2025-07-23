#include "broker/entity_id.hh"
#include "broker/format.hh"

#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

#include <caf/hash/fnv.hpp>

#include <format>

namespace broker {
size_t entity_id ::hash() const noexcept {
  return caf::hash::fnv<size_t>::compute(*this);
}

void convert(const entity_id& in, std::string& out) {
  if (in.object != 0 || in.endpoint) {
    out = std::format("{}@{}", in.object, in.endpoint);
  } else {
    out = "none";
  }
}

} // namespace broker
