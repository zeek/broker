#include "broker/endpoint_id.hh"

#include <caf/hash/fnv.hpp>
#include <caf/uuid.hpp>

namespace broker {

namespace {

std::byte nil_bytes[16];

// TODO: caf::byte is soon to get replaced by std::byte. This is going to make
//       these two conversion functions trivial.

caf::uuid to_uuid(const endpoint_id& id) {
  std::array<caf::byte, 16> tmp;
  auto& bytes = id.bytes();
  for (size_t index = 0; index < 16; ++index)
    tmp[index] = static_cast<caf::byte>(bytes[index]);
  return caf::uuid{tmp};
}

endpoint_id from_uuid(const caf::uuid& id) {
  std::array<std::byte, 16> tmp;
  auto& bytes = id.bytes();
  for (size_t index = 0; index < 16; ++index)
    tmp[index] = static_cast<std::byte>(bytes[index]);
  return endpoint_id{tmp};
}

} // namespace

endpoint_id::endpoint_id() noexcept {
  memset(bytes_.data(), 0, bytes_.size());
}

bool endpoint_id::valid() const noexcept {
  return memcmp(bytes_.data(), nil_bytes, num_bytes) != 0;
}

size_t endpoint_id::hash() const noexcept {
  return caf::hash::fnv<size_t>::compute(bytes_);
}

endpoint_id endpoint_id::random() noexcept {
  return from_uuid(caf::uuid::random());
}

endpoint_id endpoint_id::random(unsigned seed) noexcept {
  return from_uuid(caf::uuid::random(seed));
}

bool endpoint_id::can_parse(const std::string& str) {
  return caf::uuid::can_parse(str);
}

// -- free functions -----------------------------------------------------------

void convert(endpoint_id x, std::string& str) {
  str = caf::to_string(to_uuid(x));
}

bool convert(const std::string& str, endpoint_id& x) {
  caf::uuid id;
  if (auto err = caf::parse(str, id)) {
    return false;
  } else {
    x = from_uuid(id);
    return true;
  }
}

} // namespace broker
