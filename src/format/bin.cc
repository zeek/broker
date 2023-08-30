#include "broker/format/bin.hh"

#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

namespace broker::format::bin::v1 {

uint16_t to_network_order(uint16_t value) {
  return caf::detail::to_network_order(value);
}

uint64_t to_network_order(uint64_t value) {
  return caf::detail::to_network_order(value);
}

uint64_t real_to_network_representation(real value) {
  return caf::detail::pack754(value);
}

real real_from_network_representation(uint64_t value) {
  return caf::detail::unpack754(value);
}

bool read_varbyte(const_byte_pointer& first, const_byte_pointer last,
                  size_t& result) {
  // Use varbyte encoding to compress sequence size on the wire.
  uint32_t x = 0;
  int n = 0;
  uint8_t low7 = 0;
  do {
    if (first == last)
      return false;
    low7 = static_cast<uint8_t>(*first++);
    x |= static_cast<uint32_t>((low7 & 0x7F)) << (7 * n);
    ++n;
  } while (low7 & 0x80);
  result = x;
  return true;
}

} // namespace broker::format::bin::v1
