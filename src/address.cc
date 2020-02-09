#include "broker/address.hh"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "broker/config.hh"
#include "broker/detail/hash.hh"

#include <caf/detail/network_order.hpp>

namespace broker {

namespace {

static constexpr bool is_little_endian =
#ifdef BROKER_BIG_ENDIAN
  false;
#else
  true;
#endif

constexpr std::array<uint8_t, 12> v4_mapped_prefix
  = {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}};

auto to_array(const uint32_t* bytes, address::family fam,
              address::byte_order order) {
  static constexpr size_t bytes_size
    = caf::ip_address::num_bytes / sizeof(uint32_t);
  using std::make_reverse_iterator;
  caf::ip_address::array_type result;
  if (fam == address::family::ipv4) {
    auto dst = std::copy(v4_mapped_prefix.begin(), v4_mapped_prefix.end(),
                         result.begin());
    auto first = reinterpret_cast<const uint8_t*>(bytes);
    auto last = first + sizeof(uint32_t);
    if constexpr (is_little_endian) {
      if (order == address::byte_order::host) {
        std::copy(make_reverse_iterator(last), make_reverse_iterator(first),
                  dst);
        return result;
      }
    }
    std::copy(first, last, dst);
  } else {
    if constexpr (is_little_endian) {
      if (order == address::byte_order::host) {
        auto dst = result.begin();
        for (auto iter = bytes; iter != bytes + bytes_size; ++iter) {
          auto first = reinterpret_cast<const uint8_t*>(iter);
          auto last = first + sizeof(uint32_t);
          dst = std::copy(make_reverse_iterator(last),
                          make_reverse_iterator(first), dst);
        }
        return result;
      }
    }
    auto first = reinterpret_cast<const uint8_t*>(bytes);
    auto last = first + caf::ip_address::num_bytes;
    std::copy(first, last, result.begin());
  }
  return result;
}

} // namespace

address::address(const uint32_t* bytes, family fam, byte_order order)
  : addr_(to_array(bytes, fam, order)) {
  // nop
}

static uint32_t bit_mask32(int bottom_bits) {
  if (bottom_bits >= 32)
    return 0xffffffff;
  return (((uint32_t)1) << bottom_bits) - 1;
}

bool address::mask(uint8_t top_bits_to_keep) {
  if (top_bits_to_keep > 128)
    return false;
  uint32_t mask[4] = {0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff};
  auto res = std::ldiv(top_bits_to_keep, 32);
  if (res.quot < 4)
    mask[res.quot] = caf::detail::to_network_order(mask[res.quot]
                                                   & ~bit_mask32(32 - res.rem));
  for (auto i = res.quot + 1; i < 4; ++i)
    mask[i] = 0;
  auto p = reinterpret_cast<uint32_t*>(&bytes());
  for (auto i = 0; i < 4; ++i)
    p[i] &= mask[i];
  return true;
}

} // namespace broker

size_t std::hash<broker::address>::operator()(const broker::address& v) const {
  return broker::detail::hash_range(v.bytes().begin(), v.bytes().end());
}
