#include "broker/address.hh"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "broker/config.hh"

#include <caf/detail/network_order.hpp>
#include <caf/hash/fnv.hpp>
#include <caf/ip_address.hpp>

namespace broker {

namespace {

using native_t = caf::ip_address;

static constexpr bool is_little_endian =
#ifdef BROKER_BIG_ENDIAN
  false;
#else
  true;
#endif

constexpr std::array<uint8_t, 12> v4_mapped_prefix = {
  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}};

auto to_array(const uint32_t* bytes, address::family fam,
              address::byte_order order) {
  static constexpr size_t bytes_size = address::num_bytes / sizeof(uint32_t);
  using std::make_reverse_iterator;
  address::array_type result;
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
    auto last = first + address::num_bytes;
    std::copy(first, last, result.begin());
  }
  return result;
}

} // namespace

address::address() noexcept {
  memset(bytes_.data(), 0, num_bytes);
}

address::address(const address& other) noexcept : bytes_(other.bytes_) {
  // nop
}

address::address(const uint32_t* bytes, family fam, byte_order order) {
  bytes_ = to_array(bytes, fam, order);
}

address& address::operator=(const address& other) noexcept {
  bytes_ = other.bytes_;
  return *this;
}

static uint32_t bit_mask32(int bottom_bits) {
  if (bottom_bits >= 32)
    return 0xffffffff;
  return (((uint32_t) 1) << bottom_bits) - 1;
}

bool address::mask(uint8_t top_bits_to_keep) {
  if (top_bits_to_keep > 128)
    return false;
  uint32_t mask[4] = {0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff};
  auto res = std::div(top_bits_to_keep, 32);
  if (res.quot < 4)
    mask[res.quot] =
      caf::detail::to_network_order(mask[res.quot] & ~bit_mask32(32 - res.rem));
  for (auto i = res.quot + 1; i < 4; ++i)
    mask[i] = 0;
  auto p = reinterpret_cast<uint32_t*>(&bytes());
  for (auto i = 0; i < 4; ++i)
    p[i] &= mask[i];
  return true;
}

bool address::is_v4() const noexcept {
  return native_t{bytes_}.embeds_v4();
}

int address::compare(const address& other) const noexcept {
  return memcmp(bytes_.data(), other.bytes_.data(), num_bytes);
}

size_t address::hash() const {
  return caf::hash::fnv<size_t>::compute(bytes_);
}

void address::convert_to(std::string& str) const {
  str = to_string(native_t{bytes_});
}

bool address::convert_from(const std::string& str) {
  native_t tmp;
  if (auto err = caf::parse(str, tmp)) {
    return false;
  } else {
    bytes_ = tmp.bytes();
    return true;
  }
}

} // namespace broker
