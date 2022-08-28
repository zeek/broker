#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>
#include <utility>

#include <caf/hash/fnv.hpp>

#include "broker/address.hh"
#include "broker/subnet.hh"

namespace broker {

subnet::subnet() : len_(0) {}

subnet::subnet(const address& addr, uint8_t length) : net_(addr), len_(length) {
  if (init())
    return;
  net_ = {};
  len_ = 0;
}

bool subnet::init() {
  if (net_.is_v4()) {
    if (len_ > 32)
      return false;
    len_ += 96;
  } else if (len_ > 128) {
    return false;
  }
  net_.mask(len_);
  return true;
}

bool subnet::contains(const address& addr) const {
  address p{addr};
  p.mask(len_);
  return p == net_;
}

const address& subnet::network() const {
  return net_;
}

uint8_t subnet::length() const {
  return net_.is_v4() ? len_ - 96 : len_;
}

size_t subnet::hash() const {
  return caf::hash::fnv<size_t>::compute(net_, len_);
}

bool operator==(const subnet& lhs, const subnet& rhs) {
  return lhs.len_ == rhs.len_ && lhs.net_ == rhs.net_;
}

bool operator<(const subnet& lhs, const subnet& rhs) {
  return std::tie(lhs.net_, lhs.len_) < std::tie(rhs.net_, lhs.len_);
}

void convert(const subnet& sn, std::string& str) {
  convert(sn.network(), str);
  str += '/';
  str += std::to_string(sn.length());
}

bool convert(const std::string& str, subnet& sn) {
  address addr;
  auto separator_pos = str.find('/');
  if (separator_pos == std::string::npos) {
    return false;
  }
  if (convert(str.substr(0, separator_pos), addr)) {
    try {
      auto len = std::stoi(str.substr(separator_pos + 1));
      if (len >= 0 && len <= 255) {
        sn = subnet{addr, static_cast<uint8_t>(len)};
        return true;
      }
    } catch (std::exception&) {
      // nop
    }
  }
  return false;
}

} // namespace broker
