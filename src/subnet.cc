#include <utility>
#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>

#include "broker/address.hh"
#include "broker/subnet.hh"
#include "broker/detail/hash.hh"

namespace broker {

subnet::subnet() : len_(0) {
}

subnet::subnet(address addr, uint8_t length)
  : net_(std::move(addr)), len_(length) {
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

bool operator==(const subnet& lhs, const subnet& rhs) {
  return lhs.len_ == rhs.len_ && lhs.net_ == rhs.net_;
}

bool operator<(const subnet& lhs, const subnet& rhs) {
  return std::tie(lhs.net_, lhs.len_) < std::tie(rhs.net_, lhs.len_);
}

bool convert(const subnet& sn, std::string& str) {
  if (!convert(sn.network(), str))
    return false;
  str += '/';
  str += std::to_string(sn.length());
  return true;
}

} // namespace broker

size_t std::hash<broker::subnet>::operator()(const broker::subnet& v) const {
  auto result = size_t{0};
  broker::detail::hash_combine(result, v.network());
  broker::detail::hash_combine(result, v.length());
  return result;
}
