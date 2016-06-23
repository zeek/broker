#include <sstream>
#include <tuple>

#include "broker/convert.hh"
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
  str += std::to_string(sn.network().is_v4() ? sn.length() - 96 : sn.length());
  return true;
}

} // namespace broker

size_t std::hash<broker::subnet>::operator()(const broker::subnet& v) const {
  size_t rval = 0;
  broker::detail::hash_combine(rval, v.network());
  broker::detail::hash_combine(rval, v.length());
  return rval;
}

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_subnet* broker_subnet_create() {
  return reinterpret_cast<broker_subnet*>(new (nothrow) broker::subnet());
}

void broker_subnet_delete(broker_subnet* s) {
  delete reinterpret_cast<broker::subnet*>(s);
}

broker_subnet* broker_subnet_copy(const broker_subnet* s) {
  auto ss = reinterpret_cast<const broker::subnet*>(s);
  return reinterpret_cast<broker_subnet*>(new (nothrow) broker::subnet(*ss));
}

broker_subnet* broker_subnet_from(const broker_address* a, uint8_t len) {
  auto aa = reinterpret_cast<const broker::address*>(a);
  return reinterpret_cast<broker_subnet*>(new (nothrow)
                                            broker::subnet(*aa, len));
}

void broker_subnet_set(broker_subnet* dst, broker_subnet* src) {
  auto d = reinterpret_cast<broker::subnet*>(dst);
  auto s = reinterpret_cast<broker::subnet*>(src);
  *d = *s;
}

int broker_subnet_contains(const broker_subnet* s, const broker_address* a) {
  auto aa = reinterpret_cast<const broker::address*>(a);
  return reinterpret_cast<const broker::subnet*>(s)->contains(*aa);
}

const broker_address* broker_subnet_network(const broker_subnet* s) {
  auto ss = reinterpret_cast<const broker::subnet*>(s);
  return reinterpret_cast<const broker_address*>(&ss->network());
}

uint8_t broker_subnet_length(const broker_subnet* s) {
  auto ss = reinterpret_cast<const broker::subnet*>(s);
  return ss->length();
}

broker_string* broker_subnet_to_string(const broker_subnet* s) {
  auto ss = reinterpret_cast<const broker::subnet*>(s);
  try {
    auto rval = broker::to_string(*ss);
    return reinterpret_cast<broker_string*>(new std::string(std::move(rval)));
  } catch (std::bad_alloc&) {
    return nullptr;
  }
}

int broker_subnet_eq(const broker_subnet* a, const broker_subnet* b) {
  auto aa = reinterpret_cast<const broker::subnet*>(a);
  auto bb = reinterpret_cast<const broker::subnet*>(b);
  return *aa == *bb;
}

int broker_subnet_lt(const broker_subnet* a, const broker_subnet* b) {
  auto aa = reinterpret_cast<const broker::subnet*>(a);
  auto bb = reinterpret_cast<const broker::subnet*>(b);
  return *aa < *bb;
}

size_t broker_subnet_hash(const broker_subnet* s) {
  auto ss = reinterpret_cast<const broker::subnet*>(s);
  return std::hash<broker::subnet>{}(*ss);
}
