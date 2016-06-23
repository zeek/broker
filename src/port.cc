#include <cerrno>
#include <cstring>
#include <sstream>
#include <tuple>
#include <type_traits>

#include "broker/port.hh"
#include "broker/detail/hash.hh"

namespace broker {

port::port() : num_{0}, proto_{protocol::unknown} {
}

port::port(number_type n, protocol p) : num_{n}, proto_{p} {
}

port::number_type port::number() const {
  return num_;
}

port::protocol port::type() const {
  return proto_;
}

bool operator==(const port& lhs, const port& rhs) {
  return lhs.proto_ == rhs.proto_ && lhs.num_ == rhs.num_;
}

bool operator<(const port& lhs, const port& rhs) {
  return std::tie(lhs.num_, lhs.proto_) < std::tie(rhs.num_, rhs.proto_);
}

bool convert(const port& p, std::string& str) {
  std::ostringstream ss;
  ss << p.number();
  ss << '/';
  switch (p.type()) {
    default:
      ss << "?";
      break;
    case port::protocol::tcp:
      ss << "tcp";
      break;
    case port::protocol::udp:
      ss << "udp";
      break;
    case port::protocol::icmp:
      ss << "icmp";
      break;
  }
  str = ss.str();
  return true;
}

bool convert(const std::string& str, port& p) {
  auto i = str.find('/');
  if (i == std::string::npos)
    return false;
  char* end;
  auto num = std::strtoul(str.data(), &end, 10);
  if (errno == ERANGE)
    return false;
  auto slash = std::strchr(end, '/');
  if (slash == nullptr)
    return false;
  // Both strings are NUL-terminated, so strcmp is safe.
  auto proto = port::protocol::unknown;
  if (std::strcmp(slash + 1, "tcp") == 0)
    proto = port::protocol::tcp;
  else if (std::strcmp(slash + 1, "udp") == 0)
    proto = port::protocol::udp;
  else if (std::strcmp(slash + 1, "icmp") == 0)
    proto = port::protocol::icmp;
  p = {static_cast<port::number_type>(num), proto};
  return true;
}

} // namespace broker

size_t std::hash<broker::port>::operator()(const broker::port& v) const {
  using broker::port;
  size_t rval = 0;
  broker::detail::hash_combine(rval, v.number());
  auto p = static_cast<std::underlying_type<port::protocol>::type>(v.type());
  broker::detail::hash_combine(rval, p);
  return rval;
}

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_port* broker_port_create() {
  return reinterpret_cast<broker_port*>(new (nothrow) broker::port());
}

void broker_port_delete(broker_port* p) {
  delete reinterpret_cast<broker::port*>(p);
}

broker_port* broker_port_copy(const broker_port* p) {
  auto pp = reinterpret_cast<const broker::port*>(p);
  return reinterpret_cast<broker_port*>(new (nothrow) broker::port(*pp));
}

broker_port* broker_port_from(uint16_t num, broker_port_protocol p) {
  auto rval
    = new (nothrow) broker::port(num, static_cast<broker::port::protocol>(p));
  return reinterpret_cast<broker_port*>(rval);
}

void broker_port_set_number(broker_port* dst, uint16_t num) {
  auto d = reinterpret_cast<broker::port*>(dst);
  *d = broker::port(num, d->type());
}

void broker_port_set_protocol(broker_port* dst, broker_port_protocol p) {
  auto d = reinterpret_cast<broker::port*>(dst);
  *d = broker::port(d->number(), static_cast<broker::port::protocol>(p));
}

uint16_t broker_port_number(const broker_port* p) {
  return reinterpret_cast<const broker::port*>(p)->number();
}

broker_port_protocol broker_port_type(const broker_port* p) {
  auto pp = reinterpret_cast<const broker::port*>(p);
  return static_cast<broker_port_protocol>(pp->type());
}

broker_string* broker_port_to_string(const broker_port* p) {
  auto pp = reinterpret_cast<const broker::port*>(p);
  try {
    std::string str;
    if (!convert(*pp, str))
      return nullptr;
    return reinterpret_cast<broker_string*>(new std::string{std::move(str)});
  } catch (const std::bad_alloc&) {
    return nullptr;
  }
}

int broker_port_eq(const broker_port* a, const broker_port* b) {
  auto aa = reinterpret_cast<const broker::port*>(a);
  auto bb = reinterpret_cast<const broker::port*>(b);
  return *aa == *bb;
}

int broker_port_lt(const broker_port* a, const broker_port* b) {
  auto aa = reinterpret_cast<const broker::port*>(a);
  auto bb = reinterpret_cast<const broker::port*>(b);
  return *aa < *bb;
}

size_t broker_port_hash(const broker_port* p) {
  auto pp = reinterpret_cast<const broker::port*>(p);
  return std::hash<broker::port>{}(*pp);
}
