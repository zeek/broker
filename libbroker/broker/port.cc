#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <tuple>
#include <type_traits>

#include <caf/hash/fnv.hpp>

#include "broker/port.hh"

namespace broker {

port::port() : num_{0}, proto_{protocol::unknown} {}

port::port(number_type num, protocol p) : num_{num}, proto_{p} {}

int port::compare(const port& other) const {
  auto res = static_cast<int>(num_) - static_cast<int>(other.num_);
  if (res == 0) {
    return static_cast<int>(proto_) - static_cast<int>(other.proto_);
  }
  return res;
}

port::number_type port::number() const {
  return num_;
}

port::protocol port::type() const {
  return proto_;
}

void convert(const port& p, std::string& str) {
  str = std::to_string(p.number());
  str += '/';
  switch (p.type()) {
    default:
      str += '?';
      break;
    case port::protocol::tcp:
      str += "tcp";
      break;
    case port::protocol::udp:
      str += "udp";
      break;
    case port::protocol::icmp:
      str += "icmp";
      break;
  }
}

bool convert(const std::string& str, port& p) {
  auto i = str.find('/');
  // Default to TCP if no protocol is present.
  if (i == std::string::npos) {
    char* end;
    auto num = std::strtoul(str.data(), &end, 10);
    if (errno == ERANGE)
      return false;
    p = {static_cast<port::number_type>(num), port::protocol::tcp};
    return true;
  }
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

size_t port::hash() const {
  return caf::hash::fnv<size_t>::compute(num_, proto_);
}

} // namespace broker
