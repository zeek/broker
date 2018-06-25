#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
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
  auto result = size_t{0};
  broker::detail::hash_combine(result, v.number());
  auto p = static_cast<std::underlying_type<port::protocol>::type>(v.type());
  broker::detail::hash_combine(result, p);
  return result;
}
