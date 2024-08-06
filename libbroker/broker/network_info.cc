#include "broker/network_info.hh"

#include <chrono>
#include <string>
#include <tuple>
#include <utility>

#include <caf/uri.hpp>

#include "broker/network_info.hh"

namespace broker {

network_info::network_info(std::string addr, uint16_t port,
                           timeout::seconds retry)
  : address{std::move(addr)}, port{port}, retry{retry} {}

bool operator==(const network_info& x, const network_info& y) {
  return x.address == y.address && x.port == y.port;
}

bool operator<(const network_info& x, const network_info& y) {
  return std::tie(x.address, x.port) < std::tie(y.address, y.port);
}

void convert(const network_info& x, std::string& str) {
  str = x.address;
  str += ':';
  str += std::to_string(x.port);
}

std::string to_string(const network_info& x) {
  std::string result;
  convert(x, result);
  return result;
}

std::string to_string(const std::optional<network_info>& x) {
  if (x)
    return "*" + to_string(*x);
  else
    return "null";
}

} // namespace broker
