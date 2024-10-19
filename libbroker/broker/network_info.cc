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

int network_info::compare(const network_info& other) const {
  auto res = address.compare(other.address);
  if (res == 0) {
    return static_cast<int>(port) - static_cast<int>(other.port);
  }
  return res;
}

std::string to_string(const network_info& x) {
  using std::to_string;
  return x.address + ':' + to_string(x.port);
}

std::string to_string(const std::optional<network_info>& x) {
  if (x)
    return "*" + to_string(*x);
  else
    return "null";
}

} // namespace broker
