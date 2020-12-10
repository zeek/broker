#include "broker/network_info.hh"

#include <chrono>
#include <string>
#include <tuple>
#include <utility>

#include "caf/uri.hpp"

namespace broker {

network_info::network_info(std::string addr, uint16_t port,
                           timeout::seconds retry)
  : address{std::move(addr)}, port{port}, retry{retry} {
}

bool convert(const caf::uri& from, network_info& to){
  if (from.empty())
    return false;
  if (from.scheme() != "tcp")
    return false;
  const auto& auth = from.authority();
  if (auth.empty())
    return false;
  auto set_host = [&](const auto& host) {
    if constexpr (std::is_same<decltype(host), const std::string&>::value)
      to.address = host;
    else
      to.address = to_string(host);
  };
  to.port = auth.port;
  return true;
}

bool operator==(const network_info& x, const network_info& y) {
  return x.address == y.address && x.port == y.port;
}

bool operator<(const network_info& x, const network_info& y) {
  return std::tie(x.address, x.port) < std::tie(y.address, y.port);
}

std::string to_string(const network_info& info) {
  using std::to_string;
  return info.address + ':' + to_string(info.port);
}

} // namespace broker
