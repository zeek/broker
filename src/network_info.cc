#include <tuple>
#include <utility>
#include <chrono>
#include <string>

#include "broker/network_info.hh"

namespace broker {

network_info::network_info(std::string addr, uint16_t port_given,
                           timeout::seconds retry_timeout)
  : address{std::move(addr)}, port{port_given}, retry{retry_timeout} {
}

bool operator==(const network_info& x, const network_info& y) {
  return x.address == y.address && x.port == y.port;
}

bool operator<(const network_info& x, const network_info& y) {
  return std::tie(x.address, x.port) < std::tie(y.address, y.port);
}

} // namespace broker
