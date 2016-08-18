#include <tuple>

#include "broker/network_info.hh"

namespace broker {

network_info::network_info(std::string addr, uint16_t port)
  : address{std::move(addr)},
    port{port} {
}

bool operator==(const network_info& x, const network_info& y) {
  return x.address == y.address && x.port == y.port;
}

bool operator<(const network_info& x, const network_info& y) {
  return std::tie(x.address, x.port) < std::tie(y.address, y.port);
}

} // namespace broker
