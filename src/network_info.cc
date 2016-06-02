#include "broker/network_info.hh"

namespace broker {

bool operator==(const network_info& x, const network_info& y) {
  return x.address == y.address && x.port == y.port;
}

bool operator!=(const network_info& x, const network_info& y) {
  return !(x == y);
}

} // namespace broker
