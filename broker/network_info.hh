#ifndef BROKER_NETWORK_INFO_HH
#define BROKER_NETWORK_INFO_HH

#include <cstdint>
#include <string>

#include "broker/detail/operators.hh"

namespace broker {

/// Represents an IP address and TCP port combination.
struct network_info : detail::totally_ordered<network_info> {
  network_info() = default;
  network_info(std::string addr, uint16_t port);

  std::string address;
  uint16_t port;
};

/// @relates network_info
template <class Processor>
void serialize(Processor& proc, network_info& info) {
  proc & info.address;
  proc & info.port;
}

/// @relates network_info
bool operator==(const network_info& x, const network_info& y);

/// @relates network_info
bool operator<(const network_info& x, const network_info& y);

/// @relates network_info
inline std::string to_string(const network_info& info) {
  using std::to_string;
  return info.address + ':' + to_string(info.port);
}

} // namespace broker

#endif // BROKER_NETWORK_INFO_HH
