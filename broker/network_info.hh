#ifndef BROKER_NETWORK_INFO_HH
#define BROKER_NETWORK_INFO_HH

#include <cstdint>
#include <string>

namespace broker {

/// Represents an IP address and TCP port combination.
struct network_info {
  std::string address;
  uint16_t port;
};

/// @relates network_info
bool operator==(const network_info& x, const network_info& y);

/// @relates network_info
bool operator!=(const network_info& x, const network_info& y);

} // namespace broker

#endif // BROKER_NETWORK_INFO_HH
