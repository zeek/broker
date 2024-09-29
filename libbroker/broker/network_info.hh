#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "broker/detail/comparable.hh"
#include "broker/timeout.hh"

namespace broker {

/// Represents an IP address and TCP port combination.
struct network_info : detail::comparable<network_info> {
  network_info() = default;
  network_info(std::string addr, uint16_t port,
               timeout::seconds retry = timeout::seconds());

  std::string address;
  uint16_t port;
  timeout::seconds retry;
  bool has_retry_time() const noexcept {
    return retry.count() != 0;
  }

  int compare(const network_info& other) const;
};

/// @relates network_info
template <class Inspector>
bool inspect(Inspector& f, network_info& x) {
  return f.object(x).fields(f.field("address", x.address),
                            f.field("port", x.port), f.field("retry", x.retry));
}

/// @relates network_info
std::string to_string(const network_info& x);

/// @relates network_info
std::string to_string(const std::optional<network_info>& x);

} // namespace broker

namespace std {

template <>
struct hash<broker::network_info> {
  size_t operator()(const broker::network_info& x) const {
    hash<string> f;
    return f(x.address) ^ static_cast<size_t>(x.port);
  }
};

} // namespace std
