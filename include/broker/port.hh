#pragma once

#include <cstdint>
#include <string>

#include "broker/detail/operators.hh"

namespace broker {

/// A transport-layer port.
class port : detail::totally_ordered<port> {
public:
  using number_type = uint16_t;

  enum class protocol : uint8_t {
    unknown,
    tcp,
    udp,
    icmp,
  };

  /// Default construct empty port, 0/unknown.
  port();

  /// Construct a port from number/protocol.
  port(number_type num, protocol p);

  /// @return The port number.
  number_type number() const;

  /// @return The port's transport protocol.
  protocol type() const;

  friend bool operator==(const port& lhs, const port& rhs);
  friend bool operator<(const port& lhs, const port& rhs);

  template <class Inspector>
  friend bool inspect(Inspector& f, port& x) {
    return f.object(x).fields(f.field("num", x.num_),
                              f.field("proto", x.proto_));
  }

private:
  number_type num_;
  protocol proto_;
};

template <class Inspector>
bool inspect(Inspector& f, port::protocol& x) {
  auto get = [&] { return static_cast<uint8_t>(x); };
  auto set = [&](uint8_t val) {
    if (val <= static_cast<uint8_t>(port::protocol::icmp)) {
      x = static_cast<port::protocol>(val);
      return true;
    } else {
      return false;
    }
  };
  return f.apply(get, set);
}

/// @relates port
bool operator==(const port& lhs, const port& rhs);

/// @relates port
bool operator<(const port& lhs, const port& rhs);

/// @relates port
bool convert(const port& p, std::string& str);

/// @relates port
bool convert(const std::string& str, port& p);

} // namespace broker

namespace std {
template <>
struct hash<broker::port> {
  size_t operator()(const broker::port&) const;
};
} // namespace std;
