#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

#include "broker/detail/enum_inspect.hh"
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

  using protocol_ut = std::underlying_type_t<protocol>;

  /// Default construct empty port, 0/unknown.
  port();

  /// Construct a port from number/protocol.
  port(number_type num, protocol p);

  /// @return The port number.
  number_type number() const;

  /// @return The port's transport protocol.
  protocol type() const;

  size_t hash() const;

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

/// @relates port
bool operator==(const port& lhs, const port& rhs);

/// @relates port
bool operator<(const port& lhs, const port& rhs);

/// @relates port
bool convert(const port& p, std::string& str);

/// @relates port
bool convert(const std::string& str, port& p);

/// @relates port
bool convert(port::protocol, port::protocol_ut&);

/// @relates port
bool convert(port::protocol, std::string&);

/// @relates port
bool convert(port::protocol_ut, port::protocol&);

/// @relates port
bool convert(const std::string&, port::protocol&);

/// @relates port
template <class Inspector>
bool inspect(Inspector& f, port::protocol& x) {
  return detail::enum_inspect(f, x);
}

} // namespace broker

namespace std {

template <>
struct hash<broker::port> {
  size_t operator()(const broker::port& x) const {
    return x.hash();
  }
};

} // namespace std;
