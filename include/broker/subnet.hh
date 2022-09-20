#pragma once

#include "broker/address.hh"
#include "broker/detail/operators.hh"
#include "broker/fwd.hh"

namespace broker {

/// @relates subnet
void convert(const subnet& sn, std::string& str);

/// @relates subnet
bool convert(const std::string& str, subnet& sn);

/// An IPv4 or IPv6 subnet (an address prefix).
class subnet : detail::totally_ordered<subnet> {
public:
  /// Default construct empty subnet ::/0.
  subnet();

  /// Construct subnet from an address and length.
  subnet(const address& addr, uint8_t length);

  /// @return whether an address is contained within this subnet.
  bool contains(const address& addr) const;

  /// @return the network address of the subnet.
  const address& network() const;

  /// @return the prefix length of the subnet.
  uint8_t length() const;

  size_t hash() const;

  friend bool operator==(const subnet& lhs, const subnet& rhs);
  friend bool operator<(const subnet& lhs, const subnet& rhs);

  template <class Inspector>
  friend bool inspect(Inspector& f, subnet& x) {
    if (f.has_human_readable_format()) {
      auto get = [&x] {
        std::string result;
        convert(x, result);
        return result;
      };
      auto set = [&x](const std::string& str) { return convert(str, x); };
      return f.apply(get, set);
    } else {
      return f.object(x).fields(f.field("net", x.net_), f.field("len", x.len_));
    }
  }

private:
  bool init();

  address net_;
  uint8_t len_;
};

/// @relates subnet
bool operator==(const subnet& lhs, const subnet& rhs);

/// @relates subnet
bool operator<(const subnet& lhs, const subnet& rhs);

} // namespace broker

namespace std {

template <>
struct hash<broker::subnet> {
  size_t operator()(const broker::subnet& x) const {
    return x.hash();
  }
};

} // namespace std
