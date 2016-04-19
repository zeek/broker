#ifndef BROKER_STORE_VALUE_HH
#define BROKER_STORE_VALUE_HH

#include "broker/optional.hh"
#include "broker/store/expiration_time.hh"

namespace broker {
namespace store {

/// The "value" part of a key/value entry pairing.
struct value {
  data item;
  optional<expiration_time> expiry;
};

inline bool operator==(const value& lhs, const value& rhs) {
  return lhs.item == rhs.item && lhs.expiry == rhs.expiry;
}

template <class Processor>
void serialize(Processor& proc, value& v, const unsigned) {
  proc& v.item;
  proc& v.expiry;
}

} // namespace store
} // namespace broker

#endif // BROKER_STORE_VALUE_HH
