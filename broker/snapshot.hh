#ifndef BROKER_SNAPSHOT_HH
#define BROKER_SNAPSHOT_HH

#include <unordered_map>

#include "broker/data.hh"

namespace broker {

/// A snapshot of a data store's contents along with the sequence number
/// that corresponds to it.
struct snapshot {
  std::unordered_map<data, data> entries;
};

/// @relates snapshot
inline bool operator==(const snapshot& lhs, const snapshot& rhs) {
  return lhs.entries == rhs.entries;
}

/// @relates snapshot
inline bool operator!=(const snapshot& lhs, const snapshot& rhs) {
  return !(lhs == rhs);
}

/// @relates snapshot
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, snapshot& s) {
  return f(s.entries);
}

} // namespace broker

#endif // BROKER_SNAPSHOT_HH
