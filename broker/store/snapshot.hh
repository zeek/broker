#ifndef BROKER_STORE_SNAPSHOT_HH
#define BROKER_STORE_SNAPSHOT_HH

#include <unordered_map>

#include "broker/data.hh"

namespace broker {
namespace store {

/// A snapshot of a data store's contents along with the sequence number
/// that corresponds to it.
struct snapshot {
  std::unordered_map<data, data> entries;
};

template <class Processor>
void serialize(Processor& proc, snapshot& s) {
  proc & s.entries;
}

} // namespace store
} // namespace broker

#endif // BROKER_STORE_SNAPSHOT_HH
