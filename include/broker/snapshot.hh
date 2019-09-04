#ifndef BROKER_SNAPSHOT_HH
#define BROKER_SNAPSHOT_HH

#include <unordered_map>

#include "broker/data.hh"

namespace broker {

/// A snapshot of a data store's contents.
using snapshot = std::unordered_map<data, data>;

} // namespace broker

#endif // BROKER_SNAPSHOT_HH
