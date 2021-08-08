#include "broker/peer_status.hh"

#include <cstddef>

namespace broker {

namespace {

constexpr const char* peer_status_strings[] = {
  "initialized",  "connecting",   "connected", "peered",
  "disconnected", "reconnecting", "unknown",
};

} // namespace

const char* to_string(peer_status x) {
  return peer_status_strings[static_cast<size_t>(x)];
}

} // namespace broker
