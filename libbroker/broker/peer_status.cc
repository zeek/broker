#include "broker/peer_status.hh"

#include <cstddef>

namespace broker {

namespace {

constexpr const char* peer_status_strings[] = {
  "initialized",  "connecting",   "connected", "peered",
  "disconnected", "reconnecting", "unknown",
};

} // namespace

void convert(const peer_status& x, std::string& str) {
  str = peer_status_strings[static_cast<size_t>(x)];
}

} // namespace broker
