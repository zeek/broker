#include "broker/peer_flags.hh"

namespace broker {

bool convert(peer_flags src, int& dst) noexcept {
  dst = static_cast<int>(src);
  return true;
}

bool convert(int src, peer_flags& dst) noexcept {
  if (auto masked = src & 0x0F; masked == src) {
    dst = static_cast<peer_flags>(src);
    return true;
  } else {
    return false;
  }
}

} // namespace broker
