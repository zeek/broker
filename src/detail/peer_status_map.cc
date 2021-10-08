#include "broker/detail/peer_status_map.hh"

namespace broker::detail {

bool peer_status_map::insert(endpoint_id peer) {
  auto st = peer_status::initialized;
  return insert(peer, st);
}

bool peer_status_map::insert(endpoint_id peer, peer_status& desired) {
  std::unique_lock guard{mtx_};
  auto [i, added] = peers_.emplace(peer, desired);
  if (added) {
    return true;
  } else {
    desired = i->second;
    return false;
  }
}

bool peer_status_map::update(endpoint_id peer, peer_status& expected,
                             peer_status desired) {
  std::unique_lock guard{mtx_};
  if (auto i = peers_.find(peer); i != peers_.end()) {
    if (i->second == expected) {
      i->second = desired;
      return true;
    } else {
      expected = i->second;
      return false;
    }
  } else {
    expected = peer_status::unknown;
    return false;
  }
}

bool peer_status_map::remove(endpoint_id peer, peer_status& expected) {
  std::unique_lock guard{mtx_};
  if (auto i = peers_.find(peer); i != peers_.end()) {
    if (i->second == expected) {
      peers_.erase(i);
      return true;
    } else {
      expected = i->second;
      return false;
    }
  } else {
    expected = peer_status::unknown;
    return false;
  }
}

void peer_status_map::remove(endpoint_id peer) {
  std::unique_lock guard{mtx_};
  peers_.erase(peer);
}

peer_status peer_status_map::get(endpoint_id peer) {
  std::unique_lock guard{mtx_};
  if (auto i = peers_.find(peer); i != peers_.end()) {
    return i->second;
  } else {
    return peer_status::unknown;
  }
}

} // namespace broker::detail
