#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "broker/endpoint_id.hh"
#include "broker/peer_status.hh"

namespace broker::detail {

class peer_status_map {
public:
  bool insert(endpoint_id peer);

  bool insert(endpoint_id peer, peer_status& desired);

  bool update(endpoint_id peer, peer_status& expected, peer_status desired);

  bool remove(endpoint_id peer, peer_status& expected);

private:
  mutable std::mutex mtx_;
  std::unordered_map<endpoint_id, peer_status> peers_;
};

using shared_peer_status_map_ptr = std::shared_ptr<peer_status_map>;

} // namespace broker::detail
