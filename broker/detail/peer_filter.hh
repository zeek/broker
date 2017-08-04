#ifndef BROKER_DETAIL_PEER_FILTER_HH
#define BROKER_DETAIL_PEER_FILTER_HH

#include <utility>
#include <vector>

#include <caf/actor_addr.hpp>

#include "broker/topic.hh"
#include "broker/detail/prefix_matcher.hh"

namespace broker {
namespace detail {

using peer_filter = std::pair<caf::actor_addr, std::vector<topic>>;

/// Allows a stream to dynamically filter on the sender of a message.
struct peer_filter_matcher {
  caf::actor_addr active_sender;
  template <class T>
  bool operator()(const peer_filter& f, const T& x) const {
    prefix_matcher g;
    return f.first != active_sender && g(f.second, x);
  }
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_PEER_FILTER_HH
