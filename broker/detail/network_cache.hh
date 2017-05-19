#ifndef BROKER_DETAIL_NETWORK_CACHE_HPP
#define BROKER_DETAIL_NETWORK_CACHE_HPP

#include <unordered_map>
#include <unordered_set>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/optional.hpp>

#include <caf/io/middleman.hpp>


#include "broker/network_info.hh"

namespace broker {
namespace detail {

/// Maps any number of network addresses to remote actor handles. Actors can be
/// reachable under several addresses for multiple reasons. For example,
/// "127.0.0.1" and "localhost" point to the same network endpoint or an actor
/// can get published to more than one port.
class network_cache {
public:
  using network_info_set = std::unordered_set<network_info>;

  network_cache(caf::event_based_actor* selfptr);

  /// Either returns an actor handle immediately if the entry is cached or
  /// queries the middleman actor and responds later via response promise.
  caf::result<caf::actor> fetch(const network_info& x);

  template <class OnResult, class OnError>
  void fetch(const network_info& x, OnResult f, OnError g) {
    using namespace caf;
    auto y = find(x);
    if (y) {
      f(*y);
      return;
    }
    self->request(self->home_system().middleman().actor_handle(), infinite,
                  connect_atom::value, x.address, x.port)
    .then(
      [=](const node_id&, strong_actor_ptr& res,
          std::set<std::string>& ifs) mutable {
        if (!ifs.empty())
          g(sec::unexpected_actor_messaging_interface);
        else if (res == nullptr)
          g(sec::no_actor_published_at_port);
        else {
          auto hdl = actor_cast<actor>(std::move(res));
          hdls_.emplace(x, hdl);
          addrs_[hdl].emplace(x);
          f(std::move(hdl));
        }
      },
      [=](error& err) mutable {
        g(std::move(err));
      }
    );
  }

  /// Returns the handle associated to `x`, if any.
  caf::optional<caf::actor> find(const network_info& x);

  /// Returns all known network addresses for `x`.
  network_info_set find(const caf::actor& x);

private:
  // Parent.
  caf::event_based_actor* self;

  // Maps remote actor handles to network addresses.
  std::unordered_map<caf::actor, network_info_set> addrs_;

  // Maps network addresses to remote actor handles.
  std::unordered_map<network_info, caf::actor> hdls_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_NETWORK_CACHE_HPP
