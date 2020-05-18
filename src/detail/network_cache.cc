#include "broker/detail/network_cache.hh"

#include "broker/logger.hh"

namespace broker {
namespace detail {

namespace {

template <class Handle>
auto type_erase(const Handle& x) {
  return caf::actor_cast<caf::actor>(x);
}

} // namespace

network_cache::network_cache(caf::event_based_actor* selfptr) : self(selfptr) {
  auto& sys = self->home_system();
  if (sys.has_middleman())
    mm_ = type_erase(sys.middleman().actor_handle());
}

void network_cache::set_use_ssl(bool use_ssl) {
  BROKER_INFO("initiating connections using" << (use_ssl ? "SSL" : "no SSL"));
  auto& sys = self->home_system();
  mm_ = type_erase(use_ssl ? sys.openssl_manager().actor_handle()
                           : sys.middleman().actor_handle());
}

caf::result<caf::actor> network_cache::fetch(const network_info& x) {
  auto rp = self->make_response_promise();
  fetch(x,
        [=](caf::actor hdl) mutable {
          rp.deliver(std::move(hdl));
        },
        [=](caf::error err) mutable {
          rp.deliver(std::move(err));
        });
  return rp;
}

caf::optional<caf::actor> network_cache::find(const network_info& x) {
  auto i = hdls_.find(x);
  if (i != hdls_.end())
    return i->second;
  return caf::none;
}

caf::optional<network_info> network_cache::find(const caf::actor& x) {
  auto i = addrs_.find(x);
  if (i != addrs_.end())
    return i->second;
  return caf::none;
}

void network_cache::add(const caf::actor& x, const network_info& y) {
  BROKER_TRACE(BROKER_ARG(x) << BROKER_ARG(y));
  addrs_.emplace(x, y);
  hdls_.emplace(y, x);
}

void network_cache::remove(const caf::actor& x) {
  BROKER_TRACE(BROKER_ARG(x));
  auto i = addrs_.find(x);
  if (i == addrs_.end())
    return;
  BROKER_DEBUG("remove cache entry to peer:" << x);
  hdls_.erase(i->second);
  addrs_.erase(i);
}

void network_cache::remove(const network_info& x) {
  auto i = hdls_.find(x);
  if (i == hdls_.end())
    return;
  BROKER_DEBUG("remove cache entry to peer:" << i->second);
  addrs_.erase(i->second);
  hdls_.erase(i);
}

} // namespace detail
} // namespace broker
