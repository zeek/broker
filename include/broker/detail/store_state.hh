#pragma once

#include <string>

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>

#include "broker/expected.hh"
#include "broker/fwd.hh"
#include "broker/timeout.hh"

namespace broker::detail {

struct store_state {
  std::string name;
  caf::actor frontend;
  caf::scoped_actor self;
  request_id req_id = 1;

  store_state(std::string name, caf::actor frontend_hdl)
    : name(std::move(name)),
      frontend(std::move(frontend_hdl)),
      self(frontend->home_system()) {
    BROKER_DEBUG("created state for store" << name);
  }

  ~store_state() {
    BROKER_DEBUG("destroyed state for store" << name);
  }

  template <class T, class... Ts>
  expected<T> request(Ts&&... xs) {
    expected<T> res{T{}};
    self->request(frontend, timeout::frontend, std::forward<Ts>(xs)...)
      .receive([&](T& x) { res = std::move(x); },
               [&](caf::error& e) { res = std::move(e); });
    return res;
  }

  template <class... Ts>
  void anon_send(Ts&&... xs) {
    caf::anon_send(frontend, std::forward<Ts>(xs)...);
  }
};

} // namespace broker::detail
