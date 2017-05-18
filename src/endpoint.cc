#include "broker/logger.hh" // Must come before any CAF include.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/context.hh"
#include "broker/endpoint.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/die.hh"

namespace broker {

endpoint_info endpoint::info() const {
  auto result = endpoint_info{core()->node(), {}};
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), timeout::core, atom::network::value,
                atom::get::value).receive(
    [&](std::string& address, uint16_t port) {
      if (port > 0)
        result.network = network_info{std::move(address), port};
    },
    [](const caf::error& e) {
      detail::die("failed to get endpoint network info:", to_string(e));
    }
  );
  return result;
}

endpoint::endpoint(context& ctx) : ctx_(ctx) {
  // nop
}

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
  char const* addr = address.empty() ? nullptr : address.c_str();
  auto res = ctx_.system().middleman().publish(core(), port, addr);
  return res ? *res : 0;
  /*
  auto res = caf::io::publish(ctx_.core(), port);

  auto bound = caf::expected<uint16_t>{caf::error{}};
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), timeout::core, atom::network::value,
                atom::get::value).receive(
    [&](const std::string&, uint16_t p) {
      bound = p;
    },
    [](const caf::error& e) {
      detail::die("failed to get endpoint network info:", to_string(e));
    }
  );
  if (*bound > 0)
    return 0; // already listening
  char const* addr = address.empty() ? nullptr : address.c_str();
  bound = core()->home_system().middleman().publish(core(), port, addr, true);
  if (!bound)
    return 0;
  self->request(core(), timeout::core, atom::network::value, atom::put::value,
                address, *bound).receive(
    [](atom::ok) {
      // nop
    },
    [](const caf::error& e) {
      detail::die("failed to set endpoint network info:", to_string(e));
    }
  );
  return *bound;
  */
}

/* TODO: reimplement
void endpoint::peer(const std::string& address, uint16_t port) {
  CAF_LOG_TRACE(CAF_ARG(address) << CAF_ARG(port));
  auto hdl = ctx_.system().middleman().remote_actor(address, port);
  if (hdl) {
    CAF_LOG_DEBUG(CAF_ARG(core()) << CAF_ARG(hdl));
    anon_send(core(), atom::peer::value, std::move(*hdl));
  }
  //caf::anon_send(core(), atom::peer::value, network_info{address, port});
}

void endpoint::unpeer(const std::string& address, uint16_t port) {
  caf::anon_send(core(), atom::unpeer::value, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{core()->home_system()};
  auto msg = caf::make_message(atom::peer::value, atom::get::value);
  self->request(core(), timeout::core, std::move(msg)).receive(
    [&](std::vector<peer_info>& peers) {
      result = std::move(peers);
    },
    [](const caf::error& e) {
      detail::die("failed to get peers:", to_string(e));
    }
  );
  return result;
}
*/

void endpoint::publish(topic t, data d) {
  caf::anon_send(core(), atom::publish::value, std::move(t), std::move(d));
}

const caf::actor& endpoint::core() const {
  return ctx_.core();
}

void endpoint::make_actor(actor_init_fun f) {
  ctx_.system().spawn([=](caf::event_based_actor* self) {
    f(self);
  });
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                      backend_options opts) {
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{ctx_.system()};
  self->request(core(), caf::infinite, atom::store::value, atom::master::value,
                atom::attach::value, std::move(name), type, std::move(opts))
  .receive(
    [&](caf::actor& master) {
      res = store{std::move(master)};
    },
    [&](caf::error& e) {
      res = std::move(e);
    }
  );
  return res;
}

expected<store> endpoint::attach_clone(std::string name) {
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), caf::infinite, atom::store::value, atom::clone::value,
                atom::attach::value, std::move(name)).receive(
    [&](caf::actor& clone) {
      res = store{std::move(clone)};
    },
    [&](caf::error& e) {
      res = std::move(e);
    }
  );
  return res;
}

} // namespace broker
