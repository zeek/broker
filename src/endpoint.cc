#include "broker/logger.hh" // Must come before any CAF include.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/endpoint.hh"
#include "broker/event_subscriber.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/subscriber.hh"
#include "broker/timeout.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/core_actor.hh"
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

endpoint::endpoint(configuration config)
  : config_(std::move(config)),
    system_(config_) {
  core_ = system_.spawn(detail::core_actor, detail::filter_type{});
}

endpoint::~endpoint() {
  anon_send_exit(core_, caf::exit_reason::user_shutdown);
}

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
  char const* addr = address.empty() ? nullptr : address.c_str();
  auto res = system_.middleman().publish(core(), port, addr, true);
  return res ? *res : 0;
}

void endpoint::peer(const std::string& address, uint16_t port,
                    timeout::seconds retry) {
  caf::scoped_actor self{core()->home_system()};
  self->request(core_, caf::infinite, atom::peer::value,
                network_info{address, port}, retry, uint32_t{0})
  .receive(
    [](const caf::actor&) {
      // nop
    },
    [&](caf::error& err) {
      CAF_LOG_DEBUG("Cannot peer to" << address << "on port"
                    << port << ":" << err);
    }
  );
}

void endpoint::unpeer(const std::string& address, uint16_t port) {
  caf::anon_send(core(), atom::unpeer::value, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{core()->home_system()};
  auto msg = caf::make_message(atom::get::value, atom::peer::value);
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

void endpoint::publish(topic t, data d) {
  caf::anon_send(core(), atom::publish::value, std::move(t), std::move(d));
}

void endpoint::publish(const endpoint_info& dst, topic t, data d) {
  caf::anon_send(core(), atom::publish::value, dst, std::move(t), std::move(d));
}

publisher endpoint::make_publisher(topic ts) {
  return {*this, std::move(ts)};
}

event_subscriber endpoint::make_event_subscriber(bool receive_statuses) {
  return {*this, receive_statuses};
}

subscriber endpoint::make_subscriber(std::vector<topic> ts, long max_qsize) {
  return {*this, std::move(ts), max_qsize};
}

caf::actor endpoint::make_actor(actor_init_fun f) {
  return system_.spawn([=](caf::event_based_actor* self) {
    f(self);
  });
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                      backend_options opts) {
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{system_};
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
