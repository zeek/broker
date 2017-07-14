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
    system_(config_),
    await_stores_on_shutdown_(false) {
  CAF_LOG_INFO("creating endpoint");
  core_ = system_.spawn(detail::core_actor, detail::filter_type{});
}

endpoint::~endpoint() {
  CAF_LOG_INFO("destroying endpoint");
  shutdown();
}

void endpoint::shutdown() {
  CAF_LOG_INFO("shutting down endpoint");
  if (!await_stores_on_shutdown_) {
    CAF_LOG_DEBUG("tell core actor to terminate stores");
    anon_send(core_, atom::shutdown::value, atom::store::value);
  }
  if (!children_.empty()) {
    caf::scoped_actor self{system_};
    CAF_LOG_DEBUG("send exit messages to all children");
    for (auto& child : children_)
      self->send_exit(child, caf::exit_reason::user_shutdown);
    CAF_LOG_DEBUG("wait until all children have terminated");
    self->wait_for(children_);
    children_.clear();
  }
  CAF_LOG_DEBUG("send shutdown message to core actor");
  anon_send(core_, atom::shutdown::value);
}

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
  CAF_LOG_INFO("listening on" << (address + ":" + std::to_string(port)));
  char const* addr = address.empty() ? nullptr : address.c_str();
  auto res = system_.middleman().publish(core(), port, addr, true);
  return res ? *res : 0;
}

bool endpoint::peer(const std::string& address, uint16_t port,
                    timeout::seconds retry) {
  CAF_LOG_TRACE(CAF_ARG(address) << CAF_ARG(port) << CAF_ARG(retry));
  CAF_LOG_INFO("starting to peer with" << (address + ":" + std::to_string(port)) << "retry:" << to_string(retry) << "[synchronous]");
  bool result = false;
  caf::scoped_actor self{system_};
  self->request(core_, caf::infinite, atom::peer::value,
                network_info{address, port}, retry, uint32_t{0})
  .receive(
    [&](const caf::actor&) {
      result = true;
    },
    [&](caf::error& err) {
      CAF_LOG_DEBUG("Cannot peer to" << address << "on port"
                    << port << ":" << err);
    }
  );
  return result;
}

void endpoint::peer_nosync(const std::string& address, uint16_t port,
			   timeout::seconds retry) {
  CAF_LOG_TRACE(CAF_ARG(address) << CAF_ARG(port));
  CAF_LOG_INFO("starting to peer with" << (address + ":" + std::to_string(port)) << "retry:" << to_string(retry) << "[asynchronous]");
  caf::anon_send(core(), atom::peer::value, network_info{address, port}, retry, uint32_t{0});
}

bool endpoint::unpeer(const std::string& address, uint16_t port) {
  CAF_LOG_TRACE(CAF_ARG(address) << CAF_ARG(port));
  CAF_LOG_INFO("stopping to peer with" << address << ":" << port << "[synchronous]");
  bool result = false;
  caf::scoped_actor self{system_};
  self->request(core_, caf::infinite, atom::unpeer::value,
                network_info{address, port})
  .receive(
    [&](void) {
      result = true;
    },
    [&](caf::error& err) {
      CAF_LOG_DEBUG("Cannot unpeer from" << address << "on port"
                    << port << ":" << err);
    }
  );

  return result;
}

void endpoint::unpeer_nosync(const std::string& address, uint16_t port) {
  CAF_LOG_TRACE(CAF_ARG(address) << CAF_ARG(port));
  CAF_LOG_INFO("stopping to peer with " << address << ":" << port << "[asynchronous]");
  caf::anon_send(core(), atom::unpeer::value, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{system_};
  self->request(core(), timeout::core, atom::get::value, atom::peer::value)
  .receive(
    [&](std::vector<peer_info>& peers) {
      result = std::move(peers);
    },
    [](const caf::error& e) {
      detail::die("failed to get peers:", to_string(e));
    }
  );
  return result;
}

std::vector<topic> endpoint::peer_subscriptions() const {
  std::vector<topic> result;
  caf::scoped_actor self{system_};
  self->request(core(), timeout::core, atom::get::value,
                atom::peer::value, atom::subscriptions::value)
  .receive(
    [&](std::vector<topic>& ts) {
      result = std::move(ts);
    },
    [](const caf::error& e) {
      detail::die("failed to get peers:", to_string(e));
    }
  );
  return result;
}

static std::string data_to_string(data& d)
	{
	std::string s;
	convert(d, s);
	return s;
	}

void endpoint::publish(topic t, data d) {
  CAF_LOG_INFO("publishing message" << data_to_string(d) << "to topic" << t.string());
  caf::anon_send(core(), atom::publish::value, std::move(t), std::move(d));
}

void endpoint::publish(const endpoint_info& dst, topic t, data d) {
  CAF_LOG_INFO("publishing message" << data_to_string(d) << "to" << dst.node << "for topic" << t.string());
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
  auto hdl = system_.spawn([=](caf::event_based_actor* self) {
    // "Hide" unhandled-exception warning if users throw.
    self->set_exception_handler(
      [](caf::scheduled_actor* thisptr, std::exception_ptr& e) -> caf::error {
        return caf::exit_reason::unhandled_exception;
      }
    );
    // Run callback.
    f(self);
  });
  children_.emplace_back(hdl);
  return hdl;
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                      backend_options opts) {
  CAF_LOG_INFO("attaching master store" << name << "of type" << type);
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
  CAF_LOG_INFO("attaching clone store" << name);
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
