#include "broker/logger.hh" // Must come before any CAF include.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/blocking_endpoint.hh"
#include "broker/endpoint.hh"
#include "broker/nonblocking_endpoint.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/die.hh"

namespace broker {

namespace {

auto exit_deleter = [](caf::actor* a) {
  caf::anon_send_exit(*a, caf::exit_reason::user_shutdown);
  delete a;
};

} // namespace <anonymous>

endpoint::endpoint(const blocking_endpoint& other)
  : core_{other.core_},
    subscriber_{other.subscriber_} {
  // nop
}

endpoint::endpoint(const nonblocking_endpoint& other)
  : core_{other.core_},
    subscriber_{other.subscriber_} {
  // nop
}

endpoint& endpoint::operator=(const blocking_endpoint& other) {
  core_ = other.core_;
  subscriber_ = other.subscriber_;
  return *this;
}

endpoint& endpoint::operator=(const nonblocking_endpoint& other) {
  core_ = other.core_;
  subscriber_ = other.subscriber_;
  return *this;
}


endpoint_info endpoint::info() const {
  auto result = endpoint_info{core()->node(), core()->id(), {}};
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

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
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
  bound = core()->home_system().middleman().publish(core(), port, addr);
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
}

void endpoint::peer(const endpoint& other) {
  caf::anon_send(core(), atom::peer::value, other.core());
}

void endpoint::peer(const std::string& address, uint16_t port) {
  caf::anon_send(core(), atom::peer::value, network_info{address, port});
}

void endpoint::unpeer(const endpoint& other) {
  caf::anon_send(core(), atom::unpeer::value, other.core(), subscriber_);
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

void endpoint::publish(topic t, data d) {
  caf::anon_send(core(), std::move(t), caf::make_message(std::move(d)), core());
}

void endpoint::init_core(caf::actor core) {
  BROKER_ASSERT(subscriber_);
  // This local variable is just a workaround for the lack of initialized lamda
  // captures, wich are C++14.
  auto subscriber = subscriber_;
  core->attach_functor([=] {
    caf::anon_send_exit(subscriber, caf::exit_reason::user_shutdown);
  });
  auto ptr = new caf::actor{std::move(core)};
  core_ = std::shared_ptr<caf::actor>(ptr, exit_deleter);
}

const caf::actor& endpoint::core() const {
  return *core_;
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                        backend_options opts) {
  expected<store> result{sc::unspecified};
  caf::scoped_actor self{core()->home_system()};
  auto msg = caf::make_message(atom::store::value, atom::master::value,
                               atom::attach::value, std::move(name), type,
                               std::move(opts));
  self->request(core(), timeout::core, std::move(msg)).receive(
    [&](caf::actor& master) {
      result = store{std::move(master)};
    },
    [&](caf::error& e) {
      result = std::move(e);
    }
  );
  return result;
}

expected<store> endpoint::attach_clone(std::string name) {
  expected<store> result{sc::unspecified};
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), timeout::core, atom::store::value, atom::clone::value,
                atom::attach::value, std::move(name)).receive(
    [&](caf::actor& clone) {
      result = store{std::move(clone)};
    },
    [&](caf::error& e) {
      result = std::move(e);
    }
  );
  return result;
}

} // namespace broker
