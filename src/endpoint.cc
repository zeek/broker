#include "broker/logger.hh" // Must come before any CAF include.

#include <unordered_set>

#include <caf/config.hpp>
#include <caf/node_id.hpp>
#include <caf/actor_system.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/error.hpp>
#include <caf/duration.hpp>
#include <caf/send.hpp>
#include <caf/actor.hpp>
#include <caf/message.hpp>
#include <caf/io/middleman.hpp>
#include <caf/openssl/publish.hpp>

#include "broker/atoms.hh"
#include "broker/core_actor.hh"
#include "broker/endpoint.hh"
#include "broker/publisher.hh"
#include "broker/status_subscriber.hh"
#include "broker/subscriber.hh"
#include "broker/timeout.hh"

#include "broker/detail/die.hh"

namespace broker {

// --- nested classes ----------------------------------------------------------

endpoint::clock::clock(caf::actor_system* sys, bool use_real_time)
  : sys_(sys),
    real_time_(use_real_time),
    time_since_epoch_(),
    mtx_(),
    pending_(),
    pending_count_() {
  // nop
}

timestamp endpoint::clock::now() const noexcept {
  return real_time_ ? broker::now() : timestamp{time_since_epoch_};
}

void endpoint::clock::advance_time(timestamp t) {
  if (real_time_)
    return;

  if (t <= timestamp{time_since_epoch_})
    return;

  time_since_epoch_ = t.time_since_epoch();

  if (pending_count_ == 0)
    return;

  lock_type guard{mtx_};

  auto it = pending_.begin();

  if (it->first > t)
    return;

  // Note: this function is performance-sensitive in the case of Bro
  // reading pcaps and it's important to not construct this set unless
  // it's actually going to be used.
  std::unordered_set<caf::actor> sync_with_actors;

  while (it != pending_.end() && it->first <= t) {
    auto& pm = it->second;
    caf::anon_send(pm.first, std::move(pm.second));
    sync_with_actors.emplace(pm.first);
    it = pending_.erase(it);
    --pending_count_;
  }

  guard.unlock();

  caf::scoped_actor self{*sys_};
  for (auto& who : sync_with_actors) {
    self->send(who, atom::sync_point::value, self);
    self->delayed_send(self, timeout::frontend, atom::tick::value);
    self->receive(
      [&](atom::sync_point) {
        // nop
      },
      [&](atom::tick) {
        CAF_LOG_DEBUG("advance_time actor syncing timed out");
      },
      [&](caf::error& e) {
        CAF_LOG_DEBUG("advance_time actor syncing failed");
      }
    );
  }
}

void endpoint::clock::send_later(caf::actor dest, timespan after,
                                 caf::message msg) {
  if (real_time_) {
    auto& sc = sys_->clock();
    auto t = sc.now() + after;
    auto me = caf::make_mailbox_element(nullptr, caf::make_message_id(),
                                        caf::no_stages, std::move(msg));
    sc.schedule_message(t, caf::actor_cast<caf::strong_actor_ptr>(dest),
                        std::move(me));
    return;
  }
  lock_type guard{mtx_};
  auto t = this->now() + after;
  pending_.emplace(t, pending_msg_type{std::move(dest), std::move(msg)});
  ++pending_count_;
}

// --- endpoint class ----------------------------------------------------------

caf::node_id endpoint::node_id() const {
  return core()->node();
}

endpoint::endpoint(configuration config)
  : config_(std::move(config)),
    await_stores_on_shutdown_(false),
    destroyed_(false) {
  new (&system_) caf::actor_system(config_);
  clock_ = new clock(&system_, config_.options().use_real_time);
  if (( !config_.options().disable_ssl) && !system_.has_openssl_manager())
      detail::die("CAF OpenSSL manager is not available");
  BROKER_INFO("creating endpoint");
  core_ = system_.spawn(core_actor, filter_type{}, config_.options(), clock_);
}

endpoint::~endpoint() {
  BROKER_INFO("destroying endpoint");
  shutdown();
}

void endpoint::shutdown() {
  BROKER_INFO("shutting down endpoint");
  if (destroyed_)
    return;
  destroyed_ = true;
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
  core_ = nullptr;
  system_.~actor_system();
  delete clock_;
  clock_ = nullptr;
}

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
  BROKER_INFO("listening on" << (address + ":" + std::to_string(port)) << (config_.options().disable_ssl ? "(no SSL)" : "(SSL)"));
  char const* addr = address.empty() ? nullptr : address.c_str();
  expected<uint16_t> res = caf::error{};
  if (config_.options().disable_ssl)
    res = system_.middleman().publish(core(), port, addr, true);
  else
    res = caf::openssl::publish(core(), port, addr, true);
  return res ? *res : 0;
}

bool endpoint::peer(const std::string& address, uint16_t port,
                    timeout::seconds retry) {
  CAF_LOG_TRACE(CAF_ARG(address) << CAF_ARG(port) << CAF_ARG(retry));
  BROKER_INFO("starting to peer with" << (address + ":" + std::to_string(port)) << "retry:" << to_string(retry) << "[synchronous]");
  bool result = false;
  caf::scoped_actor self{system_};
  self->request(core_, caf::infinite, atom::peer::value,
                network_info{address, port, retry})
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
  BROKER_INFO("starting to peer with" << (address + ":" + std::to_string(port)) << "retry:" << to_string(retry) << "[asynchronous]");
  caf::anon_send(core(), atom::peer::value, network_info{address, port, retry});
}

bool endpoint::unpeer(const std::string& address, uint16_t port) {
  CAF_LOG_TRACE(CAF_ARG(address) << CAF_ARG(port));
  BROKER_INFO("stopping to peer with" << address << ":" << port << "[synchronous]");
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
  BROKER_INFO("stopping to peer with " << address << ":" << port << "[asynchronous]");
  caf::anon_send(core(), atom::unpeer::value, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{system_};
  self->request(core(), caf::infinite, atom::get::value, atom::peer::value)
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
  self->request(core(), caf::infinite, atom::get::value,
                atom::peer::value, atom::subscriptions::value)
  .receive(
    [&](std::vector<topic>& ts) {
      result = std::move(ts);
    },
    [](const caf::error& e) {
      detail::die("failed to get peer subscriptions:", to_string(e));
    }
  );
  return result;
}

void endpoint::forward(std::vector<topic> ts)
{
  BROKER_INFO("forwarding topics" << ts);
  caf::anon_send(core(), atom::subscribe::value, std::move(ts));
}

void endpoint::publish(topic t, data d) {
  BROKER_INFO("publishing" << std::make_pair(t, d));
  caf::anon_send(core(), atom::publish::value, std::move(t), std::move(d));
}

void endpoint::publish(const endpoint_info& dst, topic t, data d) {
  BROKER_INFO("publishing" << std::make_pair(t, d) << "to" << dst.node);
  caf::anon_send(core(), atom::publish::value, dst, std::move(t), std::move(d));
}

void endpoint::publish(std::vector<value_type> xs) {
  for ( auto& x : xs ) {
    BROKER_INFO("publishing" << x);
    caf::anon_send(core(), atom::publish::value, std::move(x.first), std::move(x.second));
  }
}

publisher endpoint::make_publisher(topic ts) {
  publisher result{*this, std::move(ts)};
  children_.emplace_back(result.worker());
  return result;
}

status_subscriber endpoint::make_status_subscriber(bool receive_statuses) {
  status_subscriber result{*this, receive_statuses};
  children_.emplace_back(result.worker());
  return result;
}

subscriber endpoint::make_subscriber(std::vector<topic> ts, size_t max_qsize) {
  subscriber result{*this, std::move(ts), max_qsize};
  children_.emplace_back(result.worker());
  return result;
}

caf::actor endpoint::make_actor(actor_init_fun f) {
  auto hdl = system_.spawn([=](caf::event_based_actor* self) {
#ifndef CAF_NO_EXCEPTION
    // "Hide" unhandled-exception warning if users throw.
    self->set_exception_handler(
      [](caf::scheduled_actor* thisptr, std::exception_ptr& e) -> caf::error {
        return caf::exit_reason::unhandled_exception;
      }
    );
#endif // CAF_NO_EXCEPTION
    // Run callback.
    f(self);
  });
  children_.emplace_back(hdl);
  return hdl;
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                        backend_options opts) {
  BROKER_INFO("attaching master store" << name << "of type" << type);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{system_};
  self->request(core(), caf::infinite, atom::store::value, atom::master::value,
                atom::attach::value, name, type, std::move(opts))
  .receive(
    [&](caf::actor& master) {
      res = store{std::move(master), std::move(name)};
    },
    [&](caf::error& e) {
      res = std::move(e);
    }
  );
  return res;
}

expected<store> endpoint::attach_clone(std::string name,
                                       double resync_interval,
                                       double stale_interval,
                                       double mutation_buffer_interval) {
  BROKER_INFO("attaching clone store" << name);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), caf::infinite, atom::store::value, atom::clone::value,
                atom::attach::value, name, resync_interval, stale_interval,
                mutation_buffer_interval).receive(
    [&](caf::actor& clone) {
      res = store{std::move(clone), std::move(name)};
    },
    [&](caf::error& e) {
      res = std::move(e);
    }
  );
  return res;
}

} // namespace broker
