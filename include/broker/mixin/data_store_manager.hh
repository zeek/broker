#pragma once

#include <string>
#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/behavior.hpp>

#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/detail/clone_actor.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/make_backend.hh"
#include "broker/detail/master_actor.hh"
#include "broker/detail/master_resolver.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/topic.hh"

namespace broker::mixin {

template <class Base, class Subtype>
class data_store_manager : public Base {
public:
  // --- member types ----------------------------------------------------------

  using super = Base;

  using extended_base = data_store_manager;

  // --- constants -------------------------------------------------------------

  static constexpr auto spawn_flags = caf::linked + caf::lazy_init;

  // --- construction and destruction ------------------------------------------

  template <class... Ts>
  explicit data_store_manager(endpoint::clock* clock, Ts&&... xs)
    : super(std::forward<Ts>(xs)...), clock_(clock) {
    // nop
  }

  // -- properties -------------------------------------------------------------

  /// Returns whether a master for `name` probably exists already on one of our
  /// peers.
  bool has_remote_master(const std::string& name) {
    // If we don't have a master recorded locally, we could still have a
    // propagated filter to a remote core hosting a master.
    return dref().has_remote_subscriber(name / topics::master_suffix);
  }

  const auto& masters() const noexcept {
    return masters_;
  }

  const auto& clones() const noexcept {
    return clones_;
  }

  // -- data store management --------------------------------------------------

  /// Attaches a master for given store to this peer.
  caf::result<caf::actor> attach_master(const std::string& name,
                                        backend backend_type,
                                        backend_options opts) {
    BROKER_TRACE(BROKER_ARG(name)
                 << BROKER_ARG(backend_type) << BROKER_ARG(opts));
    auto i = masters_.find(name);
    if (i != masters_.end())
      return i->second;
    if (has_remote_master(name)) {
      BROKER_WARNING("remote master with same name exists already");
      return ec::master_exists;
    }
    auto ptr = detail::make_backend(backend_type, std::move(opts));
    BROKER_ASSERT(ptr != nullptr);
    BROKER_INFO("spawning new master:" << name);
    auto self = super::self();
    auto ms = self->template spawn<spawn_flags>(detail::master_actor, self,
                                                name, std::move(ptr), clock_);
    filter_type filter{name / topics::master_suffix};
    if (auto err = dref().add_store(ms, filter))
      return err;
    masters_.emplace(name, ms);
    return ms;
  }

  /// Attaches a clone for given store to this peer.
  caf::result<caf::actor>
  attach_clone(const std::string& name, double resync_interval,
               double stale_interval, double mutation_buffer_interval) {
    BROKER_TRACE(BROKER_ARG(name)
                 << BROKER_ARG(resync_interval) << BROKER_ARG(stale_interval)
                 << BROKER_ARG(mutation_buffer_interval));
    auto i = masters_.find(name);
    if (i != masters_.end()) {
      BROKER_WARNING("attempted to run clone & master on the same endpoint");
      return ec::no_such_master;
    }
    BROKER_INFO("spawning new clone:" << name);
    auto self = super::self();
    auto cl = self->template spawn<spawn_flags>(detail::clone_actor, self, name,
                                                resync_interval, stale_interval,
                                                mutation_buffer_interval,
                                                clock_);
    filter_type filter{name / topics::clone_suffix};
    if (auto err = dref().add_store(cl, filter))
      return err;
    clones_.emplace(name, cl);
    return cl;
  }

  /// Returns whether the master for the given store runs at this peer.
  caf::result<caf::actor> get_master(const std::string& name) {
    auto i = masters_.find(name);
    if (i != masters_.end())
      return i->second;
    return ec::no_such_master;
  }

  /// Instructs the master of the given store to generate a snapshot.
  void snapshot(const std::string& name, caf::actor& clone) {
    auto msg = make_internal_command<snapshot_command>(super::self(),
                                                       std::move(clone));
    dref().publish(make_command_message(name / topics::master_suffix, msg));
  }

  /// Detaches all masters and clones by sending exit messages to the
  /// corresponding actors.
  void detach_stores() {
    auto self = super::self();
    auto f = [&](auto& container) {
      for (auto& kvp : container)
        self->send_exit(kvp.second, caf::exit_reason::user_shutdown);
      container.clear();
    };
    f(masters_);
    f(clones_);
  }

  // -- factories --------------------------------------------------------------

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return super::make_behavior(
      std::move(fs)...,
      lift<atom::store, atom::clone, atom::attach>(d, &Subtype::attach_clone),
      lift<atom::store, atom::master, atom::attach>(d, &Subtype::attach_master),
      lift<atom::store, atom::master, atom::get>(d, &Subtype::get_master),
      lift<atom::store, atom::master, atom::snapshot>(d, &Subtype::snapshot),
      lift<atom::shutdown, atom::store>(d, &Subtype::detach_stores),
      [this](atom::store, atom::master, atom::resolve, std::string& name,
             caf::actor& who_asked) {
        // TODO: get rid of the who_asked parameter and use proper
        // request/response semantics with forwarding/dispatching
        auto self = super::self();
        auto i = masters_.find(name);
        if (i != masters_.end()) {
          self->send(who_asked, atom::master::value, i->second);
          return;
        }
        auto peers = dref().peer_handles();
        if (peers.empty()) {
          BROKER_INFO("no peers to ask for the master");
          self->send(who_asked, atom::master::value,
                     make_error(ec::no_such_master, "no peers"));
          return;
        }
        auto resolver
          = self->template spawn<caf::lazy_init>(detail::master_resolver);
        self->send(resolver, std::move(peers), std::move(name),
                   std::move(who_asked));
      });
  }

private:
  // -- CRTP scaffold ----------------------------------------------------------

  Subtype& dref() {
    return static_cast<Subtype&>(*this);
  }

  // -- member variables -------------------------------------------------------

  /// Enables manual time management by the user.
  endpoint::clock* clock_;

  /// Stores all master actors created by this core.
  std::unordered_map<std::string, caf::actor> masters_;

  /// Stores all clone actors created by this core.
  std::unordered_multimap<std::string, caf::actor> clones_;
};

} // namespace broker::mixin
