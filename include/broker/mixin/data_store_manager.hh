#pragma once

#include <string>
#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/scheduled_actor/flow.hpp>

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
#include "broker/shutdown_options.hh"
#include "broker/topic.hh"

namespace broker::mixin {

template <class Base>
class data_store_manager : public Base {
public:
  // --- member types ----------------------------------------------------------

  using super = Base;

  using extended_base = data_store_manager;

  // --- constants -------------------------------------------------------------

  static constexpr auto spawn_flags = caf::linked + caf::lazy_init;

  // --- construction and destruction ------------------------------------------

  template <class... Ts>
  data_store_manager(caf::event_based_actor* self, endpoint::clock* clock,
                     Ts&&... xs)
    : super(self, std::forward<Ts>(xs)...), clock_(clock) {
    // nop
  }

  data_store_manager() = delete;

  data_store_manager(const data_store_manager&) = delete;

  data_store_manager& operator=(const data_store_manager&) = delete;

  // -- properties -------------------------------------------------------------

  /// Returns whether a master for `name` probably exists already on one of our
  /// peers.
  bool has_remote_master(const std::string& name) {
    // If we don't have a master recorded locally, we could still have a
    // propagated filter to a remote core hosting a master.
    return this->has_remote_subscriber(name / topic::master_suffix());
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
    if (auto i = masters_.find(name); i != masters_.end())
      return i->second;
    if (has_remote_master(name)) {
      BROKER_WARNING("remote master with same name exists already");
      return ec::master_exists;
    }
    auto ptr = detail::make_backend(backend_type, std::move(opts));
    if (!ptr)
      return ec::backend_failure;
    BROKER_INFO("spawning new master:" << name);
    auto self = super::self();
    auto& sys = self->system();
    using caf::async::make_bounded_buffer_resource;
    auto [con1, prod1] = make_bounded_buffer_resource<command_message>();
    auto [con2, prod2] = make_bounded_buffer_resource<command_message>();
    auto hdl = sys.template spawn<detail::master_actor_type>(
      this->id(), name, std::move(ptr), caf::actor{self}, clock_,
      std::move(con1), std::move(prod2));
    filter_type filter{name / topic::master_suffix()};
    this->subscribe(filter);
    this->select_local_commands(filter).subscribe(prod1);
    this->command_inputs_->add(
      this->self()->make_observable().from_resource(con2));
    masters_.emplace(name, hdl);
    self->link_to(hdl);
    return hdl;
  }

  /// Attaches a clone for given store to this peer.
  caf::result<caf::actor>
  attach_clone(const std::string& name, [[maybe_unused]] double resync_interval,
               [[maybe_unused]] double stale_interval,
               [[maybe_unused]] double mutation_buffer_interval) {
    BROKER_TRACE(BROKER_ARG(name)
                 << BROKER_ARG(resync_interval) << BROKER_ARG(stale_interval)
                 << BROKER_ARG(mutation_buffer_interval));
    if (auto i = masters_.find(name); i != masters_.end()) {
      BROKER_WARNING("attempted to run clone & master on the same endpoint");
      return ec::no_such_master;
    }
    if (auto i = clones_.find(name); i != clones_.end())
      return  i->second;
    BROKER_INFO("spawning new clone:" << name);
    auto self = super::self();
    using std::chrono::duration_cast;
    // TODO: make configurable.
    auto tout = duration_cast<timespan>(fractional_seconds{10});
    auto& sys = self->system();
    using caf::async::make_bounded_buffer_resource;
    auto [con1, prod1] = make_bounded_buffer_resource<command_message>();
    auto [con2, prod2] = make_bounded_buffer_resource<command_message>();
    auto hdl = sys.template spawn<detail::clone_actor_type>(
      this->id(), name, tout, caf::actor{self}, clock_, std::move(con1),
      std::move(prod2));
    filter_type filter{name / topic::clone_suffix()};
    this->subscribe(filter);
    this->select_local_commands(filter).subscribe(prod1);
    this->command_inputs_->add(
      this->self()->make_observable().from_resource(con2));
    clones_.emplace(name, hdl);
    return hdl;
  }

  /// Returns whether the master for the given store runs at this peer.
  caf::result<caf::actor> get_master(const std::string& name) {
    auto i = masters_.find(name);
    if (i != masters_.end())
      return i->second;
    return ec::no_such_master;
  }

  /// Detaches all masters and clones by sending exit messages to the
  /// corresponding actors.
  void detach_stores() {
    BROKER_TRACE(BROKER_ARG2("masters_.size()", masters_.size())
                 << BROKER_ARG2("clones_.size()", clones_.size()));
    auto self = super::self();
    auto f = [&](auto& container) {
      for (auto& kvp : container) {
        self->send_exit(kvp.second, caf::exit_reason::kill);
        // TODO: re-implement graceful shutdown
        // self->send_exit(kvp.second, caf::exit_reason::user_shutdown);
      }
      container.clear();
    };
    f(masters_);
    f(clones_);
  }

  // -- overrides --------------------------------------------------------------

  void shutdown(shutdown_options options) override {
    BROKER_TRACE(BROKER_ARG(options));
    detach_stores();
    super::shutdown(options);
  }

  // -- factories --------------------------------------------------------------

  caf::behavior make_behavior() override {
    using detail::lift;
    return caf::message_handler{
      lift<atom::store, atom::clone, atom::attach>(
        *this, &data_store_manager::attach_clone),
      lift<atom::store, atom::master, atom::attach>(
        *this, &data_store_manager::attach_master),
      lift<atom::store, atom::master, atom::get>(
        *this, &data_store_manager::get_master),
      lift<atom::shutdown, atom::store>(*this,
                                        &data_store_manager::detach_stores),
    }
      .or_else(super::make_behavior());
  }

private:
  // -- member variables -------------------------------------------------------

  /// Enables manual time management by the user.
  endpoint::clock* clock_;

  /// Stores all master actors created by this core.
  std::unordered_map<std::string, caf::actor> masters_;

  /// Stores all clone actors created by this core.
  std::unordered_map<std::string, caf::actor> clones_;
};

} // namespace broker::mixin
