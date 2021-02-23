#pragma once

#include <vector>

#include <caf/fwd.hpp>

#include "broker/fwd.hh"

namespace broker::detail {

/// Central point for all `unipath_manager` instances to enqueue items.
class central_dispatcher {
public:
  virtual ~central_dispatcher();

  /// Forwards given item to remote subscribers.
  virtual void dispatch(const data_message& msg) = 0;

  /// Forwards given item to remote subscribers.
  virtual void dispatch(const command_message& msg) = 0;

  /// Forwards given item to remote subscribers.
  void dispatch(const node_message_content& msg);

  /// Forwards given item to remote subscribers and to local subscribers if
  /// `this_endpoint()` is in `get_receivers(msg)`.
  virtual void dispatch(node_message&& msg) = 0;

  /// Tries to emit batches on all nested output paths.
  virtual void flush() = 0;

  /// Returns a pointer to the actor that owns this dispatcher.
  virtual caf::event_based_actor* this_actor() noexcept = 0;

  /// Returns the ID for this Broker endpoint.
  virtual endpoint_id this_endpoint() const = 0;

  /// Returns the current filter on this Broker endpoint.
  virtual filter_type local_filter() const = 0;

  /// Returns the current logical time on this Broker endpoint.
  virtual alm::lamport_timestamp local_timestamp() const noexcept = 0;

  /// Returns whether the dispatcher is shutting down.
  [[nodiscard]] bool tearing_down() const noexcept {
    return tearing_down_;
  }

protected:
  bool tearing_down_ = false;
};

} // namespace broker::detail
