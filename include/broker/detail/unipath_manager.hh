#pragma once

#include <functional>

#include <caf/downstream_manager.hpp>
#include <caf/fwd.hpp>
#include <caf/stream_manager.hpp>

#include "broker/detail/peer_handshake.hh"
#include "broker/fwd.hh"

// This file contains the declaration of `unipath_manager` as well as the
// declarations of the derived types:
//
//
//                        +-----------------------+
//                        |    unipath_manager    |
//                        +-----------+-----------+
//                                    ^
//                                    |
//               +-----------------------------------------+
//               |                    |                    |
//   +-----------+-----------+        |        +-----------+-----------+
//   |     peer_manager      |        |        |    unipath_source     |
//   +-----------------------+        |        +-----------------------+
//                                    |
//                      +-------------+-------------+
//                      |                           |
//          +-----------+-----------+   +-----------+-----------+
//          |   unipath_data_sink   |   | unipath_command_sink  |
//          +-----------------------+   +-----------------------+

namespace broker::detail {

// -- unipath_manager ----------------------------------------------------------

/// A stream manager with at most one inbound and at most one outbound path.
///
/// Unlike CAF's regular stream managers, this manager does *not* forward data
/// from its inbound paths to its outbound paths. In this design, all inbound
/// paths feed into the central dispatcher. The dispatcher then pushes data into
/// the outbound paths.
///
/// We keep both paths to a single peer in one stream manager to model a
/// bidirectional connection. Hence, forwarding from the in- to the outbound
/// path would in our case send messages received from a peer back to itself.
class unipath_manager : public caf::stream_manager {
public:
  // -- friends ----------------------------------------------------------------

  friend class peer_manager;
  friend class unipath_command_sink;
  friend class unipath_data_sink;
  friend class unipath_source;

  // -- member types -----------------------------------------------------------

  using super = caf::stream_manager;

  /// Sum type holding one of three possible derived types of `unipath_manager`.
  using derived_pointer = std::variant<peer_manager*, unipath_command_sink*,
                                       unipath_data_sink*, unipath_source*>;

  // -- nested types -----------------------------------------------------------

  struct observer {
    virtual ~observer();
    virtual void closing(unipath_manager*, bool, const caf::error&) = 0;
    virtual void downstream_connected(unipath_manager*, const caf::actor&) = 0;
    virtual bool finalize_handshake(peer_manager*) = 0;
    virtual void abort_handshake(peer_manager*) = 0;
  };

  // -- constructors, destructors, and assignment operators --------------------

  unipath_manager() = delete;

  unipath_manager(const unipath_manager&) = delete;

  unipath_manager& operator=(const unipath_manager&) = delete;

  ~unipath_manager() override;

  // -- properties -------------------------------------------------------------

  /// Removes the observer, thereby discarding all future events.
  void unobserve() noexcept {
    observer_ = nullptr;
  }

  /// Returns the filter that this manager applies to enqueued items.
  [[nodiscard]] virtual filter_type filter() const = 0;

  /// Sets the filter that this manager applies to enqueued items.
  virtual void filter(filter_type) = 0;

  /// Checks whether the filter of this manager accepts messages for the given
  /// topic.
  [[nodiscard]] virtual bool accepts(const topic&) const noexcept = 0;

  /// Returns the type ID of the message type accepted by this manager.
  [[nodiscard]] virtual caf::type_id_t message_type_id() const noexcept = 0;

  /// Returns the connected actor.
  [[nodiscard]] caf::actor hdl() const noexcept;

  /// Returns the slot for the inbound path or `caf::invalid_stream_slot` if
  /// none exists.
  [[nodiscard]] caf::stream_slot inbound_path_slot() const noexcept;

  /// Returns the slot for the outbound path or `caf::invalid_stream_slot` if
  /// none exists.
  [[nodiscard]] caf::stream_slot outbound_path_slot() const noexcept;

  /// Returns whether this manager has exactly one inbound path.
  [[nodiscard]] bool has_inbound_path() const noexcept {
    return inbound_path_slot() != caf::invalid_stream_slot;
  }

  /// Returns whether this manager has exactly one outbound path.
  [[nodiscard]] bool has_outbound_path() const noexcept {
    return outbound_path_slot() != caf::invalid_stream_slot;
  }

  /// Returns whether this manager has exactly one inbound and exactly one
  /// outbound path.
  [[nodiscard]] bool fully_connected() const noexcept {
    return has_inbound_path() && has_outbound_path();
  }

  /// Returns whether this manager has neither an inbound nor an outbound path.
  [[nodiscard]] bool unconnected() const noexcept {
    return !has_inbound_path() && !has_outbound_path();
  }

  /// Returns the dispatcher that owns this manager.
  [[nodiscard]] auto dispatcher() const noexcept {
    return dispatcher_;
  }

  /// Returns a pointer to the actor that owns this dispatcher.
  [[nodiscard]] caf::event_based_actor* this_actor() noexcept;

  /// Returns the ID for this Broker endpoint.
  [[nodiscard]] endpoint_id this_endpoint() const;

  /// Returns the current filter on this Broker endpoint.
  [[nodiscard]] filter_type local_filter() const;

  /// Returns the current logical time on this Broker endpoint.
  [[nodiscard]] alm::lamport_timestamp local_timestamp() const noexcept;

  /// Returns whether this manager currently blocks incoming batches because the
  /// handshake did not complete yet.
  [[nodiscard]] virtual bool blocks_inputs() const noexcept;

  /// Returns `this` as one of the three possible derived types.
  [[nodiscard]] virtual derived_pointer derived_ptr() noexcept = 0;

  // -- caf::stream_manager overrides ------------------------------------------

  using super::handle;

  bool congested(const caf::inbound_path&) const noexcept override;

  void handle(caf::inbound_path*, caf::downstream_msg::close&) override;

  void handle(caf::inbound_path*, caf::downstream_msg::forced_close&) override;

  void handle(caf::stream_slots, caf::upstream_msg::drop&) override;

  void handle(caf::stream_slots, caf::upstream_msg::forced_drop&) override;

protected:
  virtual void closing(bool graceful, const caf::error& reason);

  void downstream_connected(caf::actor hdl);

  central_dispatcher* dispatcher_;

  observer* observer_;

private:
  unipath_manager(central_dispatcher*, observer*); // accessible to friends only
};

/// @relates unipath_manager
using unipath_manager_ptr = caf::intrusive_ptr<unipath_manager>;

// -- unipath_data_sink --------------------------------------------------------

/// Represents a @ref data_message sink.
class unipath_data_sink : public unipath_manager {
public:
  // -- member types -----------------------------------------------------------

  using super = unipath_manager;

  using message_type = data_message;

  // -- constructors, destructors, and assignment operators --------------------

  unipath_data_sink(central_dispatcher* cd, observer* obs) : super(cd, obs) {
    // nop
  }

  ~unipath_data_sink() override;

  // -- properties -------------------------------------------------------------

  [[nodiscard]] derived_pointer derived_ptr() noexcept override;

  // -- item processing --------------------------------------------------------

  /// Pushes an item downstream.
  virtual void enqueue(const data_message& msg) = 0;
};

/// @relates unipath_data_sink
using unipath_data_sink_ptr = caf::intrusive_ptr<unipath_data_sink>;

/// @relates unipath_data_sink
unipath_data_sink_ptr make_unipath_data_sink(central_dispatcher* dispatcher,
                                             filter_type filter);

// -- unipath_command_sink -----------------------------------------------------

/// Represents a @ref command_message sink or source.
class unipath_command_sink : public unipath_manager {
public:
  // -- member types -----------------------------------------------------------

  using super = unipath_manager;

  using message_type = command_message;

  // -- constructors, destructors, and assignment operators --------------------

  unipath_command_sink(central_dispatcher* cd, observer* obs) : super(cd, obs) {
    // nop
  }

  ~unipath_command_sink() override;

  // -- properties -------------------------------------------------------------

  [[nodiscard]] derived_pointer derived_ptr() noexcept override;

  // -- item processing --------------------------------------------------------

  /// Pushes an item downstream.
  virtual void enqueue(const message_type& msg) = 0;
};

/// @relates unipath_command_sink
using unipath_command_sink_ptr = caf::intrusive_ptr<unipath_command_sink>;

/// @relates unipath_command_sink
unipath_command_sink_ptr
make_unipath_command_sink(central_dispatcher* dispatcher, filter_type filter);

// -- peer_manager -------------------------------------------------------------

/// Represents a bidirectional connection to a peer.
class peer_manager : public unipath_manager {
public:
  // -- member types -----------------------------------------------------------

  using super = unipath_manager;

  using message_type = node_message;

  // -- constructors, destructors, and assignment operators --------------------

  peer_manager(central_dispatcher*, observer*);

  ~peer_manager() override;

  // -- overrides --------------------------------------------------------------

  void closing(bool graceful, const caf::error& reason) override;

  // -- item processing --------------------------------------------------------

  /// Pushes an item downstream.
  virtual void enqueue(const message_type& msg) = 0;

  // -- properties -------------------------------------------------------------

  /// Checks whether the Broker handshake fully completed.
  [[nodiscard]] bool handshake_completed() const noexcept;

  bool blocks_inputs() const noexcept override;

  /// @pre `!blocks_inputs()`
  void release_blocked_inputs();

  /// @pre `blocks_inputs()`
  virtual void add_blocked_input(caf::message msg) = 0;

  [[nodiscard]] detail::peer_handshake& handshake() noexcept {
    return handshake_;
  }

  [[nodiscard]] derived_pointer derived_ptr() noexcept override;

  // -- peer_handshake callbacks -----------------------------------------------

  void handshake_failed(error reason);

  bool finalize_handshake();

protected:
  // -- member variables -------------------------------------------------------

  peer_handshake handshake_;
  alm::lamport_timestamp remote_ts_;

private:
  virtual void unblock_inputs() = 0;
};

/// @relates peer_unipath
using peer_manager_ptr = caf::intrusive_ptr<peer_manager>;

/// @relates peer_manager
peer_manager_ptr make_peer_manager(central_dispatcher* dispatcher,
                                   peer_manager::observer* observer);

/// @relates peer_manager
peer_manager_ptr make_peer_manager(alm::stream_transport* transport);

// -- unipath_source -----------------------------------------------------------

class unipath_source : public unipath_manager {
public:
  using super = unipath_manager;

  unipath_source(central_dispatcher* dispatcher,
                 unipath_manager::observer* observer)
    : super(dispatcher, observer), out_(this) {
    // nop
  }

  virtual ~unipath_source() override;

  [[nodiscard]] derived_pointer derived_ptr() noexcept override;

  filter_type filter() const override;

  void filter(filter_type) override;

  bool accepts(const topic&) const noexcept override;

  caf::downstream_manager& out() override;

  bool done() const override;

  bool idle() const noexcept override;

protected:
  caf::downstream_manager out_;

private:
  virtual void unblock_inputs() = 0;
  virtual void add_blocked_input(caf::message msg) = 0;
};

/// @relates unipath_source
using unipath_source_ptr = caf::intrusive_ptr<unipath_source>;

/// @relates unipath_data_source
unipath_source_ptr make_unipath_source(central_dispatcher* dispatcher,
                                       caf::stream<data_message> in);

/// @relates unipath_source
unipath_source_ptr make_unipath_source(central_dispatcher* dispatcher,
                                       caf::stream<command_message> in);

/// @relates unipath_source
unipath_source_ptr make_unipath_source(central_dispatcher* dispatcher,
                                       caf::stream<node_message_content> in);

} // namespace broker::detail

namespace std {

template <>
struct hash<broker::detail::peer_manager_ptr> {
  using argument_type = broker::detail::peer_manager_ptr;
  size_t operator()(const argument_type& x) const noexcept {
    hash<broker::detail::unipath_manager*> f;
    return f(x.get());
  }
};

} // namespace std
