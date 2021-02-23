#pragma once

#include <vector>

#include <caf/actor.hpp>
#include <caf/downstream_msg.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/upstream_msg.hpp>
#include <caf/variant.hpp>

#include "broker/alm/lamport_timestamp.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/overload.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/fwd.hh"
#include "broker/logger.hh"

namespace broker::detail {

/// A finite-state machine for modeling a peering handshake between two Broker
/// endpoints.
class peer_handshake {
public:
  // -- nested types -----------------------------------------------------------

  /// FSM scaffold for both implementations.
  struct fsm {
    // -- constants ------------------------------------------------------------

    /// This flag signals that the originator sent
    /// `(peer, init, peer_id, actor)` and is currently waiting for
    /// `open_stream_msg`.
    static constexpr int started = 0x01;

    /// This flag signals that the originator received the `open_stream_msg`
    /// from the responder.
    static constexpr int has_open_stream_msg = 0x02;

    /// This flag signals that the originator received the
    /// `upstream_msg::ack_open` from the responder.
    static constexpr int has_ack_open_msg = 0x04;

    /// The state after constructing the FSM.
    static constexpr int init_state = 0x00;

    /// The state after the FSM completed.
    static constexpr int done_state = 0x07;

    /// The state after an invalid transition.
    static constexpr int fail_state = 0x10;

    // -- constructors, destructors, and assignment operators ------------------

    explicit fsm(peer_handshake* parent) : parent(parent) {
      // nop
    }

    fsm(fsm&&) = default;

    fsm& operator=(fsm&&) = default;

    // -- member variables -----------------------------------------------------

    /// Points to the handshake object.
    peer_handshake* parent;

    /// Keeps track of the progress.
    int state = init_state;

    // -- properties -----------------------------------------------------------

    /// Queries whether the all three flags are present.
    constexpr bool done() const {
      return state == done_state;
    }

    constexpr bool has_flag(int flag) const {
      return (state & flag) == flag;
    }

    std::string pretty_state() const;
  };

  /// Implementation for the Originator of the handshake. This FSM is a simple
  /// sequence of steps:
  ///
  /// ~~~
  /// +---------------------------+
  /// |           init            |
  /// +-+-------------------------+
  ///   |
  ///   | (peer, peer_id, actor)
  ///   v
  /// +-+-------------------------+
  /// |          started          |
  /// +-+-------------------------+
  ///   |
  ///   | (caf::open_stream_msg)
  ///   v
  /// +-+-------------------------+
  /// |    has_open_stream_msg    |
  /// +-+-------------------------+
  ///   |
  ///   | (caf::upstream_msg::ack_open)
  ///   v
  /// +-+-------------------------+
  /// |           done            |
  /// +---------------------------+
  /// ~~~
  struct originator : fsm {
    // -- constructors, destructors, and assignment operators ------------------

    using fsm::fsm;

    // -- state transitions ----------------------------------------------------

    bool start();

    bool handle_open_stream_msg();

    bool handle_ack_open_msg();
  };

  /// Implementation for the Responder of the handshake. The FSM allows
  /// processing of `caf::upstream_msg::ack_open` and `caf::open_stream_msg` in
  /// any order.
  ///
  /// ~~~
  ///                 +---------------------------+
  ///                 |           init            |
  ///                 +-+-------------------------+
  ///                   |
  ///                   | (peer, init, peer_id, actor)
  ///                   v
  ///                 +-+-------------------------+
  ///                 |          started          |
  ///                 +-+--+----------------------+
  ///                   |  |
  ///   +---------------+  +------------+
  ///   |                               |
  ///   | (caf::upstream_msg::ack_open) | (caf::open_stream_msg)
  ///   v                               v
  /// +-+-------------------------+   +-+-------------------------+
  /// |      has_ack_open_msg     |   |    has_open_stream_msg    |
  /// +-+-------------------------+   +-+-------------------------+
  ///   |                               |
  ///   | (caf::open_stream_msg)        | (caf::upstream_msg::ack_open)
  ///   |                               |
  ///   +---------------+  +------------+
  ///                   |  |
  ///                   v  v
  ///                 +-+--+----------------------+
  ///                 |           done            |
  ///                 +---------------------------+
  /// ~~~
  struct responder : fsm {
    // -- constructors, destructors, and assignment operators ------------------

    using fsm::fsm;

    // -- state transitions ----------------------------------------------------

    bool start();

    bool handle_open_stream_msg();

    bool handle_ack_open_msg();

    bool post_msg_action();
  };

  // -- member types (continued) -----------------------------------------------

  using impl_type = caf::variant<caf::unit_t, originator, responder>;

  using input_msg_type
    = caf::variant<caf::downstream_msg::batch, caf::downstream_msg::close,
                   caf::downstream_msg::forced_close, caf::upstream_msg::drop,
                   caf::upstream_msg::forced_drop, caf::upstream_msg::ack_batch,
                   caf::message>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit peer_handshake(peer_manager* owner) : owner(owner) {
    // nop
  }

  peer_handshake() = delete;

  peer_handshake(const peer_handshake&) = delete;

  peer_handshake& operator=(const peer_handshake&) = delete;

  // -- state transitions ------------------------------------------------------

  /// Starts the handshake. This FSM takes on the role of the Originator.
  [[nodiscard]] bool originator_start_peering(endpoint_id peer_id,
                                              caf::actor peer_hdl,
                                              caf::response_promise rp);

  /// Processes the open_stream_msg addressed at the originator.
  [[nodiscard]] bool
  originator_handle_open_stream_msg(filter_type filter,
                                    alm::lamport_timestamp timestamp);

  /// Starts the handshake. This FSM takes on the role of the Originator.
  [[nodiscard]] bool responder_start_peering(endpoint_id peer_id,
                                             caf::actor peer_hdl);

  /// Processes the open_stream_msg addressed at the responder.
  [[nodiscard]] bool
  responder_handle_open_stream_msg(filter_type filter,
                                   alm::lamport_timestamp timestamp);

  /// Processes the `ack_open` message. Unlike the other functions, the message
  /// is always the same, whether Originator or Responder receive it. Hence,
  /// this function internally dispatches on the implementation type of the FSM.
  [[nodiscard]] bool handle_ack_open_msg();

  // -- callbacks for the FSM implementations ----------------------------------

  bool done_transition();

  // -- error handling ---------------------------------------------------------

  /// Fulfills all response promises with `reason`, sets `err` to `reason` and
  /// sets the FSM state to `fail_state`;
  void fail(error reason);

  // -- utilities --------------------------------------------------------------

  template <class... Fs>
  auto visit_impl(Fs... fs) {
    auto overload_set = detail::make_overload(std::move(fs)...);
    return caf::visit(overload_set, impl);
  }

  template <class... Fs>
  auto visit_impl(Fs... fs) const {
    auto overload_set = detail::make_overload(std::move(fs)...);
    return caf::visit(overload_set, impl);
  }

  // -- properties -------------------------------------------------------------

  // caf::intrusive_ptr<peer_handshake> strong_this() noexcept {
  //   return this;
  // }

  bool is_originator() const noexcept {
    return caf::holds_alternative<originator>(impl);
  }

  bool is_responder() const noexcept {
    return caf::holds_alternative<responder>(impl);
  }

  bool failed() const noexcept {
    return static_cast<bool>(err);
  }

  int state() const noexcept;

  auto started() const noexcept {
    return state() != fsm::init_state;
  }

  auto done() const noexcept {
    return state() == fsm::done_state;
  }

  [[nodiscard]] bool has_inbound_path() const noexcept {
    return in != caf::invalid_stream_slot;
  }

  [[nodiscard]] bool has_outbound_path() const noexcept {
    return out != caf::invalid_stream_slot;
  }

  caf::actor self_hdl();

  std::string pretty_impl() const;

  // -- FSM management ---------------------------------------------------------

  /// Forces the implementation to `responder` if the FSM has not started yet.
  bool to_responder();

  // -- member variables -------------------------------------------------------

  /// Pointer to the object that performs the handshake.
  /// @ref broker::alm::stream_transport.
  peer_manager* owner;

  /// ID of the remote endpoint.
  endpoint_id remote_id;

  /// Handle to the remote core.
  caf::actor remote_hdl;

  /// Topic filter of the remote endpoint.
  filter_type remote_filter;

  /// Logical time at the remote peer when establishing the connection.
  alm::lamport_timestamp remote_timestamp;

  error err;

  std::vector<caf::response_promise> promises;

  caf::stream_slot in = caf::invalid_stream_slot;

  caf::stream_slot out = caf::invalid_stream_slot;

  impl_type impl;

  std::vector<input_msg_type> input_buffer;
};

} // namespace broker::detail
