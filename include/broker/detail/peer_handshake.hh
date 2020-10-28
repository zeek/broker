#pragma once

#include <vector>

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/downstream_msg.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>
#include <caf/response_promise.hpp>
#include <caf/stream_slot.hpp>
#include <caf/variant.hpp>

#include "broker/alm/lamport_timestamp.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/overload.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"

namespace broker::detail {

/// A finite-state machine for modeling a peering handshake between two Broker
/// endpoints.
template <class Transport>
class peer_handshake : public caf::ref_counted {
public:
  // -- member types -----------------------------------------------------------

  using peer_id_type = typename Transport::peer_id_type;

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

    bool start() {
      auto parent = this->parent;
      switch (this->state) {
        case fsm::init_state: {
          this->state = fsm::started;
          auto transport = parent->transport;
          BROKER_ASSERT(transport != nullptr);
          auto self = transport->self();
          BROKER_ASSERT(self != nullptr);
          self
            ->request(parent->remote_hdl, std::chrono::minutes(10),
                      atom::peer_v, atom::init_v, transport->id(), self)
            .then(
              [](atom::peer, atom::ok, const peer_id_type&) {
                // Note: we do *not* fulfill the promise here. We do so after
                // receiving the ack_open.
              },
              [this, strong_parent{parent->strong_this()}](caf::error& err) {
                // If this request fails, this means most likely that the remote
                // core died or disconnected before the handshake could finish.
                // This message handler may "outlive" the transport, so we hold
                // on to a strong reference. Calling `fail` on the parent is
                // safe even after the transport terminated since it only
                // accesses local and actor state.
                strong_parent->fail(ec::peer_disconnect_during_handshake);
              });
          return true;
        }
        case fsm::fail_state:
          return false;
        default:
          parent->fail(ec::repeated_peering_handshake_request);
          return false;
      }
    }

    bool handle_open_stream_msg() {
      switch (this->state) {
        case fsm::started: {
          auto parent = this->parent;
          auto transport = parent->transport;
          parent->in = transport->make_inbound_path(parent);
          parent->out = transport->make_outbound_path(parent);
          this->state |= fsm::has_open_stream_msg;
          return true;
        }
        case fsm::fail_state:
          return false;
        default:
          this->parent->fail(ec::unexpected_handshake_message);
          return false;
      }
    }

    bool handle_ack_open_msg() {
      auto parent = this->parent;
      switch (this->state) {
        case fsm::started | fsm::has_open_stream_msg: {
          this->state |= fsm::has_ack_open_msg;
          return parent->done_transition();
        }
        case fsm::fail_state:
          return false;
        default:
          parent->fail(ec::unexpected_handshake_message);
          return false;
      }
    }
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

    bool start() {
      auto parent = this->parent;
      auto transport = parent->transport;
      switch (this->state) {
        case fsm::init_state: {
          parent->out = transport->make_outbound_path(parent);
          this->state = fsm::started;
          return true;
        }
        case fsm::fail_state:
          return false;
        default:
          parent->fail(ec::repeated_peering_handshake_request);
          return false;
      }
    }

    bool handle_open_stream_msg() {
      auto parent = this->parent;
      auto transport = parent->transport;
      switch (this->state) {
        case fsm::started:
        case fsm::started | fsm::has_ack_open_msg: {
          parent->in = transport->make_inbound_path(parent);
          this->state |= fsm::has_open_stream_msg;
          return post_msg_action();
        }
        case fsm::fail_state:
          return false;
        default:
          parent->fail(ec::repeated_peering_handshake_request);
          return false;
      }
    }

    bool handle_ack_open_msg() {
      auto parent = this->parent;
      switch (this->state) {
        case fsm::started:
        case fsm::started | fsm::has_open_stream_msg: {
          this->state |= fsm::has_ack_open_msg;
          return post_msg_action();
        }
        case fsm::fail_state:
          return false;
        default:
          parent->fail(ec::repeated_peering_handshake_request);
          return false;
      }
    }

    bool post_msg_action() {
      if (this->done()) {
        return this->parent->done_transition();
      } else {
        return true;
      }
    }
  };

  // -- member types (continued) -----------------------------------------------

  using impl_type = caf::variant<caf::unit_t, originator, responder>;

  using input_msg_type
    = caf::variant<caf::downstream_msg::batch, caf::downstream_msg::close,
                   caf::downstream_msg::forced_close>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit peer_handshake(Transport* transport) : transport(transport) {
    // nop
  }

  // -- state transitions ------------------------------------------------------

  /// Starts the handshake. This FSM takes on the role of the Originator.
  [[nodiscard]] bool originator_start_peering(peer_id_type peer_id,
                                              caf::actor peer_hdl,
                                              caf::response_promise rp = {}) {
    if (caf::holds_alternative<caf::unit_t>(impl)) {
      remote_id = std::move(peer_id);
      remote_hdl = std::move(peer_hdl);
      if (rp.pending())
        promises.emplace_back(std::move(rp));
      impl = originator{this};
      return caf::get<originator>(impl).start();
    } else {
      fail(ec::invalid_handshake_state);
      return false;
    }
  }

  /// Processes the open_stream_msg addressed at the Originator.
  [[nodiscard]] bool originator_handle_open_stream_msg(filter_type filter) {
    if (!is_originator()) {
      fail(ec::invalid_handshake_state);
      return false;
    } else {
      remote_filter = std::move(filter);
      return caf::get<originator>(impl).handle_open_stream_msg();
    }
  }

  /// Starts the handshake. This FSM takes on the role of the Originator.
  [[nodiscard]] bool responder_start_peering(peer_id_type peer_id,
                                             caf::actor peer_hdl) {
    if (caf::holds_alternative<caf::unit_t>(impl)) {
      remote_id = std::move(peer_id);
      remote_hdl = std::move(peer_hdl);
      impl = responder{this};
      return caf::get<responder>(impl).start();
    } else {
      fail(ec::invalid_handshake_state);
      return false;
    }
  }

  /// Processes the open_stream_msg addressed at the responder.
  [[nodiscard]] bool responder_handle_open_stream_msg(filter_type filter) {
    if (!is_responder()) {
      fail(ec::invalid_handshake_state);
      return false;
    } else {
      remote_filter = std::move(filter);
      return caf::get<responder>(impl).handle_open_stream_msg();
    }
  }

  /// Processes the `ack_open` message. Unlike the other functions, the message
  /// is always the same, whether Originator or Responder receive it. Hence,
  /// this function internally dispatches on the implementation type of the FSM.
  [[nodiscard]] bool handle_ack_open_msg(caf::actor rebind_hdl) {
    if (remote_hdl != rebind_hdl)
      remote_hdl = std::move(rebind_hdl);
    return visit_impl(
      [this](caf::unit_t&) {
        fail(ec::invalid_handshake_state);
        return false;
      },
      [this](auto& obj) { return obj.handle_ack_open_msg(); });
  }

  // -- callbacks for the FSM implementations ----------------------------------

  bool done_transition() {
    if (transport->finalize(this)) {
      return true;
    } else {
      fail(ec::invalid_handshake_state);
      return false;
    }
  }

  // -- error handling ---------------------------------------------------------

  /// Fulfills all response promises with `reason`, sets `err` to `reason` and
  /// sets the FSM state to `fail_state`;
  void fail(error reason) {
    if (err) {
      BROKER_ERROR("cannot fail a handshake twice");
    } else {
      visit_impl([](caf::unit_t&) {},
                 [](auto& obj) { obj.state = fsm::fail_state; });
      err = std::move(reason);
      if (!promises.empty()) {
        for (auto& promise : promises)
          promise.deliver(err);
        promises.clear();
      }
      if (in != caf::invalid_stream_slot) {
        transport->remove_input_path(in, err, false);
        in = caf::invalid_stream_slot;
      }
      if (out != caf::invalid_stream_slot) {
        transport->out().remove_path(out, err, false);
        out = caf::invalid_stream_slot;
      }
    }
  }

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

  caf::intrusive_ptr<peer_handshake> strong_this() noexcept {
    return this;
  }

  bool is_originator() const noexcept {
    return caf::holds_alternative<originator>(impl);
  }

  bool is_responder() const noexcept {
    return caf::holds_alternative<responder>(impl);
  }

  bool failed() const noexcept {
    return static_cast<bool>(err);
  }

  auto state() const noexcept {
    return visit_impl([](const caf::unit_t&) { return fsm::init_state; },
                      [](const auto& obj) { return obj.state; });
  }

  auto done() const noexcept {
    return state() == fsm::done_state;
  }

  bool has_input_slot() const noexcept {
    return in != caf::invalid_stream_slot;
  }

  bool has_output_slot() const noexcept {
    return out != caf::invalid_stream_slot;
  }

  auto self_hdl() {
    return caf::actor_cast<caf::actor>(transport->self());
  }

  // -- message buffering ------------------------------------------------------

  /// Pushes an input message (one of the three CAF downstream messages) to the
  /// buffer. This enables the transport to delay processing of any incoming
  /// message until the handshake completed.
  template <class T>
  void input_buffer_emplace(T&& x) {
    input_buffer.emplace(std::forward<T>(x));
  }

  /// Replays buffered inputs by calling `transport->handle(...)` for each
  /// message in the buffer.
  /// @note Call only after the handshake completed and ideally as the last
  ///       member function on this object before destroying it.
  bool replay_input_buffer() {
    auto path = get_inbound_path(in);
    if (!done()) {
      fail(make_error(ec::invalid_handshake_state,
                      "replay_input_buffer called on pending handshake"));
      return false;
    } else if (path == nullptr) {
      fail(make_error(ec::invalid_handshake_state,
                      "no inbound path found for input slot"));
      return false;
    } else {
      if (!input_buffer.empty()) {
        auto f = [t{transport}, path](auto& msg) { t->handle(path, msg); };
        for (auto& msg : input_buffer)
          caf::visit(f, msg);
        input_buffer.clear();
      }
      return true;
    }
  }

  // -- member variables -------------------------------------------------------

  /// Pointer to the transport object that performs the handshake. Usually a
  /// @ref broker::alm::stream_transport.
  Transport* transport;

  /// ID of the remote endpoint.
  peer_id_type remote_id;

  /// Communication handle for the remote endpoint.
  caf::actor remote_hdl;

  /// Topic filter of the remote endpoint.
  filter_type remote_filter;

  /// Topic filter of the remote endpoint.
  alm::lamport_timestamp remote_timestamp;

  error err;

  std::vector<caf::response_promise> promises;

  caf::stream_slot in = caf::invalid_stream_slot;

  caf::stream_slot out = caf::invalid_stream_slot;

  impl_type impl;

  std::vector<input_msg_type> input_buffer;
};

template <class Transport>
using peer_handshake_ptr = caf::intrusive_ptr<peer_handshake<Transport>>;

} // namespace broker::detail
