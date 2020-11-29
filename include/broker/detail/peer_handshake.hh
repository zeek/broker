#pragma once

#include <vector>

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/downstream_msg.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/make_counted.hpp>
#include <caf/ref_counted.hpp>
#include <caf/response_promise.hpp>
#include <caf/stream_slot.hpp>
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
template <class Transport>
class peer_handshake : public caf::ref_counted {
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

    std::string pretty_state() const {
      std::string result;
      switch (state) {
        case init_state:
          result = "init";
          break;
        case fail_state:
          result = "fail";
          break;
        case done_state:
          result = "done";
          break;
        case started:
          result = "started";
          break;
        default:
          BROKER_ASSERT(has_flag(started));
          if (has_flag(has_open_stream_msg)) {
            result = "has_open_stream_msg";
          } else {
            BROKER_ASSERT(has_flag(has_ack_open_msg));
            result = "has_ack_open_msg";
          }
      }
      return result;
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
      BROKER_TRACE(BROKER_ARG2("state", this->pretty_state()));
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
              [](atom::peer, atom::ok, const endpoint_id&) {
                // Note: we do *not* fulfill the promise here. We do so after
                // receiving the ack_open.
              },
              [this, strong_parent{parent->strong_this()}](caf::error& err) {
                // If this request fails, this means most likely that the remote
                // core died or disconnected before the handshake could finish.
                // This message handler may "outlive" the transport, so we hold
                // on to a strong reference. Calling `fail` on the parent is
                // safe even after the transport terminated since it only
                // accesses local and actor state. We discard the error in case
                // the FSM made state transitions in the meantime, because then
                // we have other mechanisms that detect errors plus the error we
                // receive here may be a stale request timeout.
                if (this->state == fsm::started) {
                  strong_parent->fail(ec::peer_disconnect_during_handshake);
                  strong_parent->transport->cleanup(strong_parent);
                }
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
      BROKER_TRACE(BROKER_ARG2("state", this->pretty_state()));
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
      BROKER_TRACE(BROKER_ARG2("state", this->pretty_state()));
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
      BROKER_TRACE(BROKER_ARG2("state", this->pretty_state()));
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
      BROKER_TRACE(BROKER_ARG2("state", this->pretty_state()));
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
      BROKER_TRACE(BROKER_ARG2("state", this->pretty_state()));
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
      BROKER_TRACE(BROKER_ARG2("state", this->pretty_state()));
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
                   caf::downstream_msg::forced_close, caf::upstream_msg::drop,
                   caf::upstream_msg::forced_drop, caf::upstream_msg::ack_batch,
                   caf::message>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit peer_handshake(Transport* transport) : transport(transport) {
    // nop
  }

  // -- state transitions ------------------------------------------------------

  /// Starts the handshake. This FSM takes on the role of the Originator.
  [[nodiscard]] bool originator_start_peering(endpoint_id peer_id,
                                              caf::actor peer_hdl,
                                              caf::response_promise rp = {}) {
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl())
                 << BROKER_ARG(peer_id) << BROKER_ARG(peer_hdl)
                 << BROKER_ARG2("rp.pending", rp.pending()));
    remote_id = std::move(peer_id);
    remote_hdl = std::move(peer_hdl);
    if (rp.pending())
      promises.emplace_back(std::move(rp));
    if (caf::holds_alternative<caf::unit_t>(impl)) {
      impl = originator{this};
      return caf::get<originator>(impl).start();
    } else if (is_originator() && !started()) {
      return caf::get<originator>(impl).start();
    } else {
      fail(ec::invalid_handshake_state);
      return false;
    }
  }

  /// Processes the open_stream_msg addressed at the Originator.
  [[nodiscard]] bool
  originator_handle_open_stream_msg(filter_type filter,
                                    alm::lamport_timestamp timestamp) {
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl())
                 << BROKER_ARG(filter) << BROKER_ARG(timestamp));
    if (is_originator()) {
      remote_filter = std::move(filter);
      remote_timestamp = timestamp;
      return caf::get<originator>(impl).handle_open_stream_msg();
    } else {
      fail(ec::invalid_handshake_state);
      return false;
    }
  }

  /// Starts the handshake. This FSM takes on the role of the Originator.
  [[nodiscard]] bool responder_start_peering(endpoint_id peer_id,
                                             caf::actor peer_hdl) {
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl())
                 << BROKER_ARG(peer_id) << BROKER_ARG(peer_hdl));
    remote_id = std::move(peer_id);
    remote_hdl = std::move(peer_hdl);
    if (caf::holds_alternative<caf::unit_t>(impl)) {
      impl = responder{this};
      return caf::get<responder>(impl).start();
    } else if (is_responder() && !started()) {
      return caf::get<responder>(impl).start();
    } else {
      fail(ec::invalid_handshake_state);
      return false;
    }
  }

  /// Processes the open_stream_msg addressed at the responder.
  [[nodiscard]] bool
  responder_handle_open_stream_msg(filter_type filter,
                                   alm::lamport_timestamp timestamp) {
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl())
                 << BROKER_ARG(filter) << BROKER_ARG(timestamp));
    if (is_responder()) {
      remote_filter = std::move(filter);
      remote_timestamp = timestamp;
      return caf::get<responder>(impl).handle_open_stream_msg();
    } else {
      fail(ec::invalid_handshake_state);
      return false;
    }
  }

  /// Processes the `ack_open` message. Unlike the other functions, the message
  /// is always the same, whether Originator or Responder receive it. Hence,
  /// this function internally dispatches on the implementation type of the FSM.
  [[nodiscard]] bool handle_ack_open_msg() {
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()));
    return visit_impl(
      [this](caf::unit_t&) {
        fail(ec::invalid_handshake_state);
        return false;
      },
      [this](auto& obj) { return obj.handle_ack_open_msg(); });
  }

  // -- callbacks for the FSM implementations ----------------------------------

  bool done_transition() {
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()));
    if (transport->finalize(this)) {
      if (!promises.empty()) {
        for (auto& promise : promises)
          promise.deliver(atom::peer_v, atom::ok_v, remote_id);
        promises.clear();
      }
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
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()) << BROKER_ARG(reason));
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

  auto started() const noexcept {
    return state() != fsm::init_state;
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

  std::string pretty_impl() const {
    return visit_impl([](const caf::unit_t&) { return "indeterminate"; },
                      [](const originator&) { return "originator"; },
                      [](const responder&) { return "responder"; });
  }

  // -- FSM management ---------------------------------------------------------

  /// Forces the implementation to `responder` if the FSM has not started yet.
  bool to_responder() {
    if (!started()) {
      impl = responder{this};
      return true;
    } else {
      return false;
    }
  }

  // -- message buffering ------------------------------------------------------

  /// Pushes an input message (one of the three CAF downstream messages) to the
  /// buffer. This enables the transport to delay processing of any incoming
  /// message until the handshake completed.
  template <class T>
  void input_buffer_emplace(T&& x) {
    input_buffer.emplace_back(std::forward<T>(x));
  }

  /// Replays buffered inputs by calling `transport->handle(...)` for each
  /// message in the buffer.
  /// @note Call only after the handshake completed and ideally as the last
  ///       member function on this object before destroying it.
  void replay_input_buffer() {
    BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()));
    auto in_path = transport->get_inbound_path(in);
    auto out_path = transport->out().path(out);
    if (!done()) {
      BROKER_ERROR("replay_input_buffer called on pending handshake");
    } else if (in_path == nullptr) {
      BROKER_ERROR("no path found for input slot");
    } else if (out_path == nullptr) {
      BROKER_ERROR("no path found for output slot");
    } else {
      if (!input_buffer.empty()) {
        auto f = [t{transport}, in_path, out_path](auto& msg) {
          t->handle_buffered_msg(in_path, out_path, msg);
        };
        for (auto& msg : input_buffer)
          caf::visit(f, msg);
        input_buffer.clear();
      }
    }
  }

  // -- member variables -------------------------------------------------------

  /// Pointer to the transport object that performs the handshake. Usually a
  /// @ref broker::alm::stream_transport.
  Transport* transport;

  /// ID of the remote endpoint.
  endpoint_id remote_id;

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
