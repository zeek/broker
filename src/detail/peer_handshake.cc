#include "broker/detail/peer_handshake.hh"

#include <caf/event_based_actor.hpp>
#include <caf/stream_slot.hpp>

#include "broker/detail/unipath_manager.hh"
#include "broker/message.hh"

namespace broker::detail {

// -- nested type: fsm ---------------------------------------------------------

std::string peer_handshake::fsm::pretty_state() const {
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

// -- nested type: originator --------------------------------------------------

bool peer_handshake::originator::start() {
  BROKER_TRACE(BROKER_ARG2("state", pretty_state()));
  switch (state) {
    case fsm::init_state: {
      state = fsm::started;
      auto owner = parent->owner;
      BROKER_ASSERT(owner != nullptr);
      auto strong_owner = peer_manager_ptr{owner};
      auto self = owner->this_actor();
      auto id = owner->this_endpoint();
      BROKER_ASSERT(self != nullptr);
      self
        ->request(parent->remote_hdl, std::chrono::minutes(10), atom::peer_v,
                  atom::init_v, id, self)
        .then(
          [](atom::peer, atom::ok, const endpoint_id&) {
            // Note: we do *not* fulfill the promise here. We do so after
            // receiving the ack_open.
          },
          [this, ptr{std::move(strong_owner)}](caf::error& err) {
            // If this request fails, this means most likely that the remote
            // core died or disconnected before the handshake could finish. This
            // message handler may "outlive" the transport, so we hold on to a
            // strong reference. Calling `fail` on the parent is safe even after
            // the transport terminated since it only accesses local and actor
            // state. We discard the error in case the FSM made state
            // transitions in the meantime, because then we have other
            // mechanisms that detect errors plus the error we receive here may
            // be a stale request timeout.
            if (auto hs = std::addressof(ptr->handshake())) {
              BROKER_ASSERT(hs == parent);
              if (state == fsm::started)
                hs->fail(ec::peer_disconnect_during_handshake);
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

bool peer_handshake::originator::handle_open_stream_msg() {
  BROKER_TRACE(BROKER_ARG2("state", pretty_state()));
  switch (state) {
    case fsm::started: {
      auto mgr = parent->owner;
      caf::stream<node_message> token;
      parent->in = mgr->add_unchecked_inbound_path(token);
      if (parent->in == caf::invalid_stream_slot) {
        auto err = make_error(caf::sec::runtime_error,
                              "originator failed to open inbound path");
        parent->fail(std::move(err));
        return false;
      }
      auto tup = std::make_tuple(atom::ok_v, caf::actor{mgr->this_actor()},
                                 mgr->this_endpoint(), mgr->local_filter(),
                                 mgr->local_timestamp());
      auto hdl = parent->remote_hdl;
      parent->out
        = mgr->add_unchecked_outbound_path<node_message>(hdl, std::move(tup))
            .value();
      if (parent->out == caf::invalid_stream_slot) {
        auto err = make_error(caf::sec::runtime_error,
                              "originator failed to open outbound path");
        parent->fail(std::move(err));
        return false;
      }
      state |= fsm::has_open_stream_msg;
      return true;
    }
    case fsm::fail_state:
      return false;
    default:
      parent->fail(ec::unexpected_handshake_message);
      return false;
  }
  return false;
}

bool peer_handshake::originator::handle_ack_open_msg() {
  BROKER_TRACE(BROKER_ARG2("state", pretty_state()));
  switch (state) {
    case fsm::started | fsm::has_open_stream_msg: {
      state |= fsm::has_ack_open_msg;
      return parent->done_transition();
    }
    case fsm::fail_state:
      return false;
    default:
      parent->fail(ec::unexpected_handshake_message);
      return false;
  }
}

// -- nested type: responder ---------------------------------------------------

bool peer_handshake::responder::start() {
  BROKER_TRACE(BROKER_ARG2("state", pretty_state()));
  switch (state) {
    case fsm::init_state: {
      auto mgr = parent->owner;
      auto tup = std::make_tuple(caf::actor{mgr->this_actor()},
                                 mgr->this_endpoint(),
                                 mgr->local_filter(), mgr->local_timestamp());
      auto hdl = parent->remote_hdl;
      parent->out
        = mgr->add_unchecked_outbound_path<node_message>(hdl, std::move(tup))
            .value();
      if (parent->out != caf::invalid_stream_slot) {
        state = fsm::started;
        return true;
      } else {
        auto err = make_error(caf::sec::runtime_error,
                              "responder failed to open outbound path");
        parent->fail(std::move(err));
        return false;
      }
    }
    case fsm::fail_state:
      return false;
    default:
      parent->fail(ec::repeated_peering_handshake_request);
      return false;
  }
}

bool peer_handshake::responder::handle_open_stream_msg() {
  BROKER_TRACE(BROKER_ARG2("state", pretty_state()));
  switch (state) {
    case fsm::started:
    case fsm::started | fsm::has_ack_open_msg: {
      auto mgr = parent->owner;
      caf::stream<node_message> token;
      parent->in = mgr->add_unchecked_inbound_path(token);
      if (parent->in != caf::invalid_stream_slot) {
        state |= fsm::has_open_stream_msg;
        return post_msg_action();
      } else {
        auto err = make_error(caf::sec::runtime_error,
                              "responder failed to open inbound path");
        parent->fail(std::move(err));
        return false;
      }
    }
    case fsm::fail_state:
      return false;
    default:
      parent->fail(ec::repeated_peering_handshake_request);
      return false;
  }
}

bool peer_handshake::responder::handle_ack_open_msg() {
  BROKER_TRACE(BROKER_ARG2("state", pretty_state()));
  switch (state) {
    case fsm::started:
    case fsm::started | fsm::has_open_stream_msg: {
      state |= fsm::has_ack_open_msg;
      return post_msg_action();
    }
    case fsm::fail_state:
      return false;
    default:
      parent->fail(ec::repeated_peering_handshake_request);
      return false;
  }
}

bool peer_handshake::responder::post_msg_action() {
  BROKER_TRACE(BROKER_ARG2("state", pretty_state()));
  if (done()) {
    return parent->done_transition();
  } else {
    return true;
  }
}

// -- state transitions --------------------------------------------------------

bool peer_handshake::originator_start_peering(endpoint_id peer_id,
                                              caf::actor peer_hdl,
                                              caf::response_promise rp) {
  BROKER_TRACE(BROKER_ARG2("impl", pretty_impl())
               << BROKER_ARG(peer_id)
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

bool peer_handshake::originator_handle_open_stream_msg(
  filter_type filter, alm::lamport_timestamp timestamp) {
  BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()));
  if (is_originator()) {
    remote_filter = std::move(filter);
    remote_timestamp = timestamp;
    return caf::get<originator>(impl).handle_open_stream_msg();
  } else {
    fail(ec::invalid_handshake_state);
    return false;
  }
}

bool peer_handshake::responder_start_peering(endpoint_id peer_id,
                                             caf::actor peer_hdl) {
  BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()) << BROKER_ARG(peer_id));
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

bool peer_handshake::responder_handle_open_stream_msg(
  filter_type filter, alm::lamport_timestamp timestamp) {
  BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()));
  if (is_responder()) {
    remote_filter = std::move(filter);
    remote_timestamp = timestamp;
    return caf::get<responder>(impl).handle_open_stream_msg();
  } else {
    fail(ec::invalid_handshake_state);
    return false;
  }
}

bool peer_handshake::handle_ack_open_msg() {
  BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()));
  return visit_impl(
    [this](caf::unit_t&) {
      fail(ec::invalid_handshake_state);
      return false;
    },
    [this](auto& obj) { return obj.handle_ack_open_msg(); });
}

// -- callbacks for the FSM implementations ------------------------------------

bool peer_handshake::done_transition() {
  BROKER_TRACE(BROKER_ARG2("impl", pretty_impl()));
  if (owner->finalize_handshake()) {
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

// -- error handling -----------------------------------------------------------

void peer_handshake::fail(error reason) {
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
    owner->handshake_failed(err);
  }
}

int peer_handshake::state() const noexcept {
  return visit_impl([](const caf::unit_t&) { return fsm::init_state; },
                    [](const auto& obj) { return obj.state; });
}

caf::actor peer_handshake::self_hdl() {
  return caf::actor_cast<caf::actor>(owner->this_actor());
}

std::string peer_handshake::pretty_impl() const {
  return visit_impl([](const caf::unit_t&) { return "indeterminate"; },
                    [](const originator&) { return "originator"; },
                    [](const responder&) { return "responder"; });
}

// -- FSM management -----------------------------------------------------------

bool peer_handshake::to_responder() {
  if (!started()) {
    impl = responder{this};
    return true;
  } else {
    return false;
  }
}

} // namespace broker::detail
