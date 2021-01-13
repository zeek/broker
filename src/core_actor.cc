#include "broker/core_actor.hh"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/behavior.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/group.hpp>
#include <caf/make_counted.hpp>
#include <caf/none.hpp>
#include <caf/response_promise.hpp>
#include <caf/result.hpp>
#include <caf/sec.hpp>
#include <caf/spawn_options.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/stream.hpp>
#include <caf/stream_slot.hpp>
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>

#include "broker/atoms.hh"
#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/convert.hh"
#include "broker/defaults.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/make_backend.hh"
#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/peer_status.hh"
#include "broker/status.hh"
#include "broker/topic.hh"

using namespace caf;

namespace broker {

core_state::core_state(caf::event_based_actor* ptr,
                       const filter_type& initial_filter, broker_options opts,
                       endpoint::clock* ep_clock)
  : super(ep_clock, ptr, initial_filter),
    options_(opts),
    filter_(initial_filter) {
  cache().set_use_ssl(!options_.disable_ssl);
  // We monitor remote inbound peerings and local outbound peerings.
  self_->set_down_handler([this](const caf::down_msg& down) {
    if (!down.source)
      ; // Ignore bogus message.
    else if (auto hdl = caf::actor_cast<caf::actor>(down.source))
      drop_peer(hdl, false, down.reason);
  });
}

void core_state::update_filter_on_peers() {
  BROKER_TRACE("");
  for_each_peer([this](const actor& hdl) {
    self()->send(hdl, atom::update_v, filter_);
  });
}

void core_state::subscribe(filter_type xs) {
  BROKER_TRACE(BROKER_ARG(xs));
  // Status and error topics are internal topics.
  auto internal_only = [](const topic& x) {
    return x == topics::errors || x == topics::statuses
           || topics::store_events.prefix_of(x);
  };
  xs.erase(std::remove_if(xs.begin(), xs.end(), internal_only), xs.end());
  if (xs.empty())
    return;
  if (filter_extend(filter_, xs)) {
    BROKER_DEBUG("Changed filter to " << filter_);
    update_filter_on_peers();
    super::subscribe(xs);
  }
}

bool core_state::has_remote_subscriber(const topic& x) noexcept {
  return any_peer_manager([&x](const auto& mgr) { return mgr->accepts(x); });
}

void core_state::peer_connected(const peer_id_type& peer_id,
                                const communication_handle_type& hdl) {
  super::peer_connected(peer_id, hdl);
}

caf::behavior core_state::make_behavior() {
  return super::make_behavior(
    // --- filter manipulation -------------------------------------------------
    [=](atom::subscribe, filter_type& f) {
      BROKER_TRACE(BROKER_ARG(f));
      subscribe(std::move(f));
    },
    // --- peering requests from local actors, i.e., "step 0" ------------------
    [=](atom::peer, actor remote_core) {
      auto remote_id = remote_core.node();
      start_peering(remote_id, std::move(remote_core),
                    self()->make_response_promise());
    },
    // --- 3-way handshake for establishing peering streams between A and B ----
    // --- A (this node) performs steps #1 and #3; B performs #2 and #4 --------
    // Step #1: - A demands B shall establish a stream back to A
    //          - A has subscribers to the topics `ts`
    [=](atom::peer, filter_type& peer_ts, caf::actor& peer_hdl) {
      BROKER_TRACE(BROKER_ARG(peer_ts) << BROKER_ARG(peer_hdl));
      using result_type = decltype(start_handshake<true>(peer_hdl, peer_ts));
      // Reject anonymous peering requests.
      if (peer_hdl == nullptr) {
        BROKER_DEBUG("Drop anonymous peering request.");
        return result_type{};
      }
      // Drop repeated handshake requests.
      if (connected_to(peer_hdl)) {
        BROKER_WARNING("Drop peering request from already connected peer.");
        return result_type{};
      }
      BROKER_DEBUG("received handshake step #1" << BROKER_ARG(peer_hdl)
                                                << BROKER_ARG(actor{self()}));
      // Start CAF stream.
      return start_handshake<true>(peer_hdl, std::move(peer_ts));
    },
    // Step #2: B establishes a stream to A and sends its own filter
    [=](const stream<node_message>& in, filter_type& filter,
        caf::actor& peer_hdl) {
      BROKER_TRACE(BROKER_ARG(in) << BROKER_ARG(filter) << peer_hdl);
      BROKER_DEBUG("received handshake step #2 from"
                   << peer_hdl << BROKER_ARG(actor{self()}));
      // At this stage, we expect to have no path to the peer yet.
      if (connected_to(peer_hdl)) {
        BROKER_WARNING("Received unexpected or repeated step #2 handshake.");
        return;
      }
      start_handshake<false>(peer_hdl, std::move(filter));
      ack_peering(in, peer_hdl);
    },
    // Step #3: - A establishes a stream to B
    //          - B has a stream to A and vice versa now
    [=](const stream<node_message>& in, ok_atom, caf::actor& peer_hdl) {
      BROKER_TRACE(BROKER_ARG(in) << BROKER_ARG(peer_hdl));
      if (!pending_connections().count(peer_hdl)) {
        BROKER_ERROR("Received a step #3 handshake, but no #1 previously.");
      } else if (ack_peering(in, peer_hdl))
        try_finalize_handshake(peer_hdl);
      else
        BROKER_DEBUG("Drop (repeated?) step #3 handshake.");
    },
    // --- asynchronous communication to peers ---------------------------------
    [=](atom::update, filter_type f) {
      BROKER_TRACE(BROKER_ARG(f));
      auto p = caf::actor_cast<caf::actor>(self()->current_sender());
      if (p == nullptr) {
        BROKER_DEBUG("Received anonymous filter update.");
        return;
      }
      if (!update_peer(p, std::move(f)))
        BROKER_DEBUG("Cannot update filter of unknown peer:" << to_string(p));
    },
    // --- communication to local actors: incoming streams and subscriptions ---
    [=](atom::join, filter_type& filter) {
      BROKER_TRACE(BROKER_ARG(filter));
      auto result = add_worker(filter);
      if (result != invalid_stream_slot)
        subscribe(std::move(filter));
      return result;
    },
    [=](atom::join, atom::update, stream_slot slot, filter_type& filter) {
      subscribe(filter);
      set_filter(slot, std::move(filter));
    },
    [=](atom::join, atom::update, stream_slot slot, filter_type& filter,
        caf::actor& who_asked) {
      subscribe(filter);
      set_filter(slot, std::move(filter));
      self()->send(who_asked, true);
    },
    [=](atom::join, atom::store, const filter_type& filter) {
      return add_sending_store(filter);
    },
    [=](endpoint::stream_type in) {
      BROKER_TRACE("add data_message input stream");
      detail::make_source(&dispatcher_, in);
    },
    [=](stream<node_message::value_type> in) {
      BROKER_TRACE("add node_message::value_type input stream");
      detail::make_source(&dispatcher_, in);
    },
    [=](atom::publish, data_message& x) {
      BROKER_TRACE(BROKER_ARG(x));
      publish(std::move(x));
    },
    [=](atom::publish, command_message& x) {
      BROKER_TRACE(BROKER_ARG(x));
      publish(std::move(x));
    },
    // --- communication to local actors only, i.e., never forward to peers ----
    [=](atom::publish, atom::local, data_message& x) {
      BROKER_TRACE(BROKER_ARG(x));
      local_push(std::move(x));
    },
    [=](atom::publish, atom::local, command_message& x) {
      BROKER_TRACE(BROKER_ARG(x));
      local_push(std::move(x));
    },
    // --- "one-to-one" communication that bypasses streaming entirely ---------
    [=](atom::publish, endpoint_info& e, data_message& x) {
      BROKER_TRACE(BROKER_ARG(e) << BROKER_ARG(x));
      actor hdl;
      if (e.network) {
        auto tmp = cache().find(*e.network);
        if (tmp)
          hdl = std::move(*tmp);
      }
      if (!hdl) {
        auto predicate = [&](const actor& x) { return x.node() == e.node; };
        hdl = find_output_peer_hdl(std::move(predicate));
        if (!hdl) {
          BROKER_ERROR("no node found for endpoint info" << e);
          return;
        }
      }
      self()->send(hdl, atom::publish_v, atom::local_v, std::move(x));
    },
    // --- accessors -----------------------------------------------------------
    [=](atom::get, atom::peer) {
      std::vector<peer_info> result;
      auto add = [&](actor hdl, peer_status status) {
        peer_info tmp;
        tmp.status = status;
        tmp.flags = peer_flags::remote + peer_flags::inbound
                    + peer_flags::outbound;
        tmp.peer.node = hdl.node();
        auto addrs = cache().find(hdl);
        // the peer_info only holds a single address, so ... pick first?
        if (addrs)
          tmp.peer.network = *addrs;
        result.emplace_back(std::move(tmp));
      };
      // collect connected peers
      for_each_peer([&](const actor& hdl) {
        add(hdl, peer_status::peered);
      });
      // collect pending peers
      for (const auto& kvp : pending_connections())
        add(kvp.first, peer_status::connecting);
      return result;
    },
    [=](atom::get, atom::peer, atom::subscriptions) {
      std::vector<topic> result;
      // Collect filters for all peers.
      for_each_filter([&](auto x) {
        result.insert(result.end(), std::make_move_iterator(x.begin()),
                      std::make_move_iterator(x.end()));
      });
      // Sort and drop duplicates.
      std::sort(result.begin(), result.end());
      auto e = std::unique(result.begin(), result.end());
      if (e != result.end())
        result.erase(e, result.end());
      return result;
    },
    // --- destructive state manipulations -------------------------------------
    [=](atom::unpeer, actor x) { unpeer(x); },
    [=](atom::shutdown) {
      self()->quit(exit_reason::user_shutdown);
      /* -- To consider:
         -- Terminating the actor after receiving shutdown unconditionally can
         -- cause already published data to not getting forwarded. The
         -- following code implements a more complicated shutdown procedure
         -- that would make sure data is transmitted before shutting down.
         -- However, this is often undesirable because it can take an arbitrary
         -- long time. Also, the current implementation does not terminate in
         -- all cases, i.e., seems not bug-free.
      auto& st = self->state;
      st.shutting_down = true;
      // Shutdown immediately if no local sink or source is connected.
      if (st.policy().at_end()) {
        BROKER_DEBUG("Terminate core actor after receiving 'shutdown'");
        self->quit(exit_reason::user_shutdown);
        return;
      }
      // Wait until local sinks and sources are done, but no longer respond to
      // any future message.
      BROKER_DEBUG("Delay termination of core actor after receiving "
                    "'shutdown' until local sinks and sources are done; "
                    "workers.size:" << st.policy().workers().num_paths()
                    << ", stores.size:" << st.policy().stores().num_paths());
      self->set_default_handler(caf::drop);
      self->become(
        [] {
          // Dummy behavior to keep the actor alive but unresponsive.
        }
      );
      */
    },
    [=](atom::shutdown, atom::store) {
      for (auto& kvp : masters_)
        self()->send_exit(kvp.second, caf::exit_reason::user_shutdown);
      for (auto& kvp : clones_)
        self()->send_exit(kvp.second, caf::exit_reason::user_shutdown);
    });
}

} // namespace broker
