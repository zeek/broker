#include "broker/internal/core_actor.hh"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/behavior.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/group.hpp>
#include <caf/make_counted.hpp>
#include <caf/none.hpp>
#include <caf/response_promise.hpp>
#include <caf/result.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/sec.hpp>
#include <caf/spawn_options.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/stream.hpp>
#include <caf/stream_slot.hpp>
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/make_backend.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/domain_options.hh"
#include "broker/filter_type.hh"
#include "broker/internal/clone_actor.hh"
#include "broker/internal/master_actor.hh"

namespace broker::internal {

// --- constructors and destructors --------------------------------------------

core_actor_state::core_actor_state(caf::event_based_actor* self,
                                   endpoint_id this_peer,
                                   filter_type initial_filter,
                                   endpoint::clock* clock,
                                   const domain_options* adaptation,
                                   connector_ptr conn)
  : self(self),
    id(this_peer),
    filter(std::make_shared<shared_filter_type>(std::move(initial_filter))),
    clock(clock) {
  // Check for extra configuration parameters.
  if (adaptation && adaptation->disable_forwarding) {
    BROKER_INFO("disable forwarding on this peer");
    disable_forwarding = true;
  } else {
    BROKER_INFO("enable forwarding on this peer (default)");
  }
  // Callback setup when running with a connector attached.
  if (conn) {
    auto on_peering = [this](endpoint_id remote_id, const network_info& addr,
                             const filter_type& filter,
                             pending_connection_ptr conn) {
      std::ignore = init_new_peer(remote_id, addr, filter, std::move(conn));
    };
    auto on_peer_unavailable = [this](const network_info& addr) {
      peer_unavailable(addr);
    };
    adapter.reset(new connector_adapter(self, std::move(conn), on_peering,
                                        on_peer_unavailable, filter,
                                        peer_statuses));
  }
}

core_actor_state::~core_actor_state() {
  BROKER_DEBUG("core_actor_state destroyed");
}

// -- initialization and tear down ---------------------------------------------

caf::behavior core_actor_state::make_behavior() {
  // Our metrics for keeping track of how many messages pass through this peer.
  // Indexes into the array are values of packed_message_type, they start at 1.
  auto proc_fam = self->system().metrics().counter_family(
    "broker", "processed-elements", {"type"},
    "Number of processed stream elements.");
  using counter_ptr = caf::telemetry::int_counter*;
  std::array<counter_ptr, 5> counters{{
    nullptr,
    proc_fam->get_or_add({{"type", "data"}}),
    proc_fam->get_or_add({{"type", "command"}}),
    proc_fam->get_or_add({{"type", "routing-update"}}),
  }};
  // Create the mergers.
  auto init_merger = [this](auto& ptr) {
    ptr.emplace(self);                     // Allocate the object.
    ptr->delay_error(true);                // Do not stop on errors.
    ptr->shutdown_on_last_complete(false); // Always keep running.
  };
  init_merger(data_inputs);
  init_merger(command_inputs);
  init_merger(central_merge);
  // Connect incoming data and command messages to the central merge node.
  central_merge->add(
    data_inputs->as_observable().map([this](const data_message& msg) {
      return make_node_message(id, endpoint_id::nil(), pack(msg));
    }));
  central_merge->add(
    command_inputs->as_observable().map([this](const command_message& msg) {
      return make_node_message(id, endpoint_id::nil(), pack(msg));
    }));
  // Process filter updates from our peers and add instrumentation for metrics.
  central_merge //
    ->as_observable()
    .for_each([this, counters](const node_message& msg) {
      auto sender = get_sender(msg);
      // Update metrics.
      counters[static_cast<size_t>(get_type(msg))]->inc();
      // We only care about incoming filter updates messages here.
      if (sender == id || get_type(msg) != packed_message_type::routing_update)
        return;
      // Deserialize payload and update peer filter.
      if (auto i = peer_filters.find(sender); i != peer_filters.end()) {
        filter_type new_filter;
        caf::binary_deserializer src{nullptr, get_payload(msg)};
        if (src.apply(new_filter)) {
          i->second.swap(new_filter);
        } else {
          BROKER_ERROR("received malformed routing update from" << sender);
        }
      } else {
        // Ignore. Probably a stale message after unpeering.
      }
    });
  // Override the default exit handler to add logging.
  self->set_exit_handler([this](caf::exit_msg& msg) {
    if (msg.reason) {
      BROKER_DEBUG("shutting down after receiving an exit message with reason:"
                   << msg.reason);
      shutdown(shutdown_options{});
    }
  });
  // Create the behavior (set of message handlers / callbacks) for the actor.
  caf::behavior result{
    // -- peering --------------------------------------------------------------
    [this](atom::listen, const std::string& addr, uint16_t port) {
      auto rp = self->make_response_promise();
      if (!adapter) {
        rp.deliver(make_error(ec::no_connector_available));
      } else {
        adapter->async_listen(
          addr, port,
          [rp](uint16_t actual_port) mutable {
            rp.deliver(atom::listen_v, atom::ok_v, actual_port);
          },
          [rp](const caf::error& what) mutable { rp.deliver(what); });
      }
    },
    [this](atom::peer, const network_info& addr) {
      auto rp = self->make_response_promise();
      try_connect(addr, rp);
      return rp;
    },
    [this](atom::peer, endpoint_id peer, const network_info& addr,
           const filter_type& filter, node_consumer_res in_res,
           node_producer_res out_res) -> caf::result<void> {
      if (auto err = init_new_peer(peer, addr, filter, in_res, out_res))
        return err;
      else
        return caf::unit;
    },
    // -- unpeering ------------------------------------------------------------
    [this](atom::unpeer, const network_info& peer_addr) { //
      unpeer(peer_addr);
    },
    [this](atom::unpeer, endpoint_id peer_id) { //
      unpeer(peer_id);
    },
    // -- getters --------------------------------------------------------------
    [this](atom::get, atom::peer) {
      std::vector<peer_info> result;
      for (const auto& [peer_id, state] : peers) {
        endpoint_info info{peer_id, state.addr};
        result.emplace_back(peer_info{std::move(info), peer_flags::remote,
                                      peer_status::connected});
      }
      return result;
    },
    [this](atom::get_filter) { return filter->read(); },
    // -- publishing of messages without going through a publisher -------------
    [this](atom::publish, const data_message& msg) {
      dispatch(endpoint_id::nil(), pack(msg));
    },
    [this](atom::publish, const data_message& msg, const endpoint_info& dst) {
      dispatch(dst.node, pack(msg));
    },
    [this](atom::publish, const data_message& msg, endpoint_id dst) {
      dispatch(dst, pack(msg));
    },
    [this](atom::publish, atom::local, const data_message& msg) {
      dispatch(id, pack(msg));
    },
    [this](atom::publish, const command_message& msg) {
      dispatch(endpoint_id::nil(), pack(msg));
    },
    [this](atom::publish, const command_message& msg,
           const endpoint_info& dst) { //
      dispatch(dst.node, pack(msg));
    },
    [this](atom::publish, const command_message& msg, endpoint_id dst) {
      dispatch(dst, pack(msg));
    },
    // -- interface for subscribers --------------------------------------------
    [this](filter_type& filter, data_producer_res snk) {
      subscribe(filter);
      get_or_init_data_outputs()
        .filter([xs = std::move(filter)](const data_message& msg) {
          detail::prefix_matcher f;
          return f(xs, msg);
        })
        .subscribe(std::move(snk));
    },
    [this](std::shared_ptr<filter_type> fptr, data_producer_res snk) {
      // Here, we accept a shared_ptr to the filter instead of an actual object.
      // This allows the subscriber to manipulate the filter later by sending us
      // an update message. The filter itself is not thread-safe. Hence, the
      // publishers should never write to it directly.
      subscribe(*fptr);
      get_or_init_data_outputs()
        .filter([fptr = std::move(fptr)](const data_message& msg) {
          detail::prefix_matcher f;
          return f(*fptr, msg);
        })
        .subscribe(std::move(snk));
    },
    [this](std::shared_ptr<filter_type>& fptr, topic& x, bool add,
           std::shared_ptr<std::promise<void>>& sync) {
      // We assume that fptr belongs to a previously constructed flow.
      auto e = fptr->end();
      auto i = std::find(fptr->begin(), e, x);
      if (add) {
        if (i == e) {
          fptr->emplace_back(std::move(x));
          subscribe(*fptr);
        }
      } else {
        if (i != e)
          fptr->erase(i);
      }
      if (sync)
        sync->set_value();
    },
    // -- interface for publishers ---------------------------------------------
    [this](data_consumer_res src) {
      auto sub = data_inputs->add(self->make_observable().from_resource(src));
      subscriptions.emplace_back(std::move(sub));
    },
    // -- data store management ------------------------------------------------
    [this](atom::store, atom::clone, atom::attach, const std::string& name,
           double resync_interval, double stale_interval,
           double mutation_buffer_interval) {
      return attach_clone(name, resync_interval, stale_interval,
                          mutation_buffer_interval);
    },
    [this](atom::store, atom::master, atom::attach, const std::string& name,
           backend backend_type, backend_options opts) {
      return attach_master(name, backend_type, opts);
    },
    [this](atom::store, atom::master, atom::get,
           const std::string& name) -> caf::result<caf::actor> {
      auto i = masters.find(name);
      if (i != masters.end())
        return i->second;
      else
        return caf::make_error(ec::no_such_master);
    },
    [this](atom::shutdown, atom::store) { //
      shutdown_stores();
    },
    // -- interface for legacy subscribers -------------------------------------
    [this](atom::join, const filter_type& filter) mutable {
      // Sanity checking: reject anonymous messages.
      if (self->current_sender() == nullptr)
        return;
      // Take selected messages out of the flow and send them via asynchronous
      // messages to the client.
      auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
      subscribe(filter);
      auto sub = get_or_init_data_outputs()
                   .filter([filter](const data_message& item) {
                     detail::prefix_matcher f;
                     return f(filter, item);
                   })
                   .for_each([this, hdl](const data_message& msg) {
                     self->send(hdl, msg);
                   });
      // Drop this `for_each`-subscription if the client goes down.
      auto weak_self = self->address();
      hdl->attach_functor([weak_self, sub]() mutable {
        if (auto strong_self = caf::actor_cast<caf::actor>(weak_self)) {
          auto cb = caf::make_action([sub]() mutable { sub.dispose(); });
          caf::anon_send(strong_self, std::move(cb));
        }
      });
    },
    // -- miscellaneous --------------------------------------------------------
    [this](atom::shutdown, shutdown_options opts) { //
      shutdown(opts);
    },
    [this](atom::no_events) { //
      disable_notifications = true;
    },
    [this](atom::await, endpoint_id peer_id) {
      auto rp = self->make_response_promise();
      if (auto i = peers.find(peer_id);
          i != peers.end() && !i->second.invalidated)
        rp.deliver(peer_id);
      else
        awaited_peers.emplace(peer_id, rp);
      return rp;
    },
  };
  if (adapter) {
    return adapter->message_handlers().or_else(result);
  } else {
    return result;
  }
}

void core_actor_state::shutdown(shutdown_options options) {
  BROKER_TRACE(BROKER_ARG(options));
  // Shut down data stores.
  shutdown_stores();
  // Drop all peers. Don't cancel their flows, though. Incoming flows were
  // cancelled already and output flows get closed automatically once the
  // mergers shut down.
  for (auto& kvp: peers) {
    auto& [peer_id, st] = kvp;
    if (!st.invalidated) {
      BROKER_DEBUG("drop state for" << peer_id);
      // Drop shared state for this peer.
      auto& psm = *peer_statuses;
      BROKER_DEBUG(peer_id << "::" << psm.get(peer_id) << "-> ()");
      psm.remove(peer_id);
      // Emit events.
      peer_removed(peer_id);
      peer_unreachable(peer_id);
    }
  }
  peers.clear();
  // Cancel all incoming flows.
  BROKER_DEBUG("cancel" << subscriptions.size() << "subscriptions");
  for (auto& sub : subscriptions)
    sub.dispose();
  subscriptions.clear();
  // Allow our mergers to shut down. They still process pending data.
  data_inputs->shutdown_on_last_complete(true);
  command_inputs->shutdown_on_last_complete(true);
  central_merge->shutdown_on_last_complete(true);
  // Inform our clients that we no longer wait for any peer.
  BROKER_DEBUG("cancel" << awaited_peers.size()
                        << "pending await_peer requests");
  for (auto& kvp : awaited_peers)
    kvp.second.deliver(make_error(ec::shutting_down));
  awaited_peers.clear();
  // Ignore future messages. Calling unbecome() removes our 'behavior' (set of
  // message handlers). An actor without behavior runs as long as still has
  // active flows. Once the flows processed currently pending data the actor
  // goes down.
  self->unbecome();
  self->set_default_handler(
    [](caf::scheduled_actor* sptr, caf::message&) -> caf::skippable_result {
      // Usually, the default handler always produces an error. However, we
      // simply want to ignore any incoming message from now on. This means for
      // regular, asynchronous messages we just produce a 'void' result by
      // returning an empty message. For requests, we still do produce an error
      // since we otherwise break request/response semantics silently.
      if (sptr->current_message_id().is_request())
        return caf::make_error(caf::sec::request_receiver_down);
      else
        return caf::make_message();
    });
}

// -- convenience functions ----------------------------------------------------

template <class EnumConstant>
void core_actor_state::emit(endpoint_info ep, EnumConstant code,
                            const char* msg) {
  // Sanity checking.
  if (disable_notifications || !data_outputs)
    return;
  // Pick the right topic and factory based on the event type.
  using value_type = typename EnumConstant::value_type;
  std::string str;
  if constexpr (std::is_same_v<value_type, sc>)
    str = topic::statuses_str;
  else
    str = topic::errors_str;
  using factory
    = std::conditional_t<std::is_same_v<value_type, sc>, status, error_factory>;
  // Generate a data message from the converted content and address it to this
  // node only. This ensures that the data remains visible locally only.
  auto val = factory::make(code, std::move(ep), msg);
  try {
    auto content = get_as<data>(val);
    dispatch(id, pack(make_data_message(std::move(str), std::move(content))));
  } catch (std::exception&) {
    std::cerr << "*** failed to convert " << caf::deep_to_string(val)
              << " to data\n";
  }
}

template <class T>
packed_message core_actor_state::pack(const T& msg) {
  buf.clear();
  caf::binary_serializer snk{nullptr, buf};
  if constexpr (std::is_same_v<T, data_message>) {
    std::ignore = snk.apply(get_data(msg));
  } else {
    static_assert(std::is_same_v<T, command_message>);
    std::ignore = snk.apply(get_command(msg));
  }
  return make_packed_message(packed_message_type_v<T>, ttl, get_topic(msg),
                             buf);
}

template <class T>
std::optional<T> core_actor_state::unpack(const packed_message& msg) {
  caf::binary_deserializer src{nullptr, get_payload(msg)};
  if constexpr (std::is_same_v<T, data_message>) {
    data content;
    if (src.apply(content))
      return make_data_message(get_topic(msg), std::move(content));
    else
      return std::nullopt;
  } else {
    static_assert(std::is_same_v<T, command_message>);
    internal_command content;
    if (src.apply(content))
      return make_command_message(get_topic(msg), std::move(content));
    else
      return std::nullopt;
  }
}

bool core_actor_state::has_remote_subscriber(const topic& x) const noexcept {
  auto is_subscribed = [&x, f = detail::prefix_matcher{}](auto& kvp) {
    return f(kvp.second, x);
  };
  return std::any_of(peer_filters.begin(), peer_filters.end(), is_subscribed);
}

bool core_actor_state::is_subscribed_to(endpoint_id id, const topic& what) {
  if (auto i = peer_filters.find(id); i != peer_filters.end()) {
    detail::prefix_matcher f;
    return f(i->second, what);
  } else {
    return false;
  }
}

std::optional<network_info> core_actor_state::addr_of(endpoint_id id) const {
  if (auto i = peers.find(id); i != peers.end())
    return i->second.addr;
  else
    return std::nullopt;
}

std::vector<endpoint_id> core_actor_state::peer_ids() const {
  std::vector<endpoint_id> result;
  for (auto& kvp : peers)
    result.emplace_back(kvp.first);
  return result;
}

// -- callbacks ----------------------------------------------------------------

void core_actor_state::peer_discovered(endpoint_id peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  emit(endpoint_info{peer_id, std::nullopt},
       sc_constant<sc::endpoint_discovered>(),
       "found a new peer in the network");
}

void core_actor_state::peer_connected(endpoint_id peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  emit(endpoint_info{peer_id, addr_of(peer_id)}, sc_constant<sc::peer_added>(),
       "handshake successful");
}

void core_actor_state::peer_disconnected(endpoint_id peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  emit(endpoint_info{peer_id, std::nullopt}, sc_constant<sc::peer_lost>(),
       "lost connection to remote peer");
  peer_filters.erase(peer_id);
}

void core_actor_state::peer_removed(endpoint_id peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  emit(endpoint_info{peer_id, std::nullopt}, sc_constant<sc::peer_removed>(),
       "removed connection to remote peer");
  peer_filters.erase(peer_id);
}

void core_actor_state::peer_unreachable(endpoint_id peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  emit(endpoint_info{peer_id, std::nullopt},
       sc_constant<sc::endpoint_unreachable>(), "lost the last path");
  peer_filters.erase(peer_id);
}

void core_actor_state::cannot_remove_peer(endpoint_id peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  emit(endpoint_info{peer_id, std::nullopt}, ec_constant<ec::peer_invalid>(), "cannot unpeer from unknown peer");
  BROKER_DEBUG("cannot unpeer from unknown peer" << peer_id);
}

void core_actor_state::cannot_remove_peer(const network_info& addr) {
  BROKER_TRACE(BROKER_ARG(addr));
  emit(endpoint_info{endpoint_id::nil(), addr}, ec_constant<ec::peer_invalid>(),
       "cannot unpeer from unknown peer");
  BROKER_DEBUG("cannot unpeer from unknown peer" << addr);
}

void core_actor_state::peer_unavailable(const network_info& addr) {
  BROKER_TRACE(BROKER_ARG(addr));
  emit(endpoint_info{endpoint_id::nil(), addr},
       ec_constant<ec::peer_unavailable>(), "unable to connect to remote peer");
}

// -- connection management ----------------------------------------------------

void core_actor_state::try_connect(const network_info& addr,
                                   caf::response_promise rp) {
  BROKER_TRACE(BROKER_ARG(addr));
  if (!adapter) {
    rp.deliver(make_error(ec::no_connector_available));
    return;
  }
  adapter->async_connect(
    addr,
    [this, rp](endpoint_id peer, const network_info& addr,
               const filter_type& filter, pending_connection_ptr conn) mutable {
      BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(addr) << BROKER_ARG(filter));
      if (auto err = init_new_peer(peer, addr, filter, std::move(conn));
          err && err != ec::repeated_peering_handshake_request)
        rp.deliver(std::move(err));
      else
        rp.deliver(atom::peer_v, atom::ok_v, peer);
    },
    [this, rp](endpoint_id peer, const network_info& addr) mutable {
      BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(addr));
      if (auto i = peers.find(peer); i != peers.end()) {
        // Override the address if this one has a retry field. This makes
        // sure we "prefer" a user-defined address over addresses we read
        // from sockets for incoming peerings.
        if (addr.has_retry_time() && !i->second.addr.has_retry_time())
          i->second.addr = addr;
        rp.deliver(atom::peer_v, atom::ok_v, peer);
      } else {
        // Race on the state. May happen if the remote peer already
        // started a handshake and the connector thus drops this request
        // as redundant. We should already have the connect event queued,
        // so we just enqueue this request again and the second time
        // we should find it in the cache.
        using namespace std::literals;
        self->run_delayed(1ms, [this, peer, addr, rp]() mutable {
          BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(addr));
          if (auto i = peers.find(peer); i != peers.end()) {
            if (addr.has_retry_time() && !i->second.addr.has_retry_time())
              i->second.addr = addr;
            rp.deliver(atom::peer_v, atom::ok_v, peer);
          } else {
            try_connect(addr, rp);
          }
        });
      }
    },
    [this, rp, addr](const caf::error& what) mutable {
      BROKER_TRACE(BROKER_ARG(what));
      rp.deliver(std::move(what));
      peer_unavailable(addr);
    });
}

void core_actor_state::handle_peer_close_event(endpoint_id peer_id,
                                               lamport_timestamp ts,
                                               caf::error& reason) {
  if (auto i = peers.find(peer_id);
      i != peers.end() && !i->second.invalidated && i->second.ts == ts) {
    // Update our 'global' state for this peer.
    auto status = peer_status::peered;
    if (peer_statuses->update(peer_id, status, peer_status::disconnected)) {
      BROKER_DEBUG(peer_id << ":: peered -> disconnected");
    } else {
      BROKER_ERROR("invalid status for new peer" << BROKER_ARG(peer_id)
                                                 << BROKER_ARG(status));
      // TODO: we could also consider this a fatal error instead. Probably worth
      //       a discussion or two. For now, we just assume so other part of the
      //       system is doing something with this peer and leave the state.
      return;
    }
    // Close any pending flows and mark as disconnected / invalid.
    i->second.invalidated = true;
    i->second.in.dispose();
    i->second.out.dispose();
    // Trigger events.
    peer_disconnected(peer_id);
    peer_unreachable(peer_id);
    // If there is a retry time, it means that we have established the peering.
    // Try reconnecting.
    if (i->second.addr.has_retry_time()) {
      try_connect(i->second.addr, caf::response_promise{});
    }
  } else {
    // Nothing to do. We have already cleaned up this state, probably as a
    // result of unpeering.
  }
}

// -- flow management ----------------------------------------------------------

caf::flow::observable<data_message>
core_actor_state::get_or_init_data_outputs() {
  if (!data_outputs) {
    BROKER_DEBUG("create data outputs");
    // Hook into the central merge point.
    data_outputs
      = central_merge
          ->as_observable()
          // Drop everything but data messages and only process messages that
          // are not meant for another peer.
          .filter([this](const node_message& msg) {
            // Note: local subscribers do not receive messages from local
            // publishers. Except when the message explicitly says otherwise by
            // setting receiver == id. This is the case for messages that were
            // published via `(atom::publish, atom::local, ...)` message.
            auto receiver = get_receiver(msg);
            return get_type(msg) == packed_message_type::data
                   && (get_sender(msg) != id || receiver == id)
                   && (!receiver || receiver == id);
          })
          // Deserialize payload and wrap it into an actual data message.
          .flat_map_optional([this](const node_message& msg) {
            return unpack<data_message>(get_packed_message(msg));
          })
          // Convert this blueprint to an actual observable.
          .as_observable();
  }
  return data_outputs;
}

caf::flow::observable<command_message>
core_actor_state::get_or_init_command_outputs() {
  if (!command_outputs) {
    BROKER_DEBUG("create command outputs");
    // Hook into the central merge point.
    command_outputs
      = central_merge
          ->as_observable()
          // Drop everything but command messages and only process messages that
          // are not meant for another peer.
          .filter([this](const node_message& msg) {
            auto receiver = get_receiver(msg);
            return get_type(msg) == packed_message_type::command
                   && (!receiver || receiver == id);
          })
          // Deserialize payload and wrap it into an actual command message.
          .flat_map_optional([this](const node_message& msg) {
            return unpack<command_message>(get_packed_message(msg));
          })
          // Convert this blueprint to an actual observable.
          .as_observable();
  }
  return command_outputs;
}

caf::error core_actor_state::init_new_peer(endpoint_id peer_id,
                                           const network_info& addr,
                                           const filter_type& filter,
                                           node_consumer_res in_res,
                                           node_producer_res out_res) {
  BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(filter));
  // Sanity checking: make sure this isn't a repeated handshake.
  auto i = peers.find(peer_id);
  if (i != peers.end() && !i->second.invalidated)
    return caf::make_error(ec::repeated_peering_handshake_request);
  // Set the status for this peer to 'peered'. The only legal transitions are
  // 'nil -> peered' and 'connected -> peered'.
  auto& psm = *peer_statuses;
  auto status = peer_status::peered;
  if (psm.insert(peer_id, status)) {
    BROKER_DEBUG(peer_id << ":: () -> peered");
  } else if (status == peer_status::connected
             && psm.update(peer_id, status, peer_status::peered)) {
    BROKER_DEBUG(peer_id << ":: connected -> peered");
  } else {
    BROKER_ERROR("invalid status for new peer" << BROKER_ARG(peer_id)
                                               << BROKER_ARG(status));
    return caf::make_error(ec::invalid_status, to_string(status));
  }
  // Store the filter for is_subscribed_to.
  peer_filters[peer_id] = filter;
  // Hook into the central merge point for forwarding the data to the peer.
  auto& sys = self->system();
  auto out = central_merge
               ->as_observable()
               // Select by subscription and sender/receiver fields.
               .filter([this, pid = peer_id](const node_message& msg) {
                 if (get_sender(msg) == pid )
                   return false;
                 if (disable_forwarding && get_sender(msg) != id)
                   return false;
                 auto receiver = get_receiver(msg);
                 return receiver == pid
                        || (!receiver && is_subscribed_to(pid, get_topic(msg)));
               })
               // Override the sender field. This makes sure the sender field
               // always reflects the last hop. Since we only need this
               // information to avoid forwarding loops, "sender" really just
               // means "last hop" right now.
               .map([this](const node_message& msg) {
                 if (get_sender(msg) == id) {
                   return msg;
                 } else {
                   using std::get;
                   auto cpy = msg;
                   get<0>(cpy.unshared()) = id;
                   return cpy;
                 }
               })
               // Emit values to the producer resource.
               .subscribe(out_res);
  // Increase the logical time for this connection. This timestamp is crucial
  // for our do_finally handler, because this handler must do nothing if we have
  // already removed the connection manually, e.g., as a result of unpeering.
  // Otherwise, we might drop a re-connected peer on accident.
  auto ts = (i == peers.end()) ? lamport_timestamp{} : ++i->second.ts;
  // Read messages from the peer.
  auto in = self->make_observable()
              .from_resource(in_res)
              // If the peer closes this buffer, we assume a disconnect.
              .do_finally([this, peer_id, ts] { //
                caf::error reason;
                handle_peer_close_event(peer_id, ts, reason);
              })
              .as_observable();
  // Push messages received from the peer into the central merge point.
  central_merge->add(in);
  // Store some state to allow us to unpeer() from the node later.
  subscriptions.emplace_back(in.as_disposable());
  if (i == peers.end()) {
    peers.emplace(peer_id, peer_state{std::move(in).as_disposable(),
                                      std::move(out), addr});
  } else {
    i->second.in = std::move(in).as_disposable();
    i->second.out = std::move(out);
    i->second.invalidated = false;
  }
  // Emit status updates.
  peer_discovered(peer_id);
  peer_connected(peer_id);
  // Notify clients that wait for this peering.
  if (auto [first, last] = awaited_peers.equal_range(peer_id); first != last) {
    for (auto i = first; i != last; ++i)
      i->second.deliver(peer_id);
    awaited_peers.erase(first, last);
  }
  return caf::none;
}

caf::error core_actor_state::init_new_peer(endpoint_id peer,
                                           const network_info& addr,
                                           const filter_type& filter,
                                           pending_connection_ptr ptr) {
  // Spin up a background worker that takes care of socket I/O. We communicate
  // to this worker via producer/consumer buffer resources. The [rd_1, wr_1]
  // pair is the direction core actor -> network. The other pair is for the
  // opposite direction. The caf::net::length_prefix_framing protocol is simple
  // framing protocol that suffixes a message with a four-byte 'header' for the
  // message size. So on the wire, every byte block that our trait produces gets
  // prefixed with 32 bit with the size.
  namespace cn = caf::net;
  auto [rd_1, wr_1] = caf::async::make_spsc_buffer_resource<node_message>();
  auto [rd_2, wr_2] = caf::async::make_spsc_buffer_resource<node_message>();
  if (auto err = ptr->run(self->system(), std::move(rd_1), std::move(wr_2))) {
    return err;
  } else {
    // With the connected buffers, dispatch to the other overload.
    return init_new_peer(peer, addr, filter, std::move(rd_2), std::move(wr_1));
  }
}

// -- topic management ---------------------------------------------------------

void core_actor_state::subscribe(const filter_type& what) {
  BROKER_TRACE(BROKER_ARG(what));
  auto changed = filter->update([this, &what](auto&, auto& xs) {
    auto not_internal = [](const topic& x) { return !is_internal(x); };
    if (filter_extend(xs, what, not_internal)) {
      return true;
    } else {
      return false;
    }
  });
  // Note: this member function is the only place we call `update`. Hence, we
  // need not worry about the filter changing again concurrently.
  if (changed) {
    broadcast_subscriptions();
  } else {
    BROKER_DEBUG("already subscribed to topics:" << what);
  }
}

// -- data store management --------------------------------------------------

bool core_actor_state::has_remote_master(const std::string& name) const {
  // A master would subscribe to its 'special topic', so we would see a
  // subscription for that topic if another node has a master attached. This
  // still has a big asterisk attached to it since the subscription message may
  // just be in flight. This check is better than nothing, though.
  return has_remote_subscriber(name / topic::master_suffix());
}

caf::result<caf::actor> core_actor_state::attach_master(const std::string& name,
                                                        backend backend_type,
                                                        backend_options opts) {
  BROKER_TRACE(BROKER_ARG(name)
               << BROKER_ARG(backend_type) << BROKER_ARG(opts));
  // Sanity checking: master must not already exist locally or on a peer.
  if (auto i = masters.find(name); i != masters.end())
    return i->second;
  if (has_remote_master(name)) {
    BROKER_WARNING("remote master with same name exists already");
    return caf::make_error(ec::master_exists);
  }
  // Create backend and buffers.
  auto ptr = detail::make_backend(backend_type, std::move(opts));
  if (!ptr)
    return caf::make_error(ec::backend_failure);
  BROKER_INFO("spawning new master:" << name);
  using caf::async::make_spsc_buffer_resource;
  auto [con1, prod1] = make_spsc_buffer_resource<command_message>();
  auto [con2, prod2] = make_spsc_buffer_resource<command_message>();
  // Spin up the master and connect it to our flows.
  auto hdl = self->system().spawn<master_actor_type>(
    id, name, std::move(ptr), caf::actor{self}, clock, std::move(con1),
    std::move(prod2));
  filter_type filter{name / topic::master_suffix()};
  subscribe(filter);
  get_or_init_command_outputs()
    .filter([xs = filter](const command_message& item) {
      detail::prefix_matcher f;
      return f(xs, item);
    })
    .subscribe(prod1);
  command_inputs->add(self->make_observable().from_resource(con2));
  // Save the handle and monitor the new actor.
  masters.emplace(name, hdl);
  self->link_to(hdl);
  return hdl;
}

caf::result<caf::actor>
core_actor_state::attach_clone(const std::string& name, double resync_interval,
                               double stale_interval,
                               double mutation_buffer_interval) {
  BROKER_TRACE(BROKER_ARG(name)
               << BROKER_ARG(resync_interval) << BROKER_ARG(stale_interval)
               << BROKER_ARG(mutation_buffer_interval));
  // Sanity checking: make sure there is no master or clone already.
  if (auto i = masters.find(name); i != masters.end()) {
    BROKER_WARNING("attempted to run clone & master on the same endpoint");
    return caf::make_error(ec::no_such_master);
  }
  if (auto i = clones.find(name); i != clones.end())
    return i->second;
  // Spin up the clone and connect it to our flows.
  BROKER_INFO("spawning new clone:" << name);
  using std::chrono::duration_cast;
  // TODO: make configurable.
  auto tout = duration_cast<timespan>(fractional_seconds{10});
  using caf::async::make_spsc_buffer_resource;
  auto [con1, prod1] = make_spsc_buffer_resource<command_message>();
  auto [con2, prod2] = make_spsc_buffer_resource<command_message>();
  auto hdl = self->system().spawn<clone_actor_type>(
    id, name, tout, caf::actor{self}, clock, std::move(con1), std::move(prod2));
  filter_type filter{name / topic::clone_suffix()};
  subscribe(filter);
  get_or_init_command_outputs()
    .filter([xs = filter](const command_message& item) {
      detail::prefix_matcher f;
      return f(xs, item);
    })
    .subscribe(prod1);
  command_inputs->add(self->make_observable().from_resource(con2));
  // Save the handle for later.
  clones.emplace(name, hdl);
  return hdl;
}

void core_actor_state::shutdown_stores() {
  BROKER_TRACE(BROKER_ARG2("masters.size()", masters.size())
               << BROKER_ARG2("clones.size()", clones.size()));
  // TODO: consider re-implementing graceful shutdown of the store actors
  for (auto& kvp : masters)
    self->send_exit(kvp.second, caf::exit_reason::kill);
  masters.clear();
  for (auto& kvp : clones)
    self->send_exit(kvp.second, caf::exit_reason::kill);
  clones.clear();
}

// -- dispatching of messages to peers regardless of subscriptions ------------

void core_actor_state::dispatch(endpoint_id receiver, packed_message msg) {
  central_merge->append_to_buf(make_node_message(id, receiver, msg));
  central_merge->try_push();
}

void core_actor_state::broadcast_subscriptions() {
  // Serialize the filter.
  auto fs = filter->read();
  buf.clear();
  caf::binary_serializer sink{nullptr, buf};
  [[maybe_unused]] auto ok = sink.apply(fs);
  BROKER_ASSERT(ok);
  // Pack and send to each peer.
  auto first = reinterpret_cast<std::byte*>(buf.data());
  auto last = first + buf.size();
  auto packed = packed_message{packed_message_type::routing_update, ttl,
                               topic{std::string{topic::reserved}},
                               std::vector<std::byte>{first, last}};
  for (auto& kvp : peers)
    central_merge->append_to_buf(node_message(id, kvp.first, packed));
  central_merge->try_push();
}

// -- unpeering ----------------------------------------------------------------

void core_actor_state::unpeer(endpoint_id peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  if (auto i = peers.find(peer_id); i != peers.end())
    unpeer(i);
  else
    cannot_remove_peer(peer_id);
}

void core_actor_state::unpeer(const network_info& peer_addr) {
  BROKER_TRACE(BROKER_ARG(peer_addr));
  auto pred = [peer_addr](auto& kvp) { return kvp.second.addr == peer_addr; };
  if (auto i = std::find_if(peers.begin(), peers.end(), pred); i != peers.end())
    unpeer(i);
  else
    cannot_remove_peer(peer_addr);
}

void core_actor_state::unpeer(peer_state_map::iterator i) {
  BROKER_TRACE("");
  if (i == peers.end()) {
    // Nothing to do.
  } else if (auto& st = i->second; st.invalidated) {
    BROKER_DEBUG(i->first << "already unpeered (invalidated)");
  } else {
    auto peer_id = i->first;
    BROKER_DEBUG("drop state for" << peer_id);
    // Drop local state for this peer.
    st.in.dispose();
    st.out.dispose();
    peers.erase(i);
    // Drop shared state for this peer.
    auto& psm = *peer_statuses;
    BROKER_DEBUG(peer_id << "::" << psm.get(peer_id) << "-> ()");
    psm.remove(peer_id);
    // Emit events.
    peer_removed(peer_id);
    peer_unreachable(peer_id);
  }
}

} // namespace broker::internal