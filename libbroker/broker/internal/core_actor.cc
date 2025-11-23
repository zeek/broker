#include "broker/internal/core_actor.hh"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/behavior.hpp>
#include <caf/binary_deserializer.hpp>
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
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>

#include "broker/detail/make_backend.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/domain_options.hh"
#include "broker/endpoint_id.hh"
#include "broker/filter_type.hh"
#include "broker/fwd.hh"
#include "broker/hub_id.hh"
#include "broker/internal/checked.hh"
#include "broker/internal/clone_actor.hh"
#include "broker/internal/master_actor.hh"
#include "broker/internal/wire_format.hh"
#include "broker/logger.hh"

#include <span>

using namespace std::literals;

namespace broker::internal {

// -- private utilities --------------------------------------------------------

namespace {

struct status_collector_state {
  using map_t =
    std::unordered_map<std::string, core_actor_state::store_handler_ptr>;

  status_collector_state(caf::event_based_actor* selfptr, map_t masters,
                         map_t clones)
    : self(selfptr),
      store_masters(std::move(masters)),
      store_clones(std::move(clones)) {
    // nop
  }

  caf::behavior make_behavior() {
    return {
      [this](table& res) {
        auto max_delay = defaults::store::max_get_delay;
        result = std::move(res);
        rp = self->make_response_promise();
        for (auto& entry : store_masters)
          self
            ->request(entry.second->hdl, max_delay, atom::get_v, atom::status_v)
            .then([this, key = entry.first](table& res) {
              on_master_response(key, res);
            });
        for (auto& entry : store_clones)
          self
            ->request(entry.second->hdl, max_delay, atom::get_v, atom::status_v)
            .then([this, key = entry.first](table& res) {
              on_clone_response(key, res);
            });
        self->unbecome();
        return rp;
      },
    };
  }

  void on_response(map_t& src, data category, const std::string& key,
                   table& res) {
    // Add a new entry to the result table.
    auto i = src.find(key);
    if (i == src.end())
      return;
    auto j = result.find(category);
    if (j == result.end()) {
      table entry;
      entry.emplace(data{key}, std::move(res));
      result.emplace(std::move(category), data{std::move(entry)});
    } else if (auto entries = get_if<table>(j->second)) {
      entries->emplace(data{key}, std::move(res));
    } else {
      log::core::error("on-response",
                       "status collector found a malformed result table");
    }
    // Mark as processed and emit result after receiving all responses.
    src.erase(i);
    if (store_masters.size() + store_clones.size() == 0) {
      rp.deliver(std::move(result));
      self->quit();
    }
  }

  void on_master_response(const std::string& key, table& res) {
    on_response(store_masters, {"masters"s}, key, res);
  }

  void on_clone_response(const std::string& key, table& res) {
    on_response(store_clones, {"clones"s}, key, res);
  }

  caf::event_based_actor* self;
  map_t store_masters;
  map_t store_clones;
  table result;
  caf::response_promise rp;
};

using status_collector_actor = caf::stateful_actor<status_collector_state>;

} // namespace

// -- nested classes -----------------------------------------------------------

const caf::chunk& core_actor_state::message_provider::as_binary() {
  if (!binary_) {
    buffer_.clear();
    wire_format::v1::trait trait;
    if (!trait.convert(msg_, buffer_)) {
      log::core::error("message_provider",
                       "failed to convert message to chunk");
      return binary_;
    }
    binary_ = caf::chunk{buffer_};
  }
  return binary_;
}

core_actor_state::offer_result
core_actor_state::peering_handler::offer(message_provider& provider) {
  auto& msg = provider.get();
  // Filter out messages from other peers unless forwarding is enabled.
  auto&& src = get_sender(msg);
  if (parent->disable_forwarding && src.valid() && src != parent->id) {
    return offer_result::skip;
  }
  // Filter out messages that aren't broadcasted (have no receiver) unless they
  // are directed to this peer explicitly.
  auto&& dst = get_receiver(msg);
  if (dst.valid() && dst != id) {
    return offer_result::skip;
  }
  return super::offer(provider);
}

node_message core_actor_state::peering_handler::make_bye_message() {
  bye_token token;
  assign_bye_token(token);
  return make_ping_message(parent->id, id, token.data(), token.size());
}

void core_actor_state::peering_handler::assign_bye_token(bye_token& buf) {
  const auto* prefix = "BYE";
  const auto* suffix = &bye_id;
  memcpy(buf.data(), prefix, 3);
  memcpy(buf.data() + 3, suffix, 8);
}

// -- constructors and destructors ---------------------------------------------

core_actor_state::handler::~handler() {
  // nop
}

core_actor_state::metrics_t::metrics_t(prometheus::Registry& reg) {
  metric_factory factory{reg};
  // Initialize connection metrics.
  auto [native, ws] = factory.core.connections_instances();
  native_connections = native;
  web_socket_connections = ws;
  // Initialize message metrics, indexes are according to packed_message_type.
  auto proc = factory.core.processed_messages_instances();
  message_metric_sets[0].processed = proc.data;
  message_metric_sets[1].processed = proc.command;
  message_metric_sets[2].processed = proc.routing_update;
  message_metric_sets[3].processed = proc.ping;
  message_metric_sets[4].processed = proc.pong;
}

core_actor_state::core_actor_state(caf::event_based_actor* self, //
                                   prometheus_registry_ptr reg,
                                   endpoint_id this_peer,
                                   filter_type initial_filter,
                                   endpoint::clock* clock,
                                   const domain_options* adaptation,
                                   connector_ptr conn)
  : self(self),
    id(this_peer),
    filter(std::make_shared<shared_filter_type>(std::move(initial_filter))),
    clock(clock),
    registry(checked(std::move(reg),
                     "cannot construct the core actor without registry")),
    metrics(*registry) {
  // Read config and check for extra configuration parameters.
  ttl = caf::get_or(self->config(), "broker.ttl", defaults::ttl);
  if (adaptation && adaptation->disable_forwarding) {
    log::core::info("disable-forwarding", "disable forwarding on this peer");
    disable_forwarding = true;
  } else {
    log::core::info("enable-forwarding",
                    "enable forwarding on this peer (default)");
  }
  // Callback setup when running with a connector attached.
  if (conn) {
    auto on_peering = [this](endpoint_id remote_id, const network_info& addr,
                             const filter_type& filter,
                             const pending_connection_ptr& conn) {
      std::ignore = init_new_peer(remote_id, addr, filter, conn);
    };
    auto on_peer_unavailable = [this](const network_info& addr) {
      peer_unavailable(addr);
    };
    adapter = std::make_unique<connector_adapter>(self, std::move(conn),
                                                  on_peering,
                                                  on_peer_unavailable, filter,
                                                  peer_statuses);
  }
}

core_actor_state::~core_actor_state() {
  log::core::debug("dtor", "core_actor_state destroyed");
}

// -- initialization and tear down ---------------------------------------------

caf::behavior core_actor_state::make_behavior() {
  // Override the default exit handler to add logging.
  self->set_exit_handler([this](caf::exit_msg& msg) {
    if (msg.reason) {
      log::core::debug(
        "exit-msg",
        "shutting down after receiving an exit message with reason {}",
        msg.reason);
      shutdown(shutdown_options{});
    }
  });
  // Create the behavior (set of message handlers / callbacks) for the actor.
  caf::behavior result{
    // -- peering --------------------------------------------------------------
    [this](atom::listen, const std::string& addr, uint16_t port,
           bool reuse_addr) {
      auto rp = self->make_response_promise();
      if (!adapter) {
        rp.deliver(caf::make_error(ec::no_connector_available));
      } else {
        adapter->async_listen(
          addr, port, reuse_addr,
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
    // -- unpeering ------------------------------------------------------------
    [this](atom::unpeer, const network_info& peer_addr) { //
      unpeer(peer_addr);
    },
    [this](atom::unpeer, endpoint_id peer_id) { //
      unpeer(peer_id);
    },
    // -- non-native clients, e.g., via WebSocket API --------------------------
    [this](atom::attach_client, const network_info& addr, std::string& type,
           filter_type& filter, data_consumer_res& in_res,
           data_producer_res& out_res) -> caf::result<void> {
      if (auto err = init_new_client(addr, type, filter, std::move(in_res),
                                     std::move(out_res)))
        return err;
      else
        return caf::unit;
    },
    // -- getters --------------------------------------------------------------
    [this](atom::get, atom::peer) {
      std::vector<peer_info> result;
      for (const auto& [peer_id, state] : peerings) {
        endpoint_info info{peer_id, state->addr};
        auto status = peer_statuses->get(peer_id);
        if (status != peer_status::unknown) {
          result.emplace_back(peer_info{.peer = std::move(info),
                                        .flags = peer_flags::remote,
                                        .status = status});
        }
      }
      return result;
    },
    [this](atom::get_filter) { return filter->read(); },
    // -- publishing of messages without going through a publisher -------------
    [this](atom::publish, const data_message& msg) {
      ++published_via_async_msg;
      dispatch_from(msg->with(id, endpoint_id::nil()), nullptr);
    },
    [this](atom::publish, const data_message& msg, const endpoint_info& dst) {
      ++published_via_async_msg;
      dispatch_from(msg->with(id, dst.node), nullptr);
    },
    [this](atom::publish, const data_message& msg, endpoint_id dst) {
      ++published_via_async_msg;
      dispatch_from(msg->with(id, dst), nullptr);
    },
    [this](atom::publish, atom::local, const data_message& msg) {
      ++published_via_async_msg;
      dispatch_from(msg->with(id, id), nullptr);
    },
    [this](atom::publish, const command_message& msg) { //
      dispatch(msg);
    },
    [this](atom::publish, const command_message& msg,
           const endpoint_info& dst) { //
      dispatch(msg->with(id, dst.node));
    },
    [this](atom::publish, const command_message& msg, endpoint_id dst) {
      dispatch(msg->with(id, dst));
    },
    // -- subscription management ----------------------------------------------
    [this](atom::subscribe, const filter_type& filter) {
      // Subscribe to topics without actually caring about the events. This
      // makes sure that this node receives events on the topics, which means
      // we can forward them.
      subscribe(filter);
    },
    // -- interface for hubs ---------------------------------------------------
    [this](hub_id id, filter_type& filter) {
      // Update the filter of an existing hub.
      auto i = hubs.find(id);
      if (i == hubs.end()) {
        log::core::error("update-hub-filter",
                         "cannot update filter of hub {}: not found",
                         static_cast<uint64_t>(id));
        return;
      }
      auto& ptr = i->second;
      handler_subscriptions.purge_value(ptr);
      for (auto& sub : filter) {
        handler_subscriptions.insert(sub.string(), ptr);
      }
      subscribe(filter);
    },
    [this](hub_id id, filter_type& filter, bool, data_consumer_res& src,
           data_producer_res& snk) {
      // Note: setting the filter_local flag to true means that we will not push
      //       messages from other hubs to this hub. This is used by the class
      //       `subscriber` to only receive messages from non-local sources,
      //       i.e., messages from other peers.
      if (id == hub_id::invalid) {
        log::core::error("add-hub", "cannot add hub with invalid ID");
        src.cancel();
        snk.close();
        return;
      }
      if (hubs.count(id) != 0) {
        log::core::error("add-hub", "cannot add hub with duplicate ID {}",
                         static_cast<uint64_t>(id));
        src.cancel();
        snk.close();
        return;
      }
      log::core::debug("add-hub", "add hub {}", static_cast<uint64_t>(id));
      subscribe(filter);
      auto ptr = std::make_shared<hub_handler>(this, hub_buffer_size(),
                                               hub_overflow_policy(), id);
      if (!src) {
        ptr->type(handler_type::subscriber);
        detail::fmt_to(std::back_inserter(ptr->pretty_name), "subscriber-{}",
                       static_cast<uint64_t>(id));
      } else if (!snk) {
        ptr->type(handler_type::publisher);
        detail::fmt_to(std::back_inserter(ptr->pretty_name), "publisher-{}",
                       static_cast<uint64_t>(id));
      } else {
        detail::fmt_to(std::back_inserter(ptr->pretty_name), "hub-{}",
                       static_cast<uint64_t>(id));
      }
      setup(ptr, std::move(src), std::move(snk), filter);
      hubs.emplace(id, ptr);
    },
    // -- data store management ------------------------------------------------
    [this](atom::data_store, atom::clone, atom::attach, const std::string& name,
           double resync_interval, double stale_interval,
           double mutation_buffer_interval) {
      return attach_clone(name, resync_interval, stale_interval,
                          mutation_buffer_interval);
    },
    [this](atom::data_store, atom::master, atom::attach,
           const std::string& name, backend backend_type,
           backend_options opts) {
      return attach_master(name, backend_type, std::move(opts));
    },
    [this](atom::data_store, atom::master, atom::get,
           const std::string& name) -> caf::result<caf::actor> {
      auto i = masters.find(name);
      if (i != masters.end())
        return i->second->hdl;
      else
        return caf::make_error(ec::no_such_master);
    },
    [this](atom::shutdown, atom::data_store) { //
      shutdown_stores();
    },
    // -- miscellaneous --------------------------------------------------------
    [this](atom::shutdown, shutdown_options opts) { //
      shutdown(opts);
    },
    [this](atom::await, endpoint_id peer_id) {
      auto rp = self->make_response_promise();
      if (auto i = peerings.find(peer_id); i != peerings.end())
        rp.deliver(peer_id);
      else
        awaited_peers.emplace(peer_id, rp);
      return rp;
    },
    [this](atom::get, atom::status) {
      if (masters.size() + clones.size() > 0) {
        auto worker = self->spawn<status_collector_actor>(masters, clones);
        self->delegate(worker, status_snapshot());
      } else {
        auto rp = self->make_response_promise();
        rp.deliver(status_snapshot());
      }
    },
  };
  if (adapter) {
    return adapter->message_handlers().or_else(result);
  } else {
    return result;
  }
}

void core_actor_state::shutdown(shutdown_options options) {
  if (shutting_down_)
    return;
  shutting_down_ = true;
  // Tell the connector to shut down. No new connections allowed.
  if (adapter)
    adapter->async_shutdown();
  // Shut down data stores.
  shutdown_stores();
  // Inform our clients that we are no longer waiting for any peer.
  log::core::debug("shutdown", "cancel {} pending await_peer requests",
                   awaited_peers.size());
  for (auto& kvp : awaited_peers)
    kvp.second.deliver(caf::make_error(ec::shutting_down));
  awaited_peers.clear();
  // Ignore future messages.
  self->become([](int) {
    // Dummy handler to keep the actor alive until we call quit
  });
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
  // Close all peers gracefully. By disposing their outputs, we send the BYE
  // message and wait for the "ack" (via one last pong message).
  if (peerings.empty()) {
    // Nothing to wait for, do the final steps right away.
    finalize_shutdown();
    return;
  }
  // Send BYE messages to all peers to trigger graceful connection shutdown.
  for (const auto& ptr : peering_handlers()) {
    if (send_bye_message(ptr)) {
      log::core::info("unpeer-id", "unpeering from peer {}", ptr->id);
    }
  }
  // Call `finalize_shutdown` after a timeout to ensure that we don't wait
  // forever if the peers don't respond to the BYE messages.
  shutting_down_timeout = self->run_delayed(defaults::unpeer_timeout,
                                            [this] { finalize_shutdown(); });
}

void core_actor_state::finalize_shutdown() {
  BROKER_ASSERT(shutting_down_);
  // Stop the timeout in case it's still pending.
  shutting_down_timeout.dispose();
  // Clear and dispose all containers.
  auto clear_and_dispose = []<class Container>(Container& container) {
    Container tmp;
    tmp.swap(container);
    for (auto& kvp : tmp) {
      kvp.second->dispose();
    }
  };
  clear_and_dispose(masters);
  clear_and_dispose(clones);
  clear_and_dispose(peerings);
  clear_and_dispose(clients);
  clear_and_dispose(hubs);
  // Close the shared state for all peers.
  peer_statuses->close();
  // Quit the actor, which will cause CAF to clean up the state.
  self->quit();
}

// -- convenience functions ----------------------------------------------------

template <class Info, class EnumConstant>
void core_actor_state::emit(Info&& ep, EnumConstant code, const char* msg) {
  // Pick the right topic and factory based on the event type.
  using value_type = typename EnumConstant::value_type;
  std::string str;
  if constexpr (std::is_same_v<value_type, sc>)
    str = topic::statuses_str;
  else
    str = topic::errors_str;
  using factory =
    std::conditional_t<std::is_same_v<value_type, sc>, status, error_factory>;
  // Generate a data message from the converted content and address it to this
  // node only. This ensures that the data remains visible locally only.
  auto val = factory::make(code, std::forward<Info>(ep), msg);
  try {
    auto content = get_as<data>(val);
    dispatch(make_data_message(id, id, std::move(str), std::move(content)));
  } catch (std::exception&) {
    std::cerr << "*** failed to convert " << caf::deep_to_string(val)
              << " to data\n";
  }
}

bool core_actor_state::has_remote_subscriber(const topic& x) const noexcept {
  auto is_subscribed = [&x](auto& kvp) {
    return kvp.second->is_subscribed_to(x);
  };
  return std::any_of(peerings.begin(), peerings.end(), is_subscribed);
}

std::optional<network_info> core_actor_state::addr_of(endpoint_id id) const {
  if (auto i = peerings.find(id); i != peerings.end())
    return i->second->addr;
  else
    return std::nullopt;
}

table core_actor_state::message_metrics_snapshot() const {
  table result;
  for (size_t msg_type = 1; msg_type < 6; ++msg_type) {
    auto key = static_cast<packed_message_type>(msg_type);
    auto& msg_metrics = metrics.metrics_for(key);
    table vals;
    vals.emplace("processed"s, msg_metrics.processed->Value());
    result.emplace(to_string(key), std::move(vals));
  }
  return result;
}

table core_actor_state::peer_stats_snapshot() const {
  table result;
  for (auto& [pid, state_ptr] : peerings) {
    table entry;
    result.emplace(to_string(pid), std::move(entry));
  }
  return result;
}

table core_actor_state::status_snapshot() const {
  auto env_or_default = [](const char* env_name,
                           const char* fallback) -> std::string {
    if (auto val = getenv(env_name))
      return val;
    return fallback;
  };
  table result;
  auto add = [&result](std::string key, auto value) {
    result.emplace(std::move(key), std::move(value));
  };
  add("id", to_string(id));
  add("cluster-node", env_or_default("CLUSTER_NODE", "unknown"));
  add("time", caf::timestamp_to_string(caf::make_timestamp()));
  add("native-connections", metrics.native_connections->Value());
  add("web-socket-connections", metrics.web_socket_connections->Value());
  add("message-metrics", message_metrics_snapshot());
  add("peerings", peer_stats_snapshot());
  add("published-via-async-msg", published_via_async_msg);
  return result;
}

// -- callbacks ----------------------------------------------------------------

void core_actor_state::cannot_remove_peer(endpoint_id peer_id) {
  emit(endpoint_info{peer_id}, ec_constant<ec::peer_invalid>(),
       "cannot unpeer from unknown peer");
  log::core::debug("cannot-remove-peer-id",
                   "cannot unpeer from unknown peer {}", peer_id);
}

void core_actor_state::cannot_remove_peer(const network_info& addr) {
  emit(endpoint_info{endpoint_id::nil(), addr}, ec_constant<ec::peer_invalid>(),
       "cannot unpeer from unknown peer");
  log::core::debug("cannot-remove-peer-addr",
                   "cannot unpeer from unknown peer {}", addr);
}

void core_actor_state::peer_unavailable(const network_info& addr) {
  emit(endpoint_info{endpoint_id::nil(), addr},
       ec_constant<ec::peer_unavailable>(), "unable to connect to remote peer");
  log::core::debug("peer-unavailable", "unable to connect to remote peer {}",
                   addr);
}

void core_actor_state::client_added(endpoint_id client_id,
                                    const network_info& addr,
                                    const std::string& type) {
  emit(endpoint_info{client_id, std::nullopt, type},
       sc_constant<sc::endpoint_discovered>(),
       "found a new client in the network");
  emit(endpoint_info{client_id, addr, type}, sc_constant<sc::peer_added>(),
       "handshake successful");
  log::core::debug("client-added", "added client {} of type {} with address {}",
                   client_id, type, addr);
}

void core_actor_state::client_removed(endpoint_id client_id,
                                      const network_info& addr,
                                      const std::string& type,
                                      const caf::error& reason, bool removed) {
  metrics.web_socket_connections->Decrement();
  if (removed) {
    auto msg = "client removed: " + to_string(reason);
    emit(endpoint_info{client_id, addr, type}, sc_constant<sc::peer_removed>(),
         msg.c_str());
  } else {
    emit(endpoint_info{client_id, addr, type}, sc_constant<sc::peer_lost>(),
         "lost connection to client");
  }
  emit(endpoint_info{client_id, std::nullopt, type},
       sc_constant<sc::endpoint_unreachable>(), "lost the last path");
  log::core::debug("client-removed",
                   "removed client {} of type {} with address {}", client_id,
                   type, addr);
}

void core_actor_state::on_peer_removed(const peering_handler_ptr& ptr) {
  log::core::info("on-peer-removed", "peer {} removed", ptr->id);
  emit(endpoint_info{ptr->id, ptr->addr}, sc_constant<sc::peer_removed>(),
       "removed connection to remote peer");
}

void core_actor_state::on_peer_lost(const peering_handler_ptr& ptr) {
  log::core::info("on-peer-lost", "peer {} disconnected", ptr->id);
  emit(endpoint_info{ptr->id, ptr->addr}, sc_constant<sc::peer_lost>(),
       "lost connection to remote peer");
  if (shutting_down_ || !ptr->addr.has_retry_time()) {
    // Never re-connect to a peer if we are already shutting down or if the peer
    // doesn't have a retry time.
    log::core::debug(
      "on-peer-lost",
      "not re-connecting to peer {}, shutting_down = {}, has_retry_time = {}",
      ptr->id, shutting_down_, ptr->addr.has_retry_time());
    return;
  }
  log::core::debug("on-peer-lost", "try to re-connect to peer {}, addr = {}",
                   ptr->id, ptr->addr);
  try_connect(ptr->addr, caf::response_promise{});
}

void core_actor_state::on_demand(const handler_ptr& peer, size_t demand) {
  peer->add_demand(demand);
}

void core_actor_state::on_data(const handler_ptr& ptr) {
  pull_buffer.clear();
  auto input_closed = ptr->pull(pull_buffer) == handler_result::term;
  for (auto& msg : pull_buffer) {
    dispatch_from(msg, ptr);
  }
  if (input_closed) {
    on_input_closed(ptr);
  }
}

void core_actor_state::on_data(const peering_handler_ptr& peer) {
  pull_buffer.clear();
  // Check if the peer got disconnected. If so, we will handle it later in order
  // to have the disconnect events be delivered after any messages that might
  // be in the buffer.
  auto disconnected = peer->pull(pull_buffer) == handler_result::term;
  for (auto& msg : pull_buffer) {
    log::core::debug("on-data", "received message from peer {}: {}", peer->id,
                     msg);
    dispatch_from(msg, peer);
    switch (get_type(msg)) {
      default:
        break;
      case packed_message_type::routing_update: {
        // Deserialize payload and update peer filter.
        filter_type new_filter;
        for (auto new_topic : *msg->as_routing_update()) {
          new_filter.emplace_back(std::string{new_topic});
        }
        log::core::debug("routing-update", "{} changed its filter to {}",
                         get_sender(msg), new_filter);
        handler_subscriptions.purge_value(peer);
        for (auto& sub : new_filter) {
          handler_subscriptions.insert(sub.string(), peer);
        }
        // else: ignore. Probably a stale message after unpeering.
        break;
      }
      case packed_message_type::ping: {
        // Respond to PING messages with a PONG that has the same payload.
        log::core::debug("ping",
                         "received a PING message with a payload of {} bytes",
                         msg->raw_bytes().second);
        auto ping = msg->as_ping();
        auto pong = make_pong_message(ping);
        msg_provider.set(pong->with(ping->receiver(), ping->sender()));
        offer_msg(peer);
        break;
      }
      case packed_message_type::pong: {
        // If we receive a PONG message with the byte token, we can close the
        // connection.
        auto raw_bytes = msg->raw_bytes();
        auto payload = std::span{raw_bytes.first, raw_bytes.second};
        auto i = peerings.find(peer->id);
        if (peer->unpeering && payload.size() == peering_handler::bye_token_size
            && i != peerings.end() && i->second == peer) {
          peering_handler::bye_token token;
          peer->assign_bye_token(token);
          if (std::equal(payload.begin(), payload.end(), token.begin(),
                         token.end())) {
            // Do not handle any further messages from this peer after
            // completing the BYE handshake.
            log::core::debug("pong", "{} completed the BYE handshake",
                             peer->id);
            peerings.erase(i);
            peer->dispose();
            on_handler_disposed(peer);
            return;
          }
          break;
        }
      }
    }
  }
  if (disconnected) {
    on_input_closed(peer);
  }
}

void core_actor_state::on_overflow_disconnect(const handler_ptr& ptr) {
  log::core::error("on-overflow-disconnect", "{} disconnected due to overflow",
                   ptr->pretty_name);
  erase(ptr);
  ptr->dispose();
  on_handler_disposed(ptr);
}

void core_actor_state::on_input_closed(const handler_ptr& ptr) {
  BROKER_ASSERT(ptr->input_closed());
  log::core::debug("on-input-closed", "{} closed its input buffer",
                   ptr->pretty_name);
  if (ptr->output_closed()) {
    erase(ptr);
    ptr->dispose();
    on_handler_disposed(ptr);
  }
}

void core_actor_state::on_output_closed(const handler_ptr& ptr) {
  BROKER_ASSERT(ptr->output_closed());
  log::core::debug("on-output-closed", "{} closed its output buffer",
                   ptr->pretty_name);
  if (ptr->input_closed()) {
    erase(ptr);
    ptr->dispose();
    on_handler_disposed(ptr);
  }
}

void core_actor_state::on_handler_disposed(const handler_ptr& ptr) {
  handler_subscriptions.purge_value(ptr);
  if (shutting_down_ && peerings.empty()) {
    finalize_shutdown();
  }
}

void core_actor_state::erase(const handler_ptr& what) {
  with_subtype(what, [this]<class Ptr>(const Ptr& ptr) {
    using element_type = typename Ptr::element_type;
    if constexpr (std::is_same_v<element_type, peering_handler>) {
      peerings.erase(ptr->id);
    } else if constexpr (std::is_same_v<element_type, hub_handler>) {
      hubs.erase(ptr->id);
    } else if constexpr (std::is_same_v<element_type, store_handler>) {
      if (ptr->type() == handler_type::master) {
        masters.erase(ptr->id);
      } else {
        BROKER_ASSERT(ptr->type() == handler_type::clone);
        clones.erase(ptr->id);
      }
    } else {
      static_assert(std::is_same_v<element_type, client_handler>);
      clients.erase(ptr->id);
    }
  });
}

// -- connection management ----------------------------------------------------

void core_actor_state::try_connect(const network_info& addr,
                                   caf::response_promise rp) {
  if (!adapter) {
    rp.deliver(caf::make_error(ec::no_connector_available));
    return;
  }
  adapter->async_connect(
    addr,
    [this, rp](endpoint_id peer, const network_info& addr,
               const filter_type& filter,
               const pending_connection_ptr& conn) mutable {
      log::core::debug(
        "try-connect-success",
        "connected to remote peer {} with initial filter {} at {}", peer,
        filter, addr);
      if (auto err = init_new_peer(peer, addr, filter, conn);
          err && err != ec::repeated_peering_handshake_request)
        rp.deliver(std::move(err));
      else
        rp.deliver(atom::peer_v, atom::ok_v, peer);
    },
    [this, rp, addr](endpoint_id peer,
                     const network_info& actual_addr) mutable {
      if (auto i = peerings.find(peer); i != peerings.end()) {
        log::core::debug("try-connect-redundant",
                         "dropped redundant connection to {}: tried connecting "
                         "to {}, but already connected prior via {}",
                         peer, addr, actual_addr);
        // Override the address if this one has a retry field. This makes
        // sure we "prefer" a user-defined address over addresses we read
        // from sockets for incoming peerings.
        if (actual_addr.has_retry_time() && !i->second->addr.has_retry_time())
          i->second->addr = actual_addr;
        rp.deliver(atom::peer_v, atom::ok_v, peer);
      } else {
        // Race on the state. May happen if the remote peer already
        // started a handshake and the connector thus drops this request
        // as redundant. We should already have the connect event queued,
        // so we just enqueue this request again and the second time
        // we should find it in the cache.
        using namespace std::literals;
        self->run_delayed(1ms, [this, peer, addr, actual_addr, rp]() mutable {
          if (auto i = peerings.find(peer); i != peerings.end()) {
            log::core::debug(
              "try-connect-redundant-delayed",
              "dropped redundant connection to {}: tried connecting "
              "to {}, but already connected prior via {}",
              peer, addr, actual_addr);
            if (actual_addr.has_retry_time()
                && !i->second->addr.has_retry_time())
              i->second->addr = actual_addr;
            rp.deliver(atom::peer_v, atom::ok_v, peer);
          } else {
            try_connect(actual_addr, rp);
          }
        });
      }
    },
    [this, rp, addr](const caf::error& what) mutable {
      log::core::debug("try-connect-failed", "failed to connect to {}: {}",
                       addr, what);
      rp.deliver(what);
      peer_unavailable(addr);
    });
}

// -- flow management ----------------------------------------------------------

caf::error core_actor_state::init_new_peer(endpoint_id peer_id,
                                           const network_info& addr,
                                           const filter_type& filter,
                                           chunk_consumer_res in_res,
                                           chunk_producer_res out_res) {
  if (shutting_down_) {
    log::core::debug("init-new-peer-shutdown",
                     "drop incoming peering: shutting down");
    return caf::make_error(ec::shutting_down);
  }
  // Sanity checking: make sure this isn't a repeated handshake.
  if (peerings.contains(peer_id)) {
    log::core::debug("init-new-peer-repeated",
                     "drop incoming peering: repeated handshake request");
    return caf::make_error(ec::repeated_peering_handshake_request);
  }
  // Set the status for this peer to 'peered'. The only legal transitions are
  // 'nil -> peered' and 'connected -> peered'.
  auto& psm = *peer_statuses;
  auto status = peer_status::peered;
  if (psm.insert(peer_id, status)) {
    log::core::debug("init-new-peer-nil", "{} changed state: () -> peered",
                     peer_id);
  } else if (status == peer_status::connected
             && psm.update(peer_id, status, peer_status::peered)) {
    log::core::debug("init-new-peer-connected",
                     "{} changed state: connected -> peered", peer_id);
  } else {
    log::core::error("init-new-peer-invalid", "{} reports invalid status {}",
                     peer_id, status);
    return caf::make_error(ec::invalid_status, to_string(status));
  }
  // All sanity checks have passed, update our state.
  metrics.native_connections->Increment();
  if (auto* lptr = logger()) {
    lptr->on_peer_connect(peer_id, addr);
  }
  // Create a handler for the peer.
  auto state =
    std::make_shared<peering_handler>(this, peer_id, peer_buffer_size(),
                                      peer_overflow_policy(), addr, filter);
  state->bye_id = self->new_u64_id();
  state->on_dispose = caf::make_type_erased_callback([this, state, peer_id]() {
    log::core::debug("on-peer-dispose", "dispose called for peer {}", peer_id);
    // Drop our 'local' state for this peer.
    if (peerings.count(peer_id) != 0) {
      log::core::error("on-peer-dispose",
                       "on_dispose called for peer {} that still exists",
                       peer_id);
    }
    if (auto* lptr = logger()) {
      lptr->on_peer_disconnect(peer_id, error{ec::no_path_to_peer});
    }
    // Update our 'global' state for this peer.
    auto status = peer_status::peered;
    if (peer_statuses->update(peer_id, status, peer_status::disconnected)) {
      if (state->unpeering) {
        on_peer_removed(state);
      } else {
        on_peer_lost(state);
      }
      emit(endpoint_info{peer_id}, sc_constant<sc::endpoint_unreachable>(),
           "lost the last path");
    } else {
      log::core::error("init-new-peer-invalid-disconnected",
                       "{} reports invalid status {}", peer_id, status);
      // TODO: maybe we should consider this a fatal error?
    }
  });
  detail::fmt_to(std::back_inserter(state->pretty_name), "peering-{}",
                 to_string(peer_id));
  if (auto i = awaited_peers.find(peer_id); i != awaited_peers.end()) {
    i->second.deliver(peer_id);
    awaited_peers.erase(i);
  }
  setup(state, std::move(in_res), std::move(out_res), filter);
  peerings.emplace(peer_id, state);
  // Emit status messages.
  auto make_status_msg = [peer_id]<class Info, sc S>(Info&& ep,
                                                     sc_constant<S> code,
                                                     const char* msg) {
    auto val = status::make(code, std::forward<Info>(ep), msg);
    auto content = get_as<data>(val);
    auto dmsg = make_data_message(peer_id, peer_id,
                                  topic{std::string{topic::statuses_str}},
                                  content);
    return node_message{std::move(dmsg)};
  };
  emit(endpoint_info{peer_id}, sc_constant<sc::endpoint_discovered>(),
       "found a new peer in the network");
  emit(endpoint_info{peer_id, addr}, sc_constant<sc::peer_added>(),
       "handshake successful");
  return {};
}

caf::error core_actor_state::init_new_peer(endpoint_id peer,
                                           const network_info& addr,
                                           const filter_type& filter,
                                           const pending_connection_ptr& ptr) {
  // Spin up a background worker that takes care of socket I/O. We communicate
  // to this worker via producer/consumer buffer resources. The [rd_1, wr_1]
  // pair is the direction core actor -> network. The other pair is for the
  // opposite direction. The caf::net::length_prefix_framing protocol is
  // simple framing protocol that suffixes a message with a four-byte 'header'
  // for the message size. So on the wire, every byte block that our trait
  // produces gets prefixed with 32 bit with the size.
  namespace cn = caf::net;
  // Note: structured bindings with values confuses clang-tidy's leak checker.
  auto resources1 = caf::async::make_spsc_buffer_resource<caf::chunk>();
  auto& [rd_1, wr_1] = resources1;
  auto resources2 = caf::async::make_spsc_buffer_resource<caf::chunk>();
  auto& [rd_2, wr_2] = resources2;
  if (auto err = ptr->run(self->system(), std::move(rd_1), std::move(wr_2))) {
    log::core::debug("init-new-peer-failed",
                     "failed to run pending connection: {}", err);
    return err;
  } else {
    // With the connected buffers, dispatch to the other overload.
    return init_new_peer(peer, addr, filter, std::move(rd_2), std::move(wr_1));
  }
}

caf::error core_actor_state::init_new_client(const network_info& addr,
                                             std::string& type,
                                             const filter_type& filter,
                                             data_consumer_res in_res,
                                             data_producer_res out_res) {
  // Fail early when shutting down.
  if (shutting_down_) {
    log::core::debug("init-new-client-shutdown",
                     "drop new client: shutting down");
    return caf::make_error(ec::shutting_down);
  }
  // Sanity checking.
  if (!in_res) {
    return caf::make_error(caf::sec::invalid_argument,
                           "cannot add client without valid input buffer");
  }
  // All sanity checks have passed, update our state.
  metrics.web_socket_connections->Increment();
  // Assign a UUID to each WebSocket client and treat it like a peer.
  auto client_id = endpoint_id::random();
  auto state = std::make_shared<client_handler>(this, web_socket_buffer_size(),
                                                web_socket_overflow_policy(),
                                                client_id);
  detail::fmt_to(std::back_inserter(state->pretty_name), "{}-{}", type,
                 addr.address);
  setup(state, std::move(in_res), std::move(out_res), filter);
  clients.emplace(client_id, state);
  // Emit status updates.
  if (auto* lptr = logger()) {
    lptr->on_client_connect(client_id, addr);
  }
  client_added(client_id, addr, type);
  state->on_dispose =
    caf::make_type_erased_callback([this, state, addr, type, client_id]() {
      log::core::debug("client-disconnected", "client {} disconnected", addr);
      auto reason = caf::make_error(caf::sec::connection_closed);
      if (auto* lptr = logger()) {
        lptr->on_client_disconnect(client_id, facade(reason));
      }
      client_removed(client_id, addr, type, reason, true);
    });
  return {};
}

// -- topic management ---------------------------------------------------------

void core_actor_state::subscribe(const filter_type& what) {
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
    log::core::debug("subscribe-added", "subscribed to new topics: {}", what);
    broadcast_subscriptions();
  } else {
    log::core::debug("subscribe-dropped", "already subscribed to topics: {}",
                     what);
  }
}

// -- data store management ----------------------------------------------------

bool core_actor_state::has_remote_master(const std::string& name) const {
  // A master would subscribe to its 'special topic', so we would see a
  // subscription for that topic if another node has a master attached. This
  // still has a big asterisk attached to it since the subscription message
  // may just be in flight. This check is better than nothing, though.
  return has_remote_subscriber(name / topic::master_suffix());
}

caf::result<caf::actor> core_actor_state::attach_master(const std::string& name,
                                                        backend backend_type,
                                                        backend_options opts) {
  // Sanity checking: master must not already exist locally or on a peer.
  if (auto i = masters.find(name); i != masters.end()) {
    log::core::debug("attach-master-redundant",
                     "master with name {} already exists", name);
    return i->second->hdl;
  }
  if (has_remote_master(name)) {
    log::core::warning("attach-master-failed",
                       "master with name {} already exists on a different node",
                       name);
    return caf::make_error(ec::master_exists);
  }
  // Create backend and buffers.
  auto ptr = detail::make_backend(backend_type, std::move(opts));
  if (!ptr)
    return caf::make_error(ec::backend_failure);
  log::core::info("attach-master-new", "spawning new master: {}", name);
  using caf::async::make_spsc_buffer_resource;
  // Note: structured bindings with values confuses clang-tidy's leak checker.
  auto resources1 = make_spsc_buffer_resource<command_message>();
  auto& [con1, prod1] = resources1;
  auto resources2 = make_spsc_buffer_resource<command_message>();
  auto& [con2, prod2] = resources2;
  // Spin up the master.
  auto hdl =
    self->system().spawn<master_actor_type>(registry, id, name, std::move(ptr),
                                            caf::actor{self}, clock,
                                            std::move(con1), std::move(prod2));
  // Subscribe to the master topic.
  filter_type filter{name / topic::master_suffix()};
  subscribe(filter);
  // Create a handler for the master.
  auto state = std::make_shared<store_handler>(this, store_buffer_size(),
                                               store_overflow_policy(), name,
                                               hdl, handler_type::master);
  detail::fmt_to(std::back_inserter(state->pretty_name), "master-{}", name);
  setup(state, std::move(con2), std::move(prod1), filter);
  state->on_dispose = caf::make_type_erased_callback([this, state] {
    log::core::debug("master-dispose", "dispose called for master {}",
                     state->id);
    // Note: the actor will terminate as a result of closing the buffers.
  });
  // Save the handle and monitor the new actor.
  masters.emplace(name, state);
  self->link_to(hdl);
  return hdl;
}

caf::result<caf::actor>
core_actor_state::attach_clone(const std::string& name, double resync_interval,
                               double stale_interval,
                               double mutation_buffer_interval) {
  // Sanity checking: make sure there is no master or clone already.
  if (auto i = masters.find(name); i != masters.end()) {
    log::core::warning(
      "attach-clone-failed",
      "attempted to run clone and master for {} on the same endpoint", name);
    return caf::make_error(ec::no_such_master);
  }
  if (auto i = clones.find(name); i != clones.end()) {
    log::core::debug("attach-clone-redundant",
                     "clone with name {} already exists", name);
    return i->second->hdl;
  }
  // Spin up the clone and connect it to our buffers.
  log::core::info("attach-clone-new", "spawning new clone: {}", name);
  using std::chrono::duration_cast;
  // TODO: make configurable.
  auto tout = duration_cast<timespan>(fractional_seconds{10});
  using caf::async::make_spsc_buffer_resource;
  // Note: structured bindings with values confuses clang-tidy's leak checker.
  auto resources1 = make_spsc_buffer_resource<command_message>();
  auto& [con1, prod1] = resources1;
  auto resources2 = make_spsc_buffer_resource<command_message>();
  auto& [con2, prod2] = resources2;
  auto hdl = self->system().spawn<clone_actor_type>(registry, id, name, tout,
                                                    caf::actor{self}, clock,
                                                    std::move(con1),
                                                    std::move(prod2));
  // Subscribe to the clone topic.
  filter_type filter{name / topic::clone_suffix()};
  subscribe(filter);
  // Create a handler for the clone.
  auto state = std::make_shared<store_handler>(this, store_buffer_size(),
                                               store_overflow_policy(), name,
                                               hdl, handler_type::clone);
  detail::fmt_to(std::back_inserter(state->pretty_name), "clone-{}", name);
  setup(state, std::move(con2), std::move(prod1), filter);
  state->on_dispose = caf::make_type_erased_callback([this, state] {
    log::core::debug("clone-dispose", "dispose called for clone {}", state->id);
    // Note: the actor will terminate as a result of closing the buffers.
  });
  // Save the handle and monitor the new actor.
  clones.emplace(name, state);
  self->link_to(hdl);
  return hdl;
}

void core_actor_state::shutdown_stores() {
  log::core::debug("shutdown-stores",
                   "shutting down data stores: {} masters, {} clones",
                   masters.size(), clones.size());
  auto clear_and_dispose = []<class Container>(Container& container) {
    Container tmp;
    tmp.swap(container);
    for (auto& kvp : tmp) {
      kvp.second->dispose();
    }
  };
  clear_and_dispose(masters);
  clear_and_dispose(clones);
}

// -- dispatching of messages to peers regardless of subscriptions  ------------

bool core_actor_state::offer_msg(const handler_ptr& ptr) {
  auto do_erase = [this, &ptr] {
    erase(ptr);
    ptr->dispose();
    on_handler_disposed(ptr);
  };
  switch (ptr->offer(msg_provider)) {
    case offer_result::ok:
      return true;
    case offer_result::overflow_disconnect:
      log::core::error("offer-msg", "{} disconnected due to overflow",
                       ptr->pretty_name);
      do_erase();
      break;
    case offer_result::term:
      log::core::error("offer-msg", "{} terminated while offering message",
                       ptr->pretty_name);
      do_erase();
      break;
    default: // skip
      break;
  }
  return false;
}

void core_actor_state::dispatch(const node_message& msg) {
  metrics.metrics_for(msg->type()).processed->Increment();
  msg_provider.set(msg);
  auto do_dispatch = [this, &msg](auto& selection) {
    auto accepted = size_t{0};
    for (auto& ptr : selection) {
      if (offer_msg(ptr)) {
        ++accepted;
      }
    }
    log::core::debug("dispatch",
                     "offered message to {} subscribers ({} accepted): {}",
                     selection.size(), accepted, msg);
  };
  // Use the reusable buffer if it's available (i.e., empty).
  if (selection_buffer.empty()) {
    handler_subscriptions.select(get_topic(msg), selection_buffer);
    auto buffer_guard = caf::detail::make_scope_guard([this]() noexcept {
      // Clear the buffer after dispatching all of its messages.
      selection_buffer.clear();
    });
    do_dispatch(selection_buffer);
    return;
  }
  // Otherwise, this is a nested dispatch: use a fresh buffer since
  // selection_buffer is already in use.
  std::vector<handler_ptr> selection;
  selection.reserve(64); // Avoid multiple small allocations.
  handler_subscriptions.select(get_topic(msg), selection);
  do_dispatch(selection);
}

void core_actor_state::dispatch_from(const node_message& msg,
                                     const handler_ptr& from) {
  metrics.metrics_for(msg->type()).processed->Increment();
  msg_provider.set(msg);
  auto do_dispatch = [this, &msg, &from](auto& selection) {
    auto get_pretty_name = [&from] {
      if (!from) {
        return "null"sv;
      }
      return std::string_view{from->pretty_name};
    };
    auto accepted = size_t{0};
    for (auto& ptr : selection) {
      if (ptr != from) {
        // If `from == nullptr`, it means that this message was sent via
        // endpoint::publish. We treat it as if it was published by a publisher,
        // i.e., it's a local message and subscribers should not receive it.
        if ((!from || from->type() == handler_type::publisher)
            && ptr->type() == handler_type::subscriber
            && get_receiver(msg) != id) {
          continue;
        }
        if (offer_msg(ptr)) {
          ++accepted;
        }
      }
    }
    log::core::debug("dispatch-from",
                     "offered message to {} subscribers ({} accepted): {}",
                     selection.size(), accepted, msg);
  };
  // Use the reusable buffer if it's available (i.e., empty).
  if (selection_buffer.empty()) {
    handler_subscriptions.select(get_topic(msg), selection_buffer);
    auto buffer_guard = caf::detail::make_scope_guard([this]() noexcept {
      // Clear the buffer after dispatching all of its messages.
      selection_buffer.clear();
    });
    do_dispatch(selection_buffer);
    return;
  }
  // Otherwise, this is a nested dispatch: use a fresh buffer since
  // selection_buffer is already in use.
  std::vector<handler_ptr> selection;
  selection.reserve(64); // Avoid multiple small allocations.
  handler_subscriptions.select(get_topic(msg), selection);
  do_dispatch(selection);
}

void core_actor_state::broadcast_subscriptions() {
  // Bypass subscriptions and forward the routing update directly to all peers.
  auto msg = routing_update_envelope::make(filter->read());
  msg_provider.set(msg->with(id, endpoint_id{}));
  log::core::debug("broadcast-subscriptions",
                   "broadcasting message to {} peers: {}", peerings.size(),
                   msg_provider.get());
  for (auto& [pid, ptr] : peerings) {
    offer_msg(ptr);
  }
}

bool core_actor_state::send_bye_message(const peering_handler_ptr& ptr) {
  if (!ptr->unpeering) {
    peering_handler::bye_token token;
    ptr->assign_bye_token(token);
    auto msg = make_ping_message(id, ptr->id, token.data(), token.size());
    msg_provider.set(std::move(msg));
    if (offer_msg(ptr)) {
      ptr->unpeering = true;
      return true;
    }
  }
  return false;
}

// -- unpeering ----------------------------------------------------------------

void core_actor_state::unpeer(endpoint_id peer_id) {
  if (auto i = peerings.find(peer_id); i != peerings.end()) {
    // Note: send_bye_message may modify `peerings`. Hence, we need to pass the
    // pointer by value.
    auto ptr = i->second;
    if (send_bye_message(ptr)) {
      log::core::info("unpeer-id", "unpeering from peer {}", peer_id);
      return;
    }
    log::core::debug("unpeer-id", "ignore repeated call to unpeer {}", peer_id);
    return;
  }
  cannot_remove_peer(peer_id);
}

void core_actor_state::unpeer(const network_info& addr) {
  auto pred = [&addr](auto& kvp) { return kvp.second->addr == addr; };
  if (auto i = std::find_if(peerings.begin(), peerings.end(), pred);
      i != peerings.end()) {
    // Note: send_bye_message may modify `peerings`. Hence, we need to pass the
    // pointer by value.
    auto ptr = i->second;
    if (send_bye_message(ptr)) {
      log::core::info("unpeer-addr", "unpeering from peer {}", addr);
      return;
    }
    log::core::debug("unpeer-addr", "ignore repeated call to unpeer {}", addr);
    return;
  }
  cannot_remove_peer(addr);
}

// -- properties ---------------------------------------------------------------

namespace {

overflow_policy overflow_policy_from_string(const std::string* str,
                                            overflow_policy fallback) {
  if (str != nullptr) {
    if (*str == "drop_newest") {
      return overflow_policy::drop_newest;
    }
    if (*str == "drop_oldest") {
      return overflow_policy::drop_oldest;
    }
    if (*str == "disconnect") {
      return overflow_policy::disconnect;
    }
  }
  return fallback;
}

} // namespace

size_t core_actor_state::store_buffer_size() {
  // TODO: make configurable?
  return defaults::peer_buffer_size;
}

overflow_policy core_actor_state::store_overflow_policy() {
  // TODO: make configurable?
  // Note: stores have a built-in mechanism for re-sending messages. Hence, we
  //       can drop messages in case of overflow.
  return overflow_policy::drop_newest;
}

size_t core_actor_state::peer_buffer_size() {
  return caf::get_or(self->config(), "broker.peer-buffer-size",
                     defaults::peer_buffer_size);
}

overflow_policy core_actor_state::peer_overflow_policy() {
  auto* str = caf::get_if<std::string>(&self->config(),
                                       "broker.peer-overflow-policy");
  return overflow_policy_from_string(str, defaults::peer_overflow_policy);
}

size_t core_actor_state::web_socket_buffer_size() {
  return caf::get_or(self->config(), "broker.web-socket-buffer-size",
                     defaults::web_socket_buffer_size);
}

overflow_policy core_actor_state::web_socket_overflow_policy() {
  auto* str = caf::get_if<std::string>(&self->config(),
                                       "broker.web-socket-overflow-policy");
  return overflow_policy_from_string(str, defaults::web_socket_overflow_policy);
}

size_t core_actor_state::hub_buffer_size() {
  // TODO: make configurable?
  return 1'000'000;
}

overflow_policy core_actor_state::hub_overflow_policy() {
  return overflow_policy::drop_oldest;
}

} // namespace broker::internal
