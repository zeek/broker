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
#include "broker/filter_type.hh"
#include "broker/hub_id.hh"
#include "broker/internal/checked.hh"
#include "broker/internal/clone_actor.hh"
#include "broker/internal/killswitch.hh"
#include "broker/internal/master_actor.hh"
#include "broker/internal/wire_format.hh"
#include "broker/logger.hh"

using namespace std::literals;

namespace broker::internal {

// -- private utilities --------------------------------------------------------

namespace {

struct status_collector_state {
  using map_t = std::unordered_map<std::string, caf::actor>;

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
          self->request(entry.second, max_delay, atom::get_v, atom::status_v)
            .then([this, key = entry.first](table& res) {
              on_master_response(key, res);
            });
        for (auto& entry : store_clones)
          self->request(entry.second, max_delay, atom::get_v, atom::status_v)
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

// -- constructors and destructors ---------------------------------------------

core_actor_state::metrics_t::metrics_t(prometheus::Registry& reg) {
  metric_factory factory{reg};
  // Initialize connection metrics.
  auto [native, ws] = factory.core.connections_instances();
  native_connections = native;
  web_socket_connections = ws;
  // Initialize message metrics, indexes are according to packed_message_type.
  auto proc = factory.core.processed_messages_instances();
  message_metric_sets[1].assign(proc.data);
  message_metric_sets[2].assign(proc.command);
  message_metric_sets[3].assign(proc.routing_update);
  message_metric_sets[4].assign(proc.ping);
  message_metric_sets[5].assign(proc.pong);
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
    metrics(*registry),
    hub_inputs(self),
    unsafe_inputs(self),
    flow_inputs(self) {
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
  // Create the central "bus" where everything flows through.
  hub_merge = hub_inputs.as_observable().merge().share();
  central_merge = flow_inputs.as_observable()
                    .merge()
                    .merge(hub_merge.map([](const hub_input& msg) {
                      return node_message{msg.second};
                    }))
                    .share();
  // Process control messages and add instrumentation for metrics.
  central_merge //
    .for_each([this](const node_message& msg) {
      auto sender = get_sender(msg);
      // Update metrics.
      auto& metrics = metrics_for(get_type(msg));
      metrics.processed->Increment();
      // Ignore our own outputs.
      if (is_local(msg))
        return;
      // Dispatch on the type of the message.
      switch (get_type(msg)) {
        default:
          break;
        case packed_message_type::routing_update:
          // Deserialize payload and update peer filter.
          if (auto i = peers.find(sender); i != peers.end()) {
            filter_type new_filter;
            for (auto new_topic : *msg->as_routing_update()) {
              new_filter.emplace_back(std::string{new_topic});
            }
            log::core::debug("routing-update", "{} changed its filter to {}",
                             sender, new_filter);
            i->second->filter(std::move(new_filter));
          }
          // else: ignore. Probably a stale message after unpeering.
          break;
        case packed_message_type::ping: {
          // Respond to PING messages with a PONG that has the same payload.
          log::core::debug("ping",
                           "received a PING message with a payload of {} bytes",
                           msg->raw_bytes().second);
          dispatch(make_pong_message(msg->as_ping()));
          break;
        }
      }
    });
  // Initialize data_outputs and command_outputs.
  data_outputs =
    central_merge
      // Drop everything but data messages and only process messages that are
      // not meant for another peer.
      .filter([this](const node_message& msg) {
        // Note: local subscribers do not receive messages from local
        // publishers. Except when the message explicitly says otherwise by
        // setting receiver == id. This is the case for messages that were
        // published via `(atom::publish, atom::local, ...)` message.
        auto receiver = get_receiver(msg);
        return get_type(msg) == packed_message_type::data
               && (!is_local(msg) || receiver == id)
               && (!receiver || receiver == id);
      })
      // Convert to data_message.
      .map([this](const node_message& msg) { return msg->as_data(); })
      // Convert this blueprint to a *hot* observable.
      .share();
  command_outputs =
    central_merge
      // Drop everything but command messages and only process messages that
      // are not meant for another peer.
      .filter([this](const node_message& msg) {
        auto receiver = get_receiver(msg);
        return get_type(msg) == packed_message_type::command
               && (!receiver || receiver == id);
      })
      // Deserialize payload and wrap it into an actual command message.
      .map([this](const node_message& msg) { return msg->as_command(); })
      // Convert this blueprint to a *hot* observable.
      .share();

  // Connect data messages from the unsafe inputs to the hub merge point.
  hub_inputs.push(unsafe_inputs.as_observable()
                    .filter([](const node_message& msg) {
                      return get_type(msg) == packed_message_type::data;
                    })
                    .map([](const node_message& msg) -> hub_input {
                      return {hub_id::invalid, msg->as_data()};
                    })
                    .as_observable());
  // Connect other messages from the unsafe inputs to the central merge point.
  flow_inputs.push(unsafe_inputs.as_observable()
                     .filter([](const node_message& msg) {
                       return get_type(msg) != packed_message_type::data;
                     })
                     .as_observable());
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
  // Override the down handler to drop legacy subscribers.
  self->set_down_handler([this](caf::down_msg& msg) {
    if (auto i = legacy_subs.find(msg.source); i != legacy_subs.end()) {
      i->second.sub.dispose();
      legacy_subs.erase(i);
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
    [this](atom::attach_client, const network_info& addr,
           const std::string& type, filter_type& filter,
           data_consumer_res& in_res,
           data_producer_res& out_res) -> caf::result<void> {
      if (auto err = init_new_client(addr, type, std::move(filter),
                                     std::move(in_res), std::move(out_res)))
        return err;
      else
        return caf::unit;
    },
    // -- getters --------------------------------------------------------------
    [this](atom::get, atom::peer) {
      std::vector<peer_info> result;
      for (const auto& [peer_id, state] : peers) {
        endpoint_info info{peer_id, state->addr()};
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
      dispatch(msg);
    },
    [this](atom::publish, const data_message& msg, const endpoint_info& dst) {
      ++published_via_async_msg;
      dispatch(msg->with(id, dst.node));
    },
    [this](atom::publish, const data_message& msg, endpoint_id dst) {
      ++published_via_async_msg;
      dispatch(msg->with(id, dst));
    },
    [this](atom::publish, atom::local, const data_message& msg) {
      ++published_via_async_msg;
      dispatch(msg->with(id, id));
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
      // makes sure that this node receives events on the topics, which in means
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
      if (ptr->filter != filter) {
        log::core::debug("update-hub-filter", "update filter of hub {} to {}",
                         static_cast<uint64_t>(id), filter);
        ptr->filter = std::move(filter);
        subscribe(ptr->filter);
      }
    },
    [this](hub_id id, filter_type& filter, bool filter_local,
           data_consumer_res& src, data_producer_res& snk) {
      // Note: setting the filter_local flag to true means that we will not push
      //       messages from other hubs to this hub. This is used by the class
      //       `subscriber` to only receive messages from non-local messages,
      //       i.e., messages from other peers.
      if (id == hub_id::invalid) {
        log::core::error("add-hub", "cannot add hub with invalid ID");
        src.cancel();
        snk.close();
        return;
      }
      log::core::debug("add-hub", "add hub {}", static_cast<uint64_t>(id));
      if (hubs.count(id) != 0) {
        src.cancel();
        snk.close();
        return;
      }
      subscribe(filter);
      auto ptr = std::make_shared<hub_state>();
      ptr->filter = std::move(filter);
      if (src) {
        // Connect the messages from the hub to the merge point.
        log::core::debug("connect-hub-source", "add source for hub {}",
                         static_cast<uint64_t>(id));
        // For `publisher` sources, we drop the actual ID and tag the message as
        // "local" (so a `subscriber` won't receive it) by using
        // `hub_id::invalid`.
        auto care_of_id = filter_local ? hub_id::invalid : id;
        hub_inputs.push(src.observe_on(self)
                          .compose(inject_killswitch_t{std::addressof(ptr->in)})
                          .do_finally([this, id] { drop_hub_input(id); })
                          .map([care_of_id](const data_message& msg) {
                            return hub_input{care_of_id, msg};
                          })
                          .as_observable());
      }
      if (snk) {
        log::core::debug("connect-hub-sink", "add sink for hub {}",
                         static_cast<uint64_t>(id));
        caf::flow::observable<data_envelope_ptr> local;
        local = hub_merge
                  .filter([filter_local](const hub_input& msg) {
                    // If filter_local is set, we filter out messages from
                    // `publisher` objects and from messages that are published
                    // via `endpoint::publish`. Those messages will have an
                    // invalid hub ID.
                    return !filter_local || msg.first != hub_id::invalid;
                  })
                  .filter([id, ptr](const hub_input& msg) {
                    detail::prefix_matcher f;
                    return msg.first != id && f(ptr->filter, msg.second);
                  })
                  .map([](const hub_input& msg) { return msg.second; })
                  .as_observable();
        auto non_local = data_outputs.filter([ptr](const data_message& msg) {
          detail::prefix_matcher f;
          return f(ptr->filter, msg);
        });
        // Connect the output buffer.
        ptr->out = local.merge(std::move(non_local))
                     .do_finally([this, id] { drop_hub_output(id); })
                     .subscribe(snk);
      }
      hubs.emplace(id, std::move(ptr));
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
        return i->second;
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
      if (auto i = peers.find(peer_id); i != peers.end())
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

void core_actor_state::drop_hub_input(hub_id id) {
  // Callback that does the actual work.
  auto do_drop = [this, id] {
    auto i = hubs.find(id);
    if (i == hubs.end()) {
      return;
    }
    if (auto& ptr = i->second) {
      ptr->in = nullptr; // already disposed
      if (ptr->out) {
        return; // keep the hub registered until the output is disposed as well
      }
    }
    // Remove the hub if either 1) `ptr` is null or 2) both input and output
    // are disposed.
    log::core::debug("remove-hub", "removing hub {}",
                     static_cast<uint64_t>(id));
    hubs.erase(i);
  };
  // Check if the hub is registered. If it is, we schedule a delayed call to
  // drop the input. This is necessary since we might be in the middle of a
  // processing chain that may modify the hub state.
  if (hubs.count(id) != 0) {
    self->delay_fn(do_drop);
  }
}

void core_actor_state::drop_hub_output(hub_id id) {
  // Callback that does the actual work.
  auto do_drop = [this, id] {
    auto i = hubs.find(id);
    if (i == hubs.end()) {
      return;
    }
    if (auto& ptr = i->second) {
      ptr->out = nullptr; // already disposed
      if (ptr->in) {
        return; // keep the hub registered until the input is disposed as well
      }
    }
    // Remove the hub if either 1) `ptr` is null or 2) both input and output
    // are disposed.
    log::core::debug("remove-hub", "removing hub {}",
                     static_cast<uint64_t>(id));
    hubs.erase(i);
  };
  // Same as `drop_hub_input`: avoid modifying the hub state while processing
  // messages.
  if (hubs.count(id) != 0) {
    self->delay_fn(do_drop);
  }
}

void core_actor_state::shutdown(shutdown_options options) {
  if (shutting_down())
    return;
  // Tell the connector to shut down. No new connection allowed.
  if (adapter)
    adapter->async_shutdown();
  // Shut down data stores.
  shutdown_stores();
  // We no longer add new input flows.
  flow_inputs.close();
  // Cancel subscriptions from clients (WebSocket clients).
  for (auto& [id, subs] : subscriptions) {
    for (auto& sub : subs) {
      sub.dispose();
    }
  }
  subscriptions.clear();
  // Inform our clients that we no longer wait for any peer.
  log::core::debug("shutdown", "cancel {} pending await_peer requests",
                   awaited_peers.size());
  for (auto& kvp : awaited_peers)
    kvp.second.deliver(caf::make_error(ec::shutting_down));
  awaited_peers.clear();
  // Ignore future messages. Calling unbecome() removes our 'behavior' (set of
  // message handlers). An actor without behavior runs as long as still has
  // active flows. Once the last flow terminates, the actor goes down.
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
  // Close all peers gracefully. By disposing their outputs, we send the BYE
  // message and wait for the "ack" (via one last pong message).
  if (peers.empty()) {
    // Nothing to wait for, do the final steps right away.
    finalize_shutdown();
    return;
  }
  for (auto& kvp : peers)
    kvp.second->remove(self, unsafe_inputs, false);
  shutting_down_timeout = self->run_delayed(defaults::unpeer_timeout,
                                            [this] { finalize_shutdown(); });
}

void core_actor_state::finalize_shutdown() {
  // Drop any remaining state of peers.
  for (auto& kvp : peers)
    kvp.second->force_disconnect("shutting down");
  peers.clear();
  // Close the shared state for all peers.
  peer_statuses->close();
  // Close all inputs.
  hub_inputs.close();
  unsafe_inputs.close();
  // Drop inputs from hubs.
  for (auto& kvp : hubs) {
    if (auto& ptr = kvp.second) {
      ptr->in.dispose();
      // Note: don't call `ptr->out.dispose();` since that might truncate
      // messages. We want to deliver our remaining messages to the subscribers.
      // We have closed all inputs, so the hubs will naturally receive an
      // `on_complete` event.
    }
  }
  hubs.clear();
  // After this point, any remaining flow should stop and the actor terminate.
}

// -- convenience functions ----------------------------------------------------

template <class Info, class EnumConstant>
void core_actor_state::emit(Info&& ep, EnumConstant code, const char* msg) {
  // Sanity checking.
  if (!data_outputs)
    return;
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
  return std::any_of(peers.begin(), peers.end(), is_subscribed);
}

std::optional<network_info> core_actor_state::addr_of(endpoint_id id) const {
  if (auto i = peers.find(id); i != peers.end())
    return i->second->addr();
  else
    return std::nullopt;
}

std::vector<endpoint_id> core_actor_state::peer_ids() const {
  std::vector<endpoint_id> result;
  for (auto& kvp : peers)
    result.emplace_back(kvp.first);
  return result;
}

table core_actor_state::message_metrics_snapshot() const {
  table result;
  for (size_t msg_type = 1; msg_type < 6; ++msg_type) {
    auto& msg_metrics = metrics.message_metric_sets[msg_type];
    table vals;
    vals.emplace("processed"s, msg_metrics.processed->Value());
    auto key = static_cast<packed_message_type>(msg_type);
    result.emplace(to_string(key), std::move(vals));
  }
  return result;
}

namespace {

table to_vals(const flow_scope_stats& stats) {
  table vals;
  vals.emplace("requested"s, stats.requested);
  vals.emplace("delivered"s, stats.delivered);
  return vals;
}

} // namespace

table core_actor_state::peer_stats_snapshot() const {
  table result;
  for (auto& [pid, state_ptr] : peers) {
    table entry;
    entry.emplace("input", to_vals(*state_ptr->input_stats()));
    entry.emplace("output", to_vals(*state_ptr->output_stats()));
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
  auto i = subscriptions.find(client_id);
  if (i == subscriptions.end()) {
    return;
  }
  disposable_list subs;
  i->second.swap(subs);
  subscriptions.erase(i);
  for (auto& sub : subs) {
    sub.dispose();
  }
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
      if (auto i = peers.find(peer); i != peers.end()) {
        log::core::debug("try-connect-redundant",
                         "dropped redundant connection to {}: tried connecting "
                         "to {}, but already connected prior via {}",
                         peer, addr, actual_addr);
        // Override the address if this one has a retry field. This makes
        // sure we "prefer" a user-defined address over addresses we read
        // from sockets for incoming peerings.
        if (actual_addr.has_retry_time() && !i->second->addr().has_retry_time())
          i->second->addr(actual_addr);
        rp.deliver(atom::peer_v, atom::ok_v, peer);
      } else {
        // Race on the state. May happen if the remote peer already
        // started a handshake and the connector thus drops this request
        // as redundant. We should already have the connect event queued,
        // so we just enqueue this request again and the second time
        // we should find it in the cache.
        using namespace std::literals;
        self->run_delayed(1ms, [this, peer, addr, actual_addr, rp]() mutable {
          if (auto i = peers.find(peer); i != peers.end()) {
            log::core::debug(
              "try-connect-redundant-delayed",
              "dropped redundant connection to {}: tried connecting "
              "to {}, but already connected prior via {}",
              peer, addr, actual_addr);
            if (actual_addr.has_retry_time()
                && !i->second->addr().has_retry_time())
              i->second->addr(actual_addr);
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

template <class T>
caf::error
core_actor_state::do_init_new_peer(endpoint_id peer_id,
                                   const network_info& addr,
                                   const filter_type& filter,
                                   caf::async::consumer_resource<T> in_res,
                                   caf::async::producer_resource<T> out_res) {
  if (shutting_down()) {
    log::core::debug("init-new-peer-shutdown",
                     "drop incoming peering: shutting down");
    return caf::make_error(ec::shutting_down);
  }
  // Sanity checking: make sure this isn't a repeated handshake.
  auto i = peers.find(peer_id);
  if (i != peers.end()) {
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
  // Hook into the central merge point for forwarding the data to the peer.
  caf::flow::observable<node_message> in;
  if constexpr (std::is_same_v<T, caf::chunk>) {
    in = self->make_observable()
           .from_resource(std::move(in_res))
           .map([](const caf::chunk& chunk) -> node_message {
             wire_format::v1::trait trait;
             node_message msg;
             if (trait.convert(chunk.bytes(), msg)) {
               return msg;
             }
             return nullptr;
           })
           .filter([](const node_message& msg) { return msg != nullptr; })
           .as_observable();
  } else {
    in =
      self->make_observable().from_resource(std::move(in_res)).as_observable();
  }
  auto filter_ptr = std::make_shared<filter_type>(filter);
  auto ptr = std::make_shared<peering>(addr, filter_ptr, id, peer_id);
  auto [new_in, new_out] = ptr->setup(
    self, std::move(in),
    central_merge
      // Select by subscription and sender/receiver fields.
      .filter([this, pid = peer_id, filter_ptr](const node_message& msg) {
        if (get_sender(msg) == pid)
          return false;
        if (disable_forwarding && !is_local(msg))
          return false;
        auto f = detail::prefix_matcher{};
        auto receiver = get_receiver(msg);
        return receiver == pid || (!receiver && f(*filter_ptr, get_topic(msg)));
      })
      // Override the sender field. This makes sure the sender field always
      // reflects the last hop. Since we only need this information to avoid
      // forwarding loops, "sender" really just means "last hop" in the current
      // implementation.
      .map([this](const node_message& msg) {
        if (get_sender(msg) == id)
          return msg;
        return msg->with(id, msg->receiver());
      })
      .do_on_next([this, peer_id](const node_message& msg) {
        // Record messages that are pushed into the backpressure buffer.
        if (auto* lptr = logger()) {
          lptr->on_peer_buffer_push(peer_id, msg);
        }
      })
      // Handle unresponsive peers.
      .on_backpressure_buffer(peer_buffer_size(), peer_overflow_policy())
      .do_on_next([this, peer_id](const node_message& msg) {
        // Record messages that leave the backpressure buffer.
        if (auto* lptr = logger()) {
          lptr->on_peer_buffer_pull(peer_id, msg);
        }
      })
      .do_on_complete([this, peer_id] {
        if (auto* lptr = logger()) {
          lptr->on_peer_disconnect(peer_id, error{});
        }
      })
      .do_on_error([this, ptr, peer_id](const caf::error& what) {
        log::core::debug("remove-peer", "remove peer {} due to: {}", peer_id,
                         what);
        if (auto* lptr = logger()) {
          lptr->on_peer_disconnect(peer_id, facade(what));
        }
        ptr->force_disconnect(to_string(what));
      })
      .as_observable());
  if constexpr (std::is_same_v<T, caf::chunk>) {
    new_out
      .map([buf = caf::byte_buffer{}](const node_message& msg) mutable {
        buf.clear();
        wire_format::v1::trait trait;
        if (trait.convert(msg, buf)) {
          return caf::chunk{buf};
        }
        return caf::chunk{};
      })
      .filter([](const caf::chunk& chunk) { return !chunk.empty(); })
      .subscribe(std::move(out_res));
  } else {
    new_out.subscribe(std::move(out_res));
  }
  // Push messages received from the peer into the central merge point.
  flow_inputs.push( //
    new_in
      // Handle peer disconnect events.
      .do_on_complete([this, peer_id, ptr]() mutable {
        if (!ptr)
          return;
        // Update our 'global' state for this peer.
        auto status = peer_status::peered;
        if (peer_statuses->update(peer_id, status, peer_status::disconnected)) {
          log::core::debug("init-new-peer-disconnected",
                           "{} changed state: peered -> disconnected", peer_id);
        } else {
          log::core::error("init-new-peer-invalid-disconnected",
                           "{} reports invalid status {}", peer_id, status);
          // TODO: maybe we should consider this a fatal error?
        }
        // Clean up state our local state.
        peers.erase(peer_id);
        // Trigger a reconnect if we have initiated the peering and did not
        // disconnect this peer as a result of unpeering from it.
        if (!ptr->removed() && !ptr->addr().address.empty()
            && ptr->addr().has_retry_time())
          try_connect(ptr->addr(), caf::response_promise{});
        // Shutting down? Finalize shutdown after removing the last peer.
        if (shutting_down() && peers.empty()) {
          shutting_down_timeout.dispose();
          finalize_shutdown();
        }
        ptr = nullptr;
      })
      .as_observable());
  peers.emplace(peer_id, ptr);
  // Notify clients that wait for this peering.
  if (auto [first, last] = awaited_peers.equal_range(peer_id); first != last) {
    for (auto i = first; i != last; ++i)
      i->second.deliver(peer_id);
    awaited_peers.erase(first, last);
  }
  return caf::none;
}

caf::error core_actor_state::init_new_peer(endpoint_id peer_id,
                                           const network_info& addr,
                                           const filter_type& filter,
                                           chunk_consumer_res in_res,
                                           chunk_producer_res out_res) {
  return do_init_new_peer(peer_id, addr, filter, std::move(in_res),
                          std::move(out_res));
}

caf::error core_actor_state::init_new_peer(endpoint_id peer_id,
                                           const network_info& addr,
                                           const filter_type& filter,
                                           node_consumer_res in_res,
                                           node_producer_res out_res) {
  return do_init_new_peer(peer_id, addr, filter, std::move(in_res),
                          std::move(out_res));
}

caf::error core_actor_state::init_new_peer(endpoint_id peer,
                                           const network_info& addr,
                                           const filter_type& filter,
                                           const pending_connection_ptr& ptr) {
  // Spin up a background worker that takes care of socket I/O. We communicate
  // to this worker via producer/consumer buffer resources. The [rd_1, wr_1]
  // pair is the direction core actor -> network. The other pair is for the
  // opposite direction. The caf::net::length_prefix_framing protocol is simple
  // framing protocol that suffixes a message with a four-byte 'header' for the
  // message size. So on the wire, every byte block that our trait produces gets
  // prefixed with 32 bit with the size.
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
                                             const std::string& type,
                                             filter_type filter,
                                             data_consumer_res in_res,
                                             data_producer_res out_res) {
  // Fail early when shutting down.
  if (shutting_down()) {
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
  // We cannot simply treat a client like we treat a local publisher or
  // subscriber, because events from the client must be visible locally. Hence,
  // we assign a UUID to each client and treat it almost like a peer.
  auto client_id = endpoint_id::random();
  if (auto* lptr = logger()) {
    lptr->on_client_connect(client_id, addr);
  }
  // Emit status updates.
  client_added(client_id, addr, type);
  // Hook into the central merge point for forwarding the data to the client.
  if (out_res) {
    auto sub =
      central_merge
        // Select by subscription.
        .filter(
          [this, filt = std::move(filter), client_id](const node_message& msg) {
            if (get_type(msg) != packed_message_type::data
                || get_sender(msg) == client_id)
              return false;
            detail::prefix_matcher f;
            return f(filt, get_topic(msg));
          })
        // Deserialize payload and wrap it into a data message.
        .map([this](const node_message& msg) { //
          return msg->as_data();
        })
        .do_on_next([this, client_id](const data_message& msg) {
          // Record messages that are pushed into the backpressure buffer.
          if (auto* lptr = logger()) {
            lptr->on_client_buffer_push(client_id, msg);
          }
        })
        // Handle unresponsive clients.
        .on_backpressure_buffer(web_socket_buffer_size(),
                                web_socket_overflow_policy())
        .do_on_next([this, client_id](const data_message& msg) {
          // Record messages that leave the backpressure buffer.
          if (auto* lptr = logger()) {
            lptr->on_client_buffer_pull(client_id, msg);
          }
        })
        .do_on_error([this, client_id, addr, type](const caf::error& reason) {
          log::core::debug("client-disconnected", "client {} disconnected",
                           addr);
          if (auto* lptr = logger()) {
            lptr->on_client_disconnect(client_id, facade(reason));
          }
          client_removed(client_id, addr, type, reason, true);
        })
        // Emit values to the producer resource.
        .subscribe(std::move(out_res));
    subscriptions[client_id].emplace_back(sub);
  }
  // Push messages received from the client into the central merge point.
  auto [in, ks] =
    self->make_observable()
      .from_resource(std::move(in_res))
      // If the client closes this buffer, we assume a disconnect.
      .do_on_complete([this, client_id, addr, type] {
        log::core::debug("client-disconnected",
                         "client {} of type {} at {} disconnected", client_id,
                         type, addr);
        client_removed(client_id, addr, type, caf::error{}, false);
      })
      .do_on_error([this, client_id, addr, type](const caf::error& reason) {
        log::core::debug("client-disconnected",
                         "client {} of type {} at {} disconnected", client_id,
                         type, addr);
        client_removed(client_id, addr, type, reason, false);
      })
      .map([this, client_id](const data_message& msg) {
        node_message result;
        if (msg->sender() == client_id)
          result = msg;
        else
          result = msg->with(client_id, msg->receiver());
        return result;
      })
      // Ignore any errors from the client.
      .on_error_complete()
      .compose(add_killswitch_t{});
  flow_inputs.push(in);
  subscriptions[client_id].emplace_back(ks);
  return caf::none;
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
  // Sanity checking: master must not already exist locally or on a peer.
  if (auto i = masters.find(name); i != masters.end()) {
    log::core::debug("attach-master-redundant",
                     "master with name {} already exists", name);
    return i->second;
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
  // Spin up the master and connect it to our flows.
  auto hdl =
    self->system().spawn<master_actor_type>(registry, id, name, std::move(ptr),
                                            caf::actor{self}, clock,
                                            std::move(con1), std::move(prod2));
  filter_type filter{name / topic::master_suffix()};
  subscribe(filter);
  command_outputs
    .filter([xs = filter](const command_message& item) {
      detail::prefix_matcher f;
      return f(xs, item);
    })
    .subscribe(prod1);
  auto in = self
              ->make_observable() //
              .from_resource(con2)
              .map([this](const command_message& msg) { //
                return node_message{msg};
              })
              .as_observable();
  flow_inputs.push(in);
  // Save the handle and monitor the new actor.
  masters.emplace(name, hdl);
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
    return i->second;
  }
  // Spin up the clone and connect it to our flows.
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
  filter_type filter{name / topic::clone_suffix()};
  subscribe(filter);
  command_outputs
    .filter([xs = filter](const command_message& item) {
      detail::prefix_matcher f;
      return f(xs, item);
    })
    .subscribe(prod1);
  auto in = self
              ->make_observable() //
              .from_resource(con2)
              .map([this](const command_message& msg) { //
                return node_message{msg};
              })
              .as_observable();
  flow_inputs.push(in);
  // Save the handle for later.
  clones.emplace(name, hdl);
  return hdl;
}

void core_actor_state::shutdown_stores() {
  log::core::debug("shutdown-stores",
                   "shutting down data stores: {} masters, {} clones",
                   masters.size(), clones.size());
  // TODO: consider re-implementing graceful shutdown of the store actors
  for (auto& kvp : masters)
    self->send_exit(kvp.second, caf::exit_reason::kill);
  masters.clear();
  for (auto& kvp : clones)
    self->send_exit(kvp.second, caf::exit_reason::kill);
  clones.clear();
}

// -- dispatching of messages to peers regardless of subscriptions ------------

void core_actor_state::dispatch(const node_message& msg) {
  unsafe_inputs.push(msg);
}

void core_actor_state::broadcast_subscriptions() {
  auto msg = routing_update_envelope::make(filter->read());
  for (auto& kvp : peers)
    dispatch(msg->with(id, kvp.first));
}

// -- unpeering ----------------------------------------------------------------

void core_actor_state::unpeer(endpoint_id peer_id) {
  log::core::debug("unpeer-id", "unpeering from peer {}", peer_id);
  if (auto i = peers.find(peer_id); i != peers.end())
    i->second->remove(self, unsafe_inputs);
  else
    cannot_remove_peer(peer_id);
}

void core_actor_state::unpeer(const network_info& addr) {
  log::core::debug("unpeer-addr", "unpeering from peer {}", addr);
  auto pred = [&addr](auto& kvp) { return kvp.second->addr() == addr; };
  if (auto i = std::find_if(peers.begin(), peers.end(), pred); i != peers.end())
    i->second->remove(self, unsafe_inputs);
  else
    cannot_remove_peer(addr);
}

bool core_actor_state::shutting_down() {
  // We call unbecome() in shutdown, which remove the behavior of the actor.
  // Hence, a core actor without behavior indicates shutdown has been called.
  return !self->has_behavior();
}

// -- properties ---------------------------------------------------------------

namespace {

caf::flow::backpressure_overflow_strategy
overflow_policy_from_string(const std::string* str, overflow_policy fallback) {
  using caf::flow::backpressure_overflow_strategy;
  if (str != nullptr) {
    if (*str == "drop_newest") {
      return backpressure_overflow_strategy::drop_newest;
    }
    if (*str == "drop_oldest") {
      return backpressure_overflow_strategy::drop_oldest;
    }
    if (*str == "disconnect") {
      return backpressure_overflow_strategy::fail;
    }
  }
  // Note: overflow_policy and backpressure_overflow_strategy have the same
  //       values. Hence, casting one to the other is safe.
  return static_cast<backpressure_overflow_strategy>(fallback);
}

} // namespace

size_t core_actor_state::peer_buffer_size() {
  return caf::get_or(self->config(), "broker.peer-buffer-size",
                     defaults::peer_buffer_size);
}

caf::flow::backpressure_overflow_strategy
core_actor_state::peer_overflow_policy() {
  auto* str = caf::get_if<std::string>(&self->config(),
                                       "broker.peer-overflow-policy");
  return overflow_policy_from_string(str, defaults::peer_overflow_policy);
}

size_t core_actor_state::web_socket_buffer_size() {
  return caf::get_or(self->config(), "broker.web-socket-buffer-size",
                     defaults::web_socket_buffer_size);
}

caf::flow::backpressure_overflow_strategy
core_actor_state::web_socket_overflow_policy() {
  auto* str = caf::get_if<std::string>(&self->config(),
                                       "broker.web-socket-overflow-policy");
  return overflow_policy_from_string(str, defaults::web_socket_overflow_policy);
}

} // namespace broker::internal
