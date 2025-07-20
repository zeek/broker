#include "broker/internal/clone_actor.hh"

#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/detail/appliers.hh"
#include "broker/detail/assert.hh"
#include "broker/error.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"
#include "broker/store.hh"
#include "broker/topic.hh"

#include <caf/actor.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/behavior.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/make_message.hpp>
#include <caf/message.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>

#include <chrono>
#include <memory>

using namespace std::literals;

namespace broker::internal {

namespace {

double now(endpoint::clock* clock) {
  auto d = clock->now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

} // namespace

// -- initialization -----------------------------------------------------------

clone_state::clone_state(caf::event_based_actor* ptr,
                         prometheus_registry_ptr reg, endpoint_id this_endpoint,
                         std::string nm, caf::timespan master_timeout,
                         caf::actor parent, endpoint::clock* ep_clock,
                         caf::async::consumer_resource<command_message> in_res,
                         caf::async::producer_resource<command_message> out_res)
  : super(ptr), input(this), max_sync_interval(master_timeout) {
  super::init(std::move(reg), this_endpoint, ep_clock, std::move(nm),
              std::move(parent), std::move(in_res), std::move(out_res));
  master_topic = store_name / topic::master_suffix();
  super::init(input);
  max_get_delay = caf::get_or(ptr->config(), "broker.store.max-get-delay",
                              defaults::store::max_get_delay);
  log::store::info("attached", "attached clone {} to {}", id, store_name);
}

void clone_state::forward(internal_command&& x) {
  self->send(core, atom::publish_v,
             make_command_message(master_topic, std::move(x)));
}

void clone_state::dispatch(const command_message& msg) {
  log::store::debug("dispatch", "received command {}", msg);
  // Here, we receive all command messages from the stream. The first step is
  // figuring out whether the received message stems from a writer or master.
  auto& cmd = get_command(msg);
  auto seq = cmd.seq;
  auto tag = detail::tag_of(cmd);
  auto type = detail::type_of(cmd);
  if (input.initialized() && cmd.sender != input.producer()) {
    log::store::warning("unknown-sender",
                        "clone received message from unknown sender {}",
                        cmd.sender);
    return;
  }
  auto is_receiver = [this, &cmd] {
    if (cmd.receiver == id) {
      return true;
    } else {
      if (cmd.receiver) {
        log::store::debug("discard-wrong-receiver",
                          "clone received message for {}", cmd.receiver);
      } else {
        log::store::debug("discard-broadcast",
                          "received a broadcast command message");
      }
      return false;
    }
  };
  switch (tag) {
    case command_tag::action: {
      // Action messages from the master (broadcasted).
      input.handle_event(seq, msg);
      break;
    }
    case command_tag::producer_control: {
      // Control messages must be 'unicast'.
      if (!is_receiver())
        return;
      // Control messages from the master.
      switch (type) {
        case internal_command::type::ack_clone_command: {
          if (master_id) {
            if (cmd.sender == master_id) {
              log::store::debug("repeated-ack-clone",
                                "drop repeated ack_clone from {}", master_id);
            } else {
              log::store::error(
                "conflicting-ack-clone",
                "received ack_clone from {} but already attached to {}",
                cmd.sender, master_id);
            }
            return;
          } else {
            master_id = cmd.sender;
          }
          auto& inner = get<ack_clone_command>(cmd.content);
          if (input.handle_handshake(cmd.sender, inner.offset,
                                     inner.heartbeat_interval)) {
            log::store::debug("received-ack-clone",
                              "received ack_clone from {}", cmd.sender);
            if (!master_id)
              master_id = cmd.sender;
            set_store(inner.state);
            start_output();
          } else {
            log::store::debug("repeated-ack-clone",
                              "drop repeated ack_clone from {}", master_id);
          }
          break;
        }
        case internal_command::type::keepalive_command: {
          if (!input.initialized()) {
            log::store::debug("keepalive-ignored",
                              "ignored keepalive: input not initialized yet");
            break;
          }
          auto& inner = get<keepalive_command>(cmd.content);
          log::store::debug("keepalive",
                            "keepalive: input.next_seq = {}, "
                            "input.last_seq = {}, cmd.seq = {}",
                            input.next_seq(), input.last_seq(), inner.seq);
          input.handle_heartbeat(inner.seq);
          break;
        }
        case internal_command::type::retransmit_failed_command: {
          if (!input.initialized())
            break;
          auto& inner = get<retransmit_failed_command>(cmd.content);
          input.handle_retransmit_failed(inner.seq);
          break;
        }
        default: {
          log::store::error("unexpected-producer-control",
                            "received unexpected producer control message: {}",
                            cmd);
        }
      }
      break;
    }
    default: {
      BROKER_ASSERT(tag == command_tag::consumer_control);
      // Control messages must be 'unicast'.
      if (!is_receiver()) {
        log::store::debug(
          "consumer-control-wrong-receiver",
          "dropped consumer control message for different receiver {}",
          cmd.receiver);
        break;
      } else if (!output_opt) {
        log::store::debug("consumer-control-unknown-channel",
                          "received control message for a unknown channel");
        break;
      }
      // Control messages from the master for the writer.
      switch (type) {
        case internal_command::type::cumulative_ack_command: {
          auto& inner = get<cumulative_ack_command>(cmd.content);
          // We create the path with entity_id::nil(), but the channel still
          // cares about the handle. Hence, we need to set the actual handle
          // once once the master responds with an ACK.
          if (auto i = output_opt->find_path(entity_id::nil());
              i != output_opt->paths().end()) {
            if (master_id) {
              if (master_id == cmd.sender) {
                i->hdl = cmd.sender;
                log::store::debug("received-write-ack",
                                  "received write ACK from master {}",
                                  master_id);
              } else {
                log::store::debug(
                  "unexpected-write-ack",
                  "received write ACK from unexpected source {}", cmd.sender);
                return;
              }
            } else {
              log::store::debug("received-write-ack-and-set-master",
                                "received write ACK from new master {}",
                                master_id);
              master_id = cmd.sender;
            }
            i->hdl = cmd.sender;
          }
          output_opt->handle_ack(cmd.sender, inner.seq);
          break;
        }
        case internal_command::type::nack_command: {
          auto& inner = get<nack_command>(cmd.content);
          output_opt->handle_nack(cmd.sender, inner.seqs);
          break;
        }
        default: {
          log::store::error("unexpected-consumer-control",
                            "received bogus consumer control message: {}", cmd);
        }
      }
    }
  }
}

table clone_state::status_snapshot() const {
  table result;
  result.emplace("master-id"s, to_string(master_id.endpoint));
  result.emplace("input"s, get_stats(input));
  if (output_opt)
    result.emplace("output"s, get_stats(*output_opt));
  else
    result.emplace("output"s, nil);
  return result;
}

void clone_state::tick() {
  input.tick();
  if (output_opt)
    output_opt->tick();
}

// -- callbacks for the consumer -----------------------------------------------

void clone_state::consume(consumer_type*, command_message& msg) {
  auto f = [this](auto& cmd) { consume(cmd); };
  auto val = get_command(msg);
  std::visit(f, val.content);
}

void clone_state::consume(put_command& x) {
  log::store::debug("put-command",
                    "clone received put command (expiry {}): {} -> {}",
                    expiry_formatter{x.expiry}, x.key, x.value);
  if (auto i = store.find(x.key); i != store.end()) {
    auto& value = i->second;
    auto old_value = std::move(value);
    emit_update_event(x, old_value);
    value = std::move(x.value);
  } else {
    emit_insert_event(x);
    store.emplace(std::move(x.key), std::move(x.value));
  }
}

void clone_state::consume(put_unique_result_command& cmd) {
  log::store::debug("put-unique-result-command", "clone received: {}", cmd);
  local_request_key key{cmd.who, cmd.req_id};
  if (auto i = local_requests.find(key); i != local_requests.end()) {
    i->second.deliver(data{cmd.inserted}, cmd.req_id);
    local_requests.erase(i);
  }
}

void clone_state::consume(erase_command& x) {
  log::store::debug("erase-command", "clone received erase command for key {}",
                    x.key);
  if (store.erase(x.key) != 0)
    emit_erase_event(x.key, x.publisher);
}

void clone_state::consume(expire_command& x) {
  log::store::debug("expire-command",
                    "clone received expire command for key {}", x.key);
  if (store.erase(x.key) != 0)
    emit_expire_event(x.key, x.publisher);
}

void clone_state::consume(clear_command& x) {
  log::store::info("clear-command", "clone received clear command");
  for (auto& kvp : store)
    emit_erase_event(kvp.first, x.publisher);
  store.clear();
}

error clone_state::consume_nil(consumer_type* src) {
  log::store::error("out-of-sync",
                    "clone out of sync: lost message from the master!");
  // By returning an error, we cause the channel to abort and call `close`.
  return ec::broken_clone;
}

void clone_state::close(consumer_type* src, const error& reason) {
  log::store::info("close", "clone is closing: {}", reason);
  // TODO: send some 'bye, bye' message to enable the master to remove this
  //       clone early, rather than waiting for timeout, see:
  //       https://github.com/zeek/broker/issues/142
}

void clone_state::send(consumer_type* ptr, channel_type::cumulative_ack ack) {
  log::store::debug("ack",
                    "clone received ack with seq {} from "
                    "master {} for producer {}",
                    ack.seq, master_id, ptr->producer());
  BROKER_ASSERT(master_id);
  auto msg = make_command_message(
    master_topic, internal_command{.seq = 0,
                                   .sender = id,
                                   .receiver = master_id,
                                   .content = cumulative_ack_command{ack.seq}});
  self->send(core, atom::publish_v, std::move(msg), ptr->producer().endpoint);
}

void clone_state::send(consumer_type* ptr, channel_type::nack nack) {
  log::store::debug("nack",
                    "clone received nack from "
                    "master {} for producer {}",
                    master_id, ptr->producer());
  auto msg = make_command_message(master_topic,
                                  internal_command{.seq = 0,
                                                   .sender = id,
                                                   .receiver = master_id,
                                                   .content = nack_command{
                                                     std::move(nack.seqs)}});
  if (ptr->initialized()) {
    BROKER_ASSERT(master_id == ptr->producer());
    self->send(core, atom::publish_v, std::move(msg), master_id.endpoint);
  } else {
    self->send(core, atom::publish_v, std::move(msg));
  }
}

// -- callbacks for the producer -----------------------------------------------

void clone_state::send(producer_type* ptr, const entity_id& dst,
                       channel_type::event& what) {
  log::store::debug("send-event", "send event with seq {} and type {} to {}",
                    get_command(what.content).seq,
                    get_command(what.content).content.index(), dst);
  BROKER_ASSERT(dst == master_id);
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  if (get_command(what.content).receiver != dst) {
    // Technical debt: the event really should be internal_command_variant to
    // allow us to assemble the command_message here instead of altering it.
    auto val = get_command(what.content);
    val.receiver = dst;
    what.content =
      make_command_message(what.content->sender(), what.content->receiver(),
                           std::string{what.content->topic()}, std::move(val));
  }
  self->send(core, atom::publish_v, what.content);
}

void clone_state::send(producer_type* ptr, const entity_id&,
                       channel_type::handshake what) {
  log::store::debug("send-handshake",
                    "send attach_writer_command with offset {}", what.offset);
  auto msg = make_command_message(
    master_topic,
    internal_command{.seq = 0,
                     .sender = id,
                     .receiver = master_id,
                     .content = attach_writer_command{
                       .offset = what.offset,
                       .heartbeat_interval = what.heartbeat_interval}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::send(producer_type* ptr, const entity_id&,
                       channel_type::retransmit_failed what) {
  log::store::debug("send-retransmit-failed",
                    "send retransmit_failed with seq {}", what.seq);
  auto msg = make_command_message(
    master_topic,
    internal_command{.seq = 0,
                     .sender = id,
                     .receiver = master_id,
                     .content = retransmit_failed_command{what.seq}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::broadcast(producer_type* ptr, channel_type::heartbeat what) {
  // Re-send handshakes as well. Usually, the keepalive message also acts as
  // handshake. However, the master did not open the channel in this case. We
  // first need to create it by sending `attach_writer_command`. Everything
  // received before this attach message is going to be ignored by the master.
  for (auto& path : ptr->paths()) {
    if (path.acked == 0) {
      log::store::debug("re-send-handshake", "re-send handshake to {}",
                        path.hdl);
      send(ptr, path.hdl,
           channel_type::handshake{.offset = path.offset,
                                   .heartbeat_interval =
                                     ptr->heartbeat_interval()});
    }
  }
  log::store::debug("send-keepalive", "send keepalive to master {}", master_id);
  auto msg = make_command_message(
    master_topic, internal_command{.seq = 0,
                                   .sender = id,
                                   .receiver = entity_id::nil(),
                                   .content = keepalive_command{what.seq}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::broadcast(producer_type* ptr,
                            const channel_type::event& what) {
  log::store::debug("broadcast-event",
                    "broadcast event with seq {} and type {} to {}",
                    get_command(what.content).seq,
                    get_command(what.content).content.index(), dst);
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  self->send(core, atom::publish_v, what.content);
}

void clone_state::drop(producer_type*, const entity_id&, ec reason) {
  log::store::debug("drop", "drop producer with reason {}", reason);
  // TODO: see comment in close()
}

void clone_state::handshake_completed(producer_type*, const entity_id&) {
  log::store::debug("handshake-completed",
                    "completed producer handshake for store {}", store_name);
}

// -- properties ---------------------------------------------------------------

data clone_state::keys() const {
  set result;
  for (auto& kvp : store)
    result.emplace(kvp.first);
  return result;
}

void clone_state::set_store(std::unordered_map<data, data> x) {
  log::store::debug("set-store", "set store values with {} entries", x.size());
  // We consider the master the source of all updates.
  entity_id publisher = input.producer();
  // Short-circuit messages with an empty state.
  if (x.empty()) {
    if (!store.empty()) {
      clear_command cmd{publisher};
      consume(cmd);
    }
  } else if (store.empty()) {
    // Emit insert events.
    for (auto& [key, value] : x)
      emit_insert_event(key, value, std::nullopt, publisher);
  } else {
    // Emit erase and put events.
    std::vector<const data*> keys;
    keys.reserve(store.size());
    for (auto& kvp : store)
      keys.emplace_back(&kvp.first);
    auto is_erased = [&x](const data* key) { return x.count(*key) == 0; };
    auto p = std::remove_if(keys.begin(), keys.end(), is_erased);
    for (auto i = keys.begin(); i != p; ++i)
      emit_erase_event(**i, entity_id{});
    for (auto i = p; i != keys.end(); ++i) {
      const auto& value = x[**i];
      emit_update_event(**i, store[**i], value, std::nullopt, publisher);
    }
    // Emit insert events.
    auto is_new = [&keys](const data& key) {
      for (const auto key_ptr : keys)
        if (*key_ptr == key)
          return false;
      return true;
    };
    for (const auto& [key, value] : x)
      if (is_new(key))
        emit_insert_event(key, value, std::nullopt, publisher);
  }
  // Override local state.
  store = std::move(x);
  // Trigger any GET messages waiting for a reply.
  for (auto& callback : on_set_store_callbacks)
    callback();
  on_set_store_callbacks.clear();
}

bool clone_state::has_master() const noexcept {
  return input.initialized();
}

bool clone_state::idle() const noexcept {
  return input.idle() && (!output_opt || output_opt->idle());
}

// -- helper functions ---------------------------------------------------------

void clone_state::start_output() {
  if (output_opt) {
    log::store::warning("repeat-start-output",
                        "repeated calls to clone_state::start_output");
    return;
  }
  BROKER_ASSERT(master_id);
  log::store::debug("add-output-channel", "clone {} adds an output channel",
                    id);
  auto& out = output_opt.emplace(this);
  super::init(out);
  out.add(master_id);
  if (!stalled.empty()) {
    std::vector<internal_command_variant> buf;
    buf.swap(stalled);
    for (auto& content : buf)
      send_to_master(std::move(content));
    BROKER_ASSERT(stalled.empty());
  }
}

void clone_state::send_to_master(internal_command_variant&& content) {
  if (output_opt) {
    BROKER_ASSERT(master_id);
    log::store::debug("send-to-master", "send command of type {} to master",
                      content.index());
    auto& out = *output_opt;
    auto msg =
      make_command_message(master_topic,
                           internal_command{.seq = out.next_seq(),
                                            .sender = id,
                                            .receiver = master_id,
                                            .content = std::move(content)});
    out.produce(std::move(msg));
  } else {
    log::store::debug("buffer-to-master",
                      "buffer command of type {} for master", content.index());
    stalled.emplace_back(std::move(content));
  }
}

// -- clone actor --------------------------------------------------------------

caf::behavior clone_state::make_behavior() {
  // Setup.
  self->monitor(core);
  self->set_down_handler(
    [this](const caf::down_msg& msg) { on_down_msg(msg.source, msg.reason); });
  // Ask the master to add this clone.
  send(std::addressof(input), clone_state::channel_type::nack{{0}});
  // Schedule first tick and set a timeout for the attach operation.
  send_later(self, defaults::store::tick_interval,
             caf::make_message(atom::tick_v));
  if (max_sync_interval.count() > 0)
    sync_timeout = caf::make_timestamp() + max_sync_interval;
  return super::make_behavior(
    // --- local communication -------------------------------------------------
    [this](atom::local, internal_command_variant& content) {
      if (auto inner = get_if<put_unique_command>(&content)) {
        if (inner->who) {
          log::store::debug("received-put-unique",
                            "received put_unique: who = {}, req_id = {}",
                            inner->who, inner->req_id);
          local_request_key key{inner->who, inner->req_id};
          local_requests.emplace(key, self->make_response_promise());
        } else {
          log::store::error("invalid-put-unique",
                            "received put_unique with invalid sender: DROP!");
          auto rp = self->make_response_promise();
          rp.deliver(caf::make_error(caf::sec::invalid_argument,
                                     "put_unique: invalid sender information"),
                     inner->req_id);
          return;
        }
      }
      send_to_master(std::move(content));
    },
    [this](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point_v);
    },
    [this](atom::tick) {
      tick();
      if (sync_timeout) {
        if (has_master()) {
          sync_timeout.reset();
        } else if (caf::make_timestamp() >= *sync_timeout) {
          log::store::error("no-master", "unable to find a master for",
                            store_name);
          auto err = caf::make_error(ec::no_such_master, store_name);
          for (auto& rp : idle_callbacks)
            rp.deliver(err);
          idle_callbacks.clear();
          self->quit(err);
          return;
        }
      }
      send_later(self, defaults::store::tick_interval,
                 caf::make_message(atom::tick_v));
      if (!idle_callbacks.empty() && idle()) {
        for (auto& rp : idle_callbacks)
          rp.deliver(atom::ok_v);
        idle_callbacks.clear();
      }
    },
    [this](atom::get, atom::keys) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp]() mutable { rp.deliver(keys()); });
      return rp;
    },
    [this](atom::get, atom::keys, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp, id]() mutable { rp.deliver(keys(), id); }, id);
      return rp;
    },
    [this](atom::exists, data& key) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp, key{std::move(key)}]() mutable {
        auto result = this->store.count(key) != 0;
        rp.deliver(data{result});
      });
      return rp;
    },
    [this](atom::exists, data& key, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(
        rp,
        [this, rp, key{std::move(key)}, id]() mutable {
          auto result = this->store.count(key) != 0;
          rp.deliver(data{result}, id);
        },
        id);
      return rp;
    },
    [this](atom::get, data& key) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp, key{std::move(key)}]() mutable {
        if (rp.pending()) {
          if (auto i = this->store.find(key); i != this->store.end()) {
            rp.deliver(i->second);
          } else {
            rp.deliver(caf::make_error(ec::no_such_key));
          }
        }
      });
      return rp;
    },
    [this](atom::get, data& key, data& aspect) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp, key{std::move(key)},
                    aspect{std::move(aspect)}]() mutable {
        if (auto i = this->store.find(key); i != this->store.end()) {
          if (auto res = visit(detail::retriever{aspect}, i->second))
            rp.deliver(std::move(*res));
          else
            rp.deliver(native(res.error()));
        } else {
          rp.deliver(caf::make_error(ec::no_such_key));
        }
      });
      return rp;
    },
    [this](atom::get, data& key, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(
        rp,
        [this, rp, key{std::move(key)}, id]() mutable {
          if (auto i = this->store.find(key); i != this->store.end()) {
            rp.deliver(i->second, id);
          } else {
            rp.deliver(caf::make_error(ec::no_such_key), id);
          }
        },
        id);
      return rp;
    },
    [this](atom::get, data& key, data& aspect, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(
        rp,
        [this, rp, key{std::move(key)}, asp{std::move(aspect)}, id]() mutable {
          if (auto i = this->store.find(key); i != this->store.end()) {
            auto x = visit(detail::retriever{asp}, i->second);
            if (x)
              rp.deliver(std::move(*x), id);
            else
              rp.deliver(std::move(native(x.error())), id);
          } else {
            rp.deliver(caf::make_error(ec::no_such_key), id);
          }
        },
        id);
      return rp;
    },
    [this](atom::get, atom::name) { return store_name; },
    [this](atom::await, atom::idle) -> caf::result<atom::ok> {
      if (idle())
        return atom::ok_v;
      auto rp = self->make_response_promise();
      idle_callbacks.emplace_back(std::move(rp));
      return caf::delegated<atom::ok>();
    });
}

} // namespace broker::internal
