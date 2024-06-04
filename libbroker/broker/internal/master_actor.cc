#include "broker/internal/logger.hh" // Needs to come before CAF includes.

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/make_message.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>

#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/detail/abstract_backend.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/die.hh"
#include "broker/internal/checked.hh"
#include "broker/internal/master_actor.hh"
#include "broker/internal/metric_factory.hh"
#include "broker/store.hh"
#include "broker/time.hh"
#include "broker/topic.hh"

using namespace std::literals;

namespace broker::internal {

namespace {

std::optional<timestamp> to_opt_timestamp(timestamp ts,
                                          std::optional<timespan> span) {
  return span ? ts + *span : std::optional<timestamp>();
}

template <class T>
auto to_caf_res(expected<T>&& x) {
  if (x)
    return caf::result<T>{std::move(*x)};
  else
    return caf::result<T>{std::move(native(x.error()))};
}

} // namespace

// -- metrics ------------------------------------------------------------------

master_state::metrics_t::metrics_t(prometheus::Registry& reg,
                                   const std::string& name) noexcept {
  metric_factory factory{reg};
  entries = factory.store.entries_instance(name);
}

// -- initialization -----------------------------------------------------------

master_state::master_state(
  caf::event_based_actor* ptr, prometheus_registry_ptr reg,
  endpoint_id this_endpoint, std::string nm, backend_pointer bp,
  caf::actor parent, endpoint::clock* ep_clock,
  caf::async::consumer_resource<command_message> in_res,
  caf::async::producer_resource<command_message> out_res)
  : super(ptr),
    output(this),
    metrics(checked_deref(reg,
                          "cannot construct a master actor without registry"),
            nm) {
  super::init(std::move(reg), this_endpoint, ep_clock, std::move(nm),
              std::move(parent), std::move(in_res), std::move(out_res));
  super::init(output);
  clones_topic = store_name / topic::clone_suffix();
  backend = std::move(bp);
  if (auto es = backend->expiries()) {
    for (auto& [key, expire_time] : *es)
      expirations.emplace(key, expire_time);
  } else {
    detail::die("failed to get master expiries while initializing");
  }
  if (auto entries = backend->size(); entries && *entries > 0) {
    metrics.entries->Set(*entries);
  }
  log::store::info("attached", "attached master {} to {}", id, store_name);
}

void master_state::dispatch(const command_message& msg) {
  log::store::debug("dispatch", "received command {}", msg);
  // Here, we receive all command messages from the stream. The first step is
  // figuring out whether the received message stems from a writer or clone.
  // Clones can only send control messages (they are always consumers). Writers
  // can send us either actions or control messages (they are producers).
  auto& cmd = get_command(msg);
  auto seq = cmd.seq;
  auto tag = detail::tag_of(cmd);
  auto type = detail::type_of(cmd);
  switch (tag) {
    case command_tag::action: {
      // Action messages from writers.
      if (auto i = inputs.find(cmd.sender); i != inputs.end())
        i->second.handle_event(seq, msg);
      else
        log::store::debug("unknown-sender-action",
                          "master received action from unknown sender {}",
                          cmd.sender);
      break;
    }
    case command_tag::producer_control: {
      // Control messages from writers.
      if (auto i = inputs.find(cmd.sender); i != inputs.end()) {
        switch (type) {
          case internal_command::type::attach_writer_command: {
            log::store::debug("repeated-attach-writer",
                              "master ignores repeated handshake from {}",
                              cmd.sender);
            break;
          }
          case internal_command::type::keepalive_command: {
            auto& inner = get<keepalive_command>(cmd.content);
            i->second.handle_heartbeat(inner.seq);
            break;
          }
          case internal_command::type::retransmit_failed_command: {
            auto& inner = get<retransmit_failed_command>(cmd.content);
            i->second.handle_retransmit_failed(inner.seq);
            break;
          }
          default: {
            log::store::error(
              "bogus-producer-control",
              "master received bogus producer control message {}", cmd);
          }
        }
      } else if (type == internal_command::type::attach_writer_command) {
        log::store::debug("attach-writer", "master attaches new writer: {}",
                          cmd.sender);
        auto& inner = get<attach_writer_command>(cmd.content);
        i = inputs.emplace(cmd.sender, this).first;
        super::init(i->second);
        i->second.producer(cmd.sender);
        if (!i->second.handle_handshake(inner.offset,
                                        inner.heartbeat_interval)) {
          log::store::error(
            "store-handshake-error",
            "master aborts connection: handle_handshake returned false");
          inputs.erase(i);
        }
      } else {
        log::store::debug(
          "unknown-sender-producer-control",
          "master received producer control message from unknown sender {}",
          cmd.sender);
      }
      break;
    }
    default: {
      BROKER_ASSERT(tag == command_tag::consumer_control);
      // Control messages from clones.
      switch (type) {
        case internal_command::type::cumulative_ack_command: {
          auto& inner = get<cumulative_ack_command>(cmd.content);
          output.handle_ack(cmd.sender, inner.seq);
          break;
        }
        case internal_command::type::nack_command: {
          auto& inner = get<nack_command>(cmd.content);
          output.handle_nack(cmd.sender, inner.seqs);
          break;
        }
        default: {
          log::store::debug(
            "unknown-sender-consumer-control",
            "master received consumer control message from unknown sender {}",
            cmd.sender);
        }
      }
    }
  }
}

table master_state::status_snapshot() const {
  auto inputs_stats = [this] {
    table result;
    for (auto& [key, in] : inputs)
      result.emplace(to_string(key.endpoint), get_stats(in));
    return result;
  };
  table result;
  if (auto val = backend->size())
    result.emplace("entries"s, *val);
  result.emplace("inputs"s, inputs_stats());
  result.emplace("output"s, get_stats(output));
  return result;
}

void master_state::tick() {
  output.tick();
  for (auto& kvp : inputs)
    kvp.second.tick();
  auto t = clock->now();
  for (auto i = expirations.begin(); i != expirations.end();) {
    if (t > i->second) {
      const auto& key = i->first;
      if (auto result = backend->expire(key, t); !result) {
        log::store::error("expire-error", "failed to expire key {}: {}", key,
                          result.error());

      } else if (!*result) {
        log::store::warning("expire-stale-key", "tried to expire stale key {}",
                            key);
      } else {
        log::store::info("expire", "expired key {}", key);
        expire_command cmd{key, id};
        emit_expire_event(cmd);
        broadcast(std::move(cmd));
        metrics.entries->Decrement();
      }
      i = expirations.erase(i);
    } else {
      ++i;
    }
  }
}

void master_state::set_expire_time(const data& key,
                                   const std::optional<timespan>& expiry) {
  if (expiry)
    expirations.insert_or_assign(key, clock->now() + *expiry);
  else
    expirations.erase(key);
}

// -- callbacks for the consumer -----------------------------------------------

void master_state::consume(consumer_type*, command_message& msg) {
  auto f = [this](auto& cmd) { consume(cmd); };
  auto val = msg->value();
  std::visit(f, val.content);
}

void master_state::consume(put_command& x) {
  log::store::debug("put-command",
                    "master received put command (expiry {}): {} -> {}",
                    expiry_formatter{x.expiry}, x.key, x.value);
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto old_value = backend->get(x.key);
  auto result = backend->put(x.key, x.value, et);
  if (!result) {
    log::store::error("put-command-failed", "failed to write to key {}: {}",
                      x.key, result.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  set_expire_time(x.key, x.expiry);
  if (old_value) {
    emit_update_event(x, *old_value);
  } else {
    emit_insert_event(x);
    metrics.entries->Increment();
  }
  broadcast(std::move(x));
}

void master_state::consume(put_unique_command& x) {
  log::store::debug("put-unique-command",
                    "master received put unique command (expiry {}): {} -> {}",
                    expiry_formatter{x.expiry}, x.key, x.value);
  auto broadcast_result = [this, &x](bool inserted) {
    broadcast(put_unique_result_command{inserted, x.who, x.req_id, id});
    if (x.who) {
      local_request_key key{x.who, x.req_id};
      if (auto i = local_requests.find(key); i != local_requests.end()) {
        i->second.deliver(data{inserted}, x.req_id);
        local_requests.erase(i);
      }
    }
  };
  if (exists(x.key)) {
    broadcast_result(false);
    return;
  }
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  if (auto res = backend->put(x.key, x.value, et); !res) {
    log::store::error("put-unique-command-failed",
                      "failed to write to key {}: {}", x.key, res.error());
    broadcast_result(false);
    return;
  }
  set_expire_time(x.key, x.expiry);
  emit_insert_event(x);
  metrics.entries->Increment();
  // Broadcast a regular "put" command (clones don't have to do their own
  // existence check) followed by the (positive) result message.
  broadcast(
    put_command{std::move(x.key), std::move(x.value), x.expiry, x.publisher});
  broadcast_result(true);
}

void master_state::consume(erase_command& x) {
  log::store::debug("erase-command", "master received erase command for key {}",
                    x.key);
  if (!exists(x.key)) {
    log::store::debug("erase-command-no-such-key",
                      "master failed to erase key {}: no such key", x.key);
    return;
  }
  if (auto res = backend->erase(x.key); !res) {
    log::store::error("erase-command-failed",
                      "master failed to erase key {}: {}", x.key, res.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  emit_erase_event(x.key, x.publisher);
  metrics.entries->Decrement();
  broadcast(std::move(x));
}

void master_state::consume(add_command& x) {
  log::store::debug("add-command",
                    "master received add command (expiry {}): {} -> {}",
                    expiry_formatter{x.expiry}, x.key, x.value);
  auto old_value = backend->get(x.key);
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  if (auto res = backend->add(x.key, x.value, x.init_type, et); !res) {
    log::store::error("add-command-failed",
                      "master failed to add {} to key {}: {}", x.value, x.key,
                      res.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (auto val = backend->get(x.key); !val) {
    log::store::error("add-then-read-failed",
                      "master failed to read new value for key {}: {}", x.key,
                      val.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  } else {
    set_expire_time(x.key, x.expiry);
    // Broadcast a regular "put" command. Clones don't have to repeat the same
    // processing again.
    put_command cmd{std::move(x.key), std::move(*val), std::nullopt,
                    x.publisher};
    if (old_value) {
      emit_update_event(cmd, *old_value);
    } else {
      emit_insert_event(cmd);
      metrics.entries->Increment();
    }
    broadcast(std::move(cmd));
  }
}

void master_state::consume(subtract_command& x) {
  log::store::debug("subtract-command",
                    "master received subtract command (expiry {}): {} -> {}",
                    expiry_formatter{x.expiry}, x.key, x.value);
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto old_value = backend->get(x.key);
  if (!old_value) {
    // Unlike `add`, `subtract` fails if the key didn't exist previously.
    log::store::warning("subtract-command-invalid-key",
                        "master failed to subtract {} from key {}: no such key",
                        x.value, x.key, old_value.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (auto res = backend->subtract(x.key, x.value, et); !res) {
    log::store::error("subtract-command-failed",
                      "master failed to subtract {} from key {}: {}", x.value,
                      x.key, res.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (auto val = backend->get(x.key); !val) {
    log::store::error("subtract-then-read-failed",
                      "master failed to read new value for key {}: {}", x.key,
                      val.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  } else {
    set_expire_time(x.key, x.expiry);
    // Broadcast a regular "put" command. Clones don't have to repeat the same
    // processing again.
    put_command cmd{std::move(x.key), std::move(*val), std::nullopt,
                    x.publisher};
    emit_update_event(cmd, *old_value);
    broadcast(std::move(cmd));
  }
}

void master_state::consume(clear_command& x) {
  log::store::info("clear-command", "master received clear command");
  if (auto keys_res = backend->keys(); !keys_res) {
    log::store::error("clear-command-no-key-res",
                      "master failed to retrieve keys for clear command: {}",
                      keys_res.error());
    return;
  } else {
    if (auto keys = get_if<vector>(*keys_res)) {
      for (auto& key : *keys)
        emit_erase_event(key, x.publisher);
      metrics.entries->Set(0);
    } else if (auto keys = get_if<set>(*keys_res)) {
      for (auto& key : *keys)
        emit_erase_event(key, x.publisher);
      metrics.entries->Set(0);
    } else if (!is<none>(*keys_res)) {
      log::store::error("clear-command-invalid-keys",
                        "master failed to retrieve keys for clear "
                        "command: unexpected result type");
    }
  }
  if (auto res = backend->clear(); !res) {
    log::store::critical("clear-command-failed",
                         "master failed to clear the table: {}", res.error());
    detail::die("failed to clear master");
  }
  broadcast(x);
}

error master_state::consume_nil(consumer_type* src) {
  // We lost a message from a writer. This is obviously bad, since we lost some
  // information before it made it into the backend. However, it is not a fatal
  // error in the sense that we must abort processing. Hence, we return `none`
  // here to keep processing messages from the writer.
  log::store::error("lost-consumer-message", "lost a message from {}",
                    src->producer());
  return {};
}

void master_state::close(consumer_type* src, const error& reason) {
  if (auto i = inputs.find(src->producer()); i != inputs.end()) {
    if (reason)
      log::store::info("close-consumer-with-error",
                       "removed producer {} due to an error: {}",
                       src->producer(), reason);
    else
      log::store::info("close-consumer",
                       "removed producer {} after graceful shutdown",
                       src->producer());
    inputs.erase(i);
    return;
  }
  log::store::error("close-consumer-unknown",
                    "received close request from unknown producer {}",
                    src->producer());
}

void master_state::send(consumer_type* ptr, channel_type::cumulative_ack ack) {
  auto dst = ptr->producer();
  log::store::debug("send-cumulative-ack",
                    "send cumulative ack with seq {} to {}", ack.seq, dst);
  auto msg = make_command_message(
    clones_topic,
    internal_command{0, id, dst, cumulative_ack_command{ack.seq}});
  self->send(core, atom::publish_v, std::move(msg), dst.endpoint);
}

void master_state::send(consumer_type* ptr, channel_type::nack nack) {
  auto dst = ptr->producer();
  log::store::debug("send-nack", "send nack to {}", dst);
  auto msg = make_command_message(
    clones_topic,
    internal_command{0, id, dst, nack_command{std::move(nack.seqs)}});
  self->send(core, atom::publish_v, std::move(msg), dst.endpoint);
}

// -- callbacks for the producer -----------------------------------------------

void master_state::send(producer_type*, const entity_id& whom,
                        const channel_type::event& what) {
  log::store::debug("send-event", "send event with seq {} and type {} to {}",
                    get_command(what.content).seq,
                    get_command(what.content).content.index(), whom);
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  self->send(core, atom::publish_v, what.content, whom.endpoint);
}

void master_state::send(producer_type*, const entity_id& whom,
                        channel_type::handshake msg) {
  auto i = open_handshakes.find(whom);
  if (i == open_handshakes.end()) {
    auto ss = backend->snapshot();
    if (!ss)
      detail::die("failed to snapshot master");
    auto cmd = make_command_message(
      clones_topic,
      internal_command{msg.offset, id, whom,
                       ack_clone_command{msg.offset, msg.heartbeat_interval,
                                         std::move(*ss)}});
    i = open_handshakes.emplace(whom, std::move(cmd)).first;
  }
  log::store::debug("send-handshake", "send handshake with offset {} to {}",
                    msg.offset, whom);
  self->send(core, atom::publish_v, i->second, whom.endpoint);
}

void master_state::send(producer_type*, const entity_id& whom,
                        channel_type::retransmit_failed msg) {
  auto cmd = make_command_message(
    clones_topic,
    internal_command{0, id, whom, retransmit_failed_command{msg.seq}});
  log::store::debug("send-retransmit-failed",
                    "send retransmit_failed with seq {} to {}", msg.seq, whom);
  self->send(core, atom::publish_v, std::move(cmd), whom.endpoint);
}

void master_state::broadcast(producer_type*, channel_type::heartbeat msg) {
  log::store::debug("broadcast-heartbeat", "broadcast heartbeat with seq {}",
                    msg.seq);
  auto cmd = make_command_message(clones_topic,
                                  internal_command{0, id, entity_id::nil(),
                                                   keepalive_command{msg.seq}});
  self->send(core, atom::publish_v, std::move(cmd));
}

void master_state::broadcast(producer_type*, const channel_type::event& what) {
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  log::store::debug("broadcast-event",
                    "broadcast event with seq {} and type {}",
                    get_command(what.content).seq,
                    get_command(what.content).content.index());
  self->send(core, atom::publish_v, what.content);
}

void master_state::drop(producer_type*, const entity_id& clone,
                        [[maybe_unused]] ec reason) {
  log::core::info("drop-clone", "drop clone {}", clone);
  open_handshakes.erase(clone);
  inputs.erase(clone);
}

void master_state::handshake_completed(producer_type*, const entity_id& clone) {
  log::store::info("handshake-completed", "producer handshake completed for {}",
                   clone);
  open_handshakes.erase(clone);
}

// -- properties ---------------------------------------------------------------

bool master_state::exists(const data& key) {
  if (auto res = backend->exists(key))
    return *res;
  return false;
}

bool master_state::idle() const noexcept {
  auto is_idle = [](auto& kvp) { return kvp.second.idle(); };
  return output.idle() && std::all_of(inputs.begin(), inputs.end(), is_idle)
         && open_handshakes.empty();
}

// -- initial behavior ---------------------------------------------------------

caf::behavior master_state::make_behavior() {
  // Setup.
  self->monitor(core);
  self->set_down_handler([this](const caf::down_msg& msg) { //
    on_down_msg(msg.source, msg.reason);
  });
  // Schedule first tick.
  send_later(self, tick_interval, caf::make_message(atom::tick_v));
  return super::make_behavior(
    // --- local communication -------------------------------------------------
    [this](atom::local, internal_command_variant& content) {
      // Locally received message are already ordered and reliable. Hence, we
      // can process them immediately.
      auto tag = detail::tag_of(content);
      if (tag == command_tag::action) {
        if (auto ptr = get_if<put_unique_command>(&content); ptr && ptr->who) {
          if (auto rp = self->make_response_promise(); rp.pending()) {
            store_actor_state::local_request_key key{ptr->who, ptr->req_id};
            if (!local_requests.emplace(key, rp).second) {
              rp.deliver(caf::make_error(ec::repeated_request_id), ptr->req_id);
              return;
            }
          }
        }
        std::visit([this](auto& x) { consume(x); }, content);
      } else {
        log::store::error("unexpected-command",
                          "received unexpected command locally");
      }
    },
    [this](atom::tick) {
      tick();
      send_later(self, tick_interval, caf::make_message(atom::tick_v));
      if (!idle_callbacks.empty() && idle()) {
        for (auto& rp : idle_callbacks)
          rp.deliver(atom::ok_v);
        idle_callbacks.clear();
      }
    },
    [this](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point_v);
    },
    [this](atom::get, atom::keys) -> caf::result<data> {
      auto x = backend->keys();
      return to_caf_res(std::move(x));
    },
    [this](atom::get, atom::keys, request_id id) {
      auto x = backend->keys();
      if (x)
        return caf::make_message(std::move(*x), id);
      else
        return caf::make_message(native(x.error()), id);
    },
    [this](atom::exists, const data& key) -> caf::result<data> {
      auto x = backend->exists(key);
      return {data{*x}};
    },
    [this](atom::exists, const data& key, request_id id) {
      auto x = backend->exists(key);
      return caf::make_message(data{*x}, id);
    },
    [this](atom::get, const data& key) -> caf::result<data> {
      auto x = backend->get(key);
      return to_caf_res(std::move(x));
    },
    [this](atom::get, const data& key,
           const data& aspect) -> caf::result<data> {
      auto x = backend->get(key, aspect);
      return to_caf_res(std::move(x));
    },
    [this](atom::get, const data& key, request_id id) {
      auto x = backend->get(key);
      if (x)
        return caf::make_message(std::move(*x), id);
      else
        return caf::make_message(native(x.error()), id);
    },
    [this](atom::get, const data& key, const data& value, request_id id) {
      auto x = backend->get(key, value);
      if (x)
        return caf::make_message(std::move(*x), id);
      else
        return caf::make_message(std::move(native(x.error())), id);
    },
    [this](atom::get, atom::name) { return store_name; },
    [this](atom::await, atom::idle) -> caf::result<atom::ok> {
      if (idle())
        return atom::ok_v;
      auto rp = self->make_response_promise();
      idle_callbacks.emplace_back(std::move(rp));
      return caf::delegated<atom::ok>();
    },
    [this](atom::get, atom::name) { return store_name; });
}

} // namespace broker::internal
