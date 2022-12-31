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

master_state::metrics_t::metrics_t(caf::actor_system& sys,
                                   const std::string& name) noexcept {
  metric_factory factory{sys};
  entries = factory.store.entries_instance(name);
}

// -- initialization -----------------------------------------------------------

master_state::master_state(
  caf::event_based_actor* ptr, endpoint_id this_endpoint, std::string nm,
  backend_pointer bp, caf::actor parent, endpoint::clock* ep_clock,
  caf::async::consumer_resource<command_message> in_res,
  caf::async::producer_resource<command_message> out_res)
  : super(ptr), output(this), metrics(ptr->system(), nm) {
  super::init(this_endpoint, ep_clock, std::move(nm), std::move(parent),
              std::move(in_res), std::move(out_res));
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
    metrics.entries->value(static_cast<int64_t>(*entries));
  }
  BROKER_INFO("attached master" << id << "to" << store_name);
}

void master_state::dispatch(const command_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
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
        BROKER_DEBUG("received action from unknown sender:" << cmd.sender);
      break;
    }
    case command_tag::producer_control: {
      // Control messages from writers.
      if (auto i = inputs.find(cmd.sender); i != inputs.end()) {
        switch (type) {
          case internal_command::type::attach_writer_command: {
            BROKER_DEBUG("ignore repeated handshake from" << cmd.sender);
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
            BROKER_ERROR("received bogus producer control message:" << cmd);
          }
        }
      } else if (type == internal_command::type::attach_writer_command) {
        BROKER_DEBUG("attach new writer:" << cmd.sender);
        auto& inner = get<attach_writer_command>(cmd.content);
        i = inputs.emplace(cmd.sender, this).first;
        super::init(i->second);
        i->second.producer(cmd.sender);
        if (!i->second.handle_handshake(inner.offset,
                                        inner.heartbeat_interval)) {
          BROKER_ERROR("abort connection: handle_handshake returned false");
          inputs.erase(i);
        }
      } else {
        BROKER_DEBUG("received command from unknown sender:" << cmd);
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
          BROKER_ERROR("received bogus consumer control message:" << cmd);
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
  BROKER_TRACE("");
  output.tick();
  for (auto& kvp : inputs)
    kvp.second.tick();
  auto t = clock->now();
  for (auto i = expirations.begin(); i != expirations.end();) {
    if (t > i->second) {
      const auto& key = i->first;
      BROKER_INFO("EXPIRE" << key);
      if (auto result = backend->expire(key, t); !result) {
        BROKER_ERROR("EXPIRE" << key << "(FAILED)"
                              << to_string(result.error()));
      } else if (!*result) {
        BROKER_INFO("EXPIRE" << key << "(IGNORE/STALE)");
      } else {
        expire_command cmd{key, id};
        emit_expire_event(cmd);
        broadcast(std::move(cmd));
        metrics.entries->dec();
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
  std::visit(f, get<1>(msg.unshared()).content);
}

void master_state::consume(put_command& x) {
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("PUT" << x.key << "->" << x.value << "with expiry"
                    << (x.expiry ? to_string(*x.expiry) : "none"));
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto old_value = backend->get(x.key);
  auto result = backend->put(x.key, x.value, et);
  if (!result) {
    BROKER_WARNING("failed to put" << x.key << "->" << x.value);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  set_expire_time(x.key, x.expiry);
  if (old_value) {
    emit_update_event(x, *old_value);
  } else {
    emit_insert_event(x);
    metrics.entries->inc();
  }
  broadcast(std::move(x));
}

void master_state::consume(put_unique_command& x) {
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("PUT_UNIQUE" << x.key << "->" << x.value << "with expiry"
                           << (x.expiry ? to_string(*x.expiry) : "none")
                           << "from" << x.who);
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
    BROKER_WARNING("failed to put_unique" << x.key << "->" << x.value);
    broadcast_result(false);
    return;
  }
  set_expire_time(x.key, x.expiry);
  emit_insert_event(x);
  metrics.entries->inc();
  // Broadcast a regular "put" command (clones don't have to do their own
  // existence check) followed by the (positive) result message.
  broadcast(
    put_command{std::move(x.key), std::move(x.value), x.expiry, x.publisher});
  broadcast_result(true);
}

void master_state::consume(erase_command& x) {
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("ERASE" << x.key);
  if (!exists(x.key)) {
    BROKER_DEBUG("failed to erase" << x.key << "-> no such key");
    return;
  }
  if (auto res = backend->erase(x.key); !res) {
    BROKER_WARNING("failed to erase" << x.key << "->" << res.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  emit_erase_event(x.key, x.publisher);
  metrics.entries->dec();
  broadcast(std::move(x));
}

void master_state::consume(add_command& x) {
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("ADD" << x);
  auto old_value = backend->get(x.key);
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  if (auto res = backend->add(x.key, x.value, x.init_type, et); !res) {
    BROKER_WARNING("failed to add" << x.value << "to" << x.key << "->"
                                   << res.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (auto val = backend->get(x.key); !val) {
    BROKER_ERROR("failed to get"
                 << x.value << "after add() returned success:" << val.error());
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
      metrics.entries->inc();
    }
    broadcast(std::move(cmd));
  }
}

void master_state::consume(subtract_command& x) {
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("SUBTRACT" << x);
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto old_value = backend->get(x.key);
  if (!old_value) {
    // Unlike `add`, `subtract` fails if the key didn't exist previously.
    BROKER_WARNING("cannot substract from non-existing value for key" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (auto res = backend->subtract(x.key, x.value, et); !res) {
    BROKER_WARNING("failed to substract" << x.value << "from" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (auto val = backend->get(x.key); !val) {
    BROKER_ERROR("failed to get"
                 << x.value
                 << "after subtract() returned success:" << val.error());
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
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("CLEAR" << x);
  if (auto keys_res = backend->keys(); !keys_res) {
    BROKER_ERROR("unable to obtain keys:" << keys_res.error());
    return;
  } else {
    if (auto keys = get_if<vector>(*keys_res)) {
      for (auto& key : *keys)
        emit_erase_event(key, x.publisher);
      metrics.entries->value(0);
    } else if (auto keys = get_if<set>(*keys_res)) {
      for (auto& key : *keys)
        emit_erase_event(key, x.publisher);
      metrics.entries->value(0);
    } else if (!is<none>(*keys_res)) {
      BROKER_ERROR("backend->keys() returned an unexpected result type");
    }
  }
  if (auto res = backend->clear(); !res)
    detail::die("failed to clear master");
  broadcast(x);
}

error master_state::consume_nil(consumer_type* src) {
  BROKER_TRACE("");
  // We lost a message from a writer. This is obviously bad, since we lost some
  // information before it made it into the backend. However, it is not a fatal
  // error in the sense that we must abort processing. Hence, we return `none`
  // here to keep processing messages from the writer.
  BROKER_ERROR("lost a message from" << src->producer());
  return {};
}

void master_state::close(consumer_type* src, const error& reason) {
  BROKER_TRACE(BROKER_ARG(reason));
  if (auto i = inputs.find(src->producer()); i != inputs.end()) {
    if (reason)
      BROKER_INFO("removed" << src->producer() << "due to an error:" << reason);
    else
      BROKER_DEBUG("received graceful shutdown for" << src->producer());
    inputs.erase(i);
  } else {
    BROKER_ERROR("close called from an unknown consumer");
  }
}

void master_state::send(consumer_type* ptr, channel_type::cumulative_ack ack) {
  auto dst = ptr->producer();
  BROKER_DEBUG(BROKER_ARG(ack) << BROKER_ARG(dst));
  auto msg = make_command_message(
    clones_topic,
    internal_command{0, id, dst, cumulative_ack_command{ack.seq}});
  self->send(core, atom::publish_v, std::move(msg), dst.endpoint);
}

void master_state::send(consumer_type* ptr, channel_type::nack nack) {
  auto dst = ptr->producer();
  BROKER_DEBUG(BROKER_ARG(nack) << BROKER_ARG(dst));
  auto msg = make_command_message(
    clones_topic,
    internal_command{0, id, dst, nack_command{std::move(nack.seqs)}});
  self->send(core, atom::publish_v, std::move(msg), dst.endpoint);
}

// -- callbacks for the producer -----------------------------------------------

void master_state::send(producer_type*, const entity_id& whom,
                        const channel_type::event& what) {
  BROKER_DEBUG("send event with seq"
               << get_command(what.content).seq << "and type"
               << get_command(what.content).content.index() << "to" << whom);
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
  BROKER_DEBUG("send producer handshake with offset" << msg.offset << "to"
                                                     << whom);
  self->send(core, atom::publish_v, i->second, whom.endpoint);
}

void master_state::send(producer_type*, const entity_id& whom,
                        channel_type::retransmit_failed msg) {
  BROKER_TRACE(BROKER_ARG(whom) << BROKER_ARG(msg));
  auto cmd = make_command_message(
    clones_topic,
    internal_command{0, id, whom, retransmit_failed_command{msg.seq}});
  BROKER_DEBUG("send retransmit_failed with seq" << msg.seq << "to" << whom);
  self->send(core, atom::publish_v, std::move(cmd), whom.endpoint);
}

void master_state::broadcast(producer_type*, channel_type::heartbeat msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  BROKER_DEBUG("broadcast keepalive_command with seq" << msg.seq);
  auto cmd = make_command_message(clones_topic,
                                  internal_command{0, id, entity_id::nil(),
                                                   keepalive_command{msg.seq}});
  self->send(core, atom::publish_v, std::move(cmd));
}

void master_state::broadcast(producer_type*, const channel_type::event& what) {
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  BROKER_DEBUG("broadcast event with seq"
               << get_command(what.content).seq << "and type"
               << get_command(what.content).content.index());
  self->send(core, atom::publish_v, what.content);
}

void master_state::drop(producer_type*, const entity_id& clone,
                        [[maybe_unused]] ec reason) {
  BROKER_TRACE(BROKER_ARG(clone) << BROKER_ARG(reason));
  BROKER_INFO("drop" << clone);
  open_handshakes.erase(clone);
  inputs.erase(clone);
}

void master_state::handshake_completed(producer_type*, const entity_id& clone) {
  BROKER_TRACE(BROKER_ARG(clone));
  BROKER_INFO("producer handshake completed for" << clone);
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
  BROKER_TRACE(BROKER_ARG(id) << BROKER_ARG(core) << BROKER_ARG(store_name));
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
      BROKER_TRACE(BROKER_ARG(content));
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
        BROKER_ERROR("received unexpected command locally:" << content);
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
      BROKER_INFO("KEYS ->" << x);
      return to_caf_res(std::move(x));
    },
    [this](atom::get, atom::keys, request_id id) {
      auto x = backend->keys();
      BROKER_INFO("KEYS"
                  << "with id:" << id << "->" << x);
      if (x)
        return caf::make_message(std::move(*x), id);
      else
        return caf::make_message(native(x.error()), id);
    },
    [this](atom::exists, const data& key) -> caf::result<data> {
      auto x = backend->exists(key);
      BROKER_INFO("EXISTS" << key << "->" << x);
      return {data{*x}};
    },
    [this](atom::exists, const data& key, request_id id) {
      auto x = backend->exists(key);
      BROKER_INFO("EXISTS" << key << "with id:" << id << "->" << x);
      return caf::make_message(data{*x}, id);
    },
    [this](atom::get, const data& key) -> caf::result<data> {
      auto x = backend->get(key);
      BROKER_INFO("GET" << key << "->" << x);
      return to_caf_res(std::move(x));
    },
    [this](atom::get, const data& key,
           const data& aspect) -> caf::result<data> {
      auto x = backend->get(key, aspect);
      BROKER_INFO("GET" << key << aspect << "->" << x);
      return to_caf_res(std::move(x));
    },
    [this](atom::get, const data& key, request_id id) {
      auto x = backend->get(key);
      BROKER_INFO("GET" << key << "with id:" << id << "->" << x);
      if (x)
        return caf::make_message(std::move(*x), id);
      else
        return caf::make_message(native(x.error()), id);
    },
    [this](atom::get, const data& key, const data& value, request_id id) {
      auto x = backend->get(key, value);
      BROKER_INFO("GET" << key << "->" << value << "with id:" << id << "->"
                        << x);
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
