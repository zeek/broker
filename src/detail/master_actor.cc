#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/actor.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/behavior.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/make_message.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/sum_type.hpp>
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/store.hh"
#include "broker/time.hh"
#include "broker/topic.hh"

#include "broker/detail/abstract_backend.hh"
#include "broker/detail/die.hh"
#include "broker/detail/master_actor.hh"

namespace broker {
namespace detail {

static optional<timestamp> to_opt_timestamp(timestamp ts,
                                            optional<timespan> span) {
  return span ? ts + *span : optional<timestamp>();
}

// -- initialization -----------------------------------------------------------

master_state::master_state() : output(this) {
  // nop
}

void master_state::init(caf::event_based_actor* ptr, endpoint_id this_endpoint,
                        std::string&& nm, backend_pointer&& bp,
                        caf::actor&& parent, endpoint::clock* ep_clock) {
  super::init(ptr, std::move(this_endpoint), ep_clock, std::move(nm),
              std::move(parent));
  super::init(output);
  clones_topic = store_name / topics::clone_suffix;
  backend = std::move(bp);
  if (auto es = backend->expiries()) {
    for (auto& e : *es) {
      auto& key = e.first;
      auto& expire_time = e.second;
      auto n = clock->now();
      auto dur = expire_time - n;
      auto msg = caf::make_message(atom::expire_v, std::move(key));
      clock->send_later(self, dur, std::move(msg));
    }
  } else {
    die("failed to get master expiries while initializing");
  }
}

void master_state::dispatch(command_message& msg) {
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
        i->second.handle_event(seq, std::move(msg));
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

void master_state::tick() {
  BROKER_TRACE("");
  output.tick();
  for (auto& kvp : inputs)
    kvp.second.tick();
}

void master_state::remind(timespan expiry, const data& key) {
  auto msg = caf::make_message(atom::expire_v, key);
  clock->send_later(self, expiry, std::move(msg));
}

void master_state::expire(data& key) {
  BROKER_INFO("EXPIRE" << key);
  if (auto result = backend->expire(key, clock->now()); !result) {
    BROKER_ERROR("EXPIRE" << key << "(FAILED)" << to_string(result.error()));
  } else if (!*result) {
    BROKER_INFO("EXPIRE" << key << "(IGNORE/STALE)");
  } else {
    expire_command cmd{std::move(key), id};
    emit_expire_event(cmd);
    broadcast(std::move(cmd));
  }
}

// -- callbacks for the consumer -----------------------------------------------

void master_state::consume(consumer_type*, command_message& msg) {
  auto f = [this](auto& cmd) { consume(cmd); };
  caf::visit(f, get<1>(msg.unshared()).content);
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
  if (x.expiry)
    remind(*x.expiry, x.key);
  if (old_value)
    emit_update_event(x, *old_value);
  else
    emit_insert_event(x);
  broadcast(std::move(x));
}

void master_state::consume(put_unique_command& x) {
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("PUT_UNIQUE" << x.key << "->" << x.value << "with expiry"
                           << (x.expiry ? to_string(*x.expiry) : "none"));
  auto broadcast_result = [this, &x](bool inserted) {
    broadcast(put_unique_result_command{inserted, x.who, x.req_id, id});
    if (x.who) {
      local_request_key key{x.who, x.req_id};
      if (auto i = local_requests.find(key); i != local_requests.end()) {
        i->second.deliver(data{inserted});
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
  if (x.expiry)
    remind(*x.expiry, x.key);
  emit_insert_event(x);
  // Broadcast a regular "put" command (clones don't have to do their own
  // existence check) followed by the (positive) result message.
  broadcast(put_command{std::move(x.key), std::move(x.value), x.expiry,
                        std::move(x.publisher)});
  broadcast_result(true);
}

void master_state::consume(erase_command& x) {
  BROKER_TRACE(BROKER_ARG(x));
  BROKER_INFO("ERASE" << x.key);
  if (auto res = backend->erase(x.key); !res) {
    BROKER_WARNING("failed to erase" << x.key << "->" << res.error());
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  emit_erase_event(x.key, x.publisher);
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
    if (x.expiry)
      remind(*x.expiry, x.key);
    // Broadcast a regular "put" command. Clones don't have to repeat the same
    // processing again.
    put_command cmd{std::move(x.key), std::move(*val), nil,
                    std::move(x.publisher)};
    if (old_value)
      emit_update_event(cmd, *old_value);
    else
      emit_insert_event(cmd);
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
    if (x.expiry)
      remind(*x.expiry, x.key);
    // Broadcast a regular "put" command. Clones don't have to repeat the same
    // processing again.
    put_command cmd{std::move(x.key), std::move(*val), nil,
                    std::move(x.publisher)};
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
    } else if (auto keys = get_if<set>(*keys_res)) {
      for (auto& key : *keys)
        emit_erase_event(key, x.publisher);
    } else if (!is<none>(*keys_res)) {
      BROKER_ERROR("backend->keys() returned an unexpected result type");
    }
  }
  if (auto res = backend->clear(); !res)
    die("failed to clear master");
  broadcast(std::move(x));
}

error master_state::consume_nil(consumer_type* src) {
  BROKER_TRACE("");
  // We lost a message from a writer. This is obviously bad, since we lost some
  // information before it made it into the backend. However, it is not a fatal
  // error in the sense that we must abort processing. Hence, we return `none`
  // here to keep processing messages from the writer.
  BROKER_ERROR("lost a message from" << src->producer());
  return nil;
}

void master_state::close(consumer_type* src, [[maybe_unused]] error reason) {
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
  BROKER_TRACE(BROKER_ARG(ack));
  auto msg = make_command_message(
    clones_topic, internal_command{0, id, cumulative_ack_command{ack.seq}});
  self->send(core, atom::publish_v, std::move(msg), ptr->producer().endpoint);
}

void master_state::send(consumer_type* ptr, channel_type::nack nack) {
  BROKER_TRACE(BROKER_ARG(nack));
  auto msg = make_command_message(
    clones_topic, internal_command{0, id, nack_command{std::move(nack.seqs)}});
  self->send(core, atom::publish_v, std::move(msg), ptr->producer().endpoint);
}

// -- callbacks for the producer -----------------------------------------------

void master_state::send(producer_type*, const entity_id& whom,
                        const channel_type::event& what) {
  BROKER_TRACE(BROKER_ARG(whom) << BROKER_ARG(what));
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  self->send(core, atom::publish_v, what.content, whom.endpoint);
}

void master_state::send(producer_type*, const entity_id& whom,
                        channel_type::handshake msg) {
  BROKER_TRACE(BROKER_ARG(whom) << BROKER_ARG(msg));
  auto i = open_handshakes.find(whom);
  if (i == open_handshakes.end()) {
    auto ss = backend->snapshot();
    if (!ss)
      die("failed to snapshot master");
    auto cmd = make_command_message(
      clones_topic,
      internal_command{
        msg.offset, id,
        ack_clone_command{msg.offset, msg.heartbeat_interval, std::move(*ss)}});
    i = open_handshakes.emplace(whom, std::move(cmd)).first;
  }
  self->send(core, atom::publish_v, i->second, whom.endpoint);
}

void master_state::send(producer_type*, const entity_id& whom,
                        channel_type::retransmit_failed msg) {
  BROKER_TRACE(BROKER_ARG(whom) << BROKER_ARG(msg));
  auto cmd = make_command_message(
    clones_topic, internal_command{0, id, retransmit_failed_command{msg.seq}});
  self->send(core, atom::publish_v, std::move(cmd), whom.endpoint);
}

void master_state::broadcast(producer_type*, channel_type::heartbeat msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  auto cmd = make_command_message(
    clones_topic, internal_command{0, id, keepalive_command{msg.seq}});
  self->send(core, atom::publish_v, std::move(cmd));
}

void master_state::broadcast(producer_type*, const channel_type::event& what) {
  BROKER_TRACE(BROKER_ARG(what));
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  self->send(core, atom::publish_v, what.content);
}

void master_state::drop(producer_type*, const entity_id& clone,
                        [[maybe_unused]] ec reason) {
  BROKER_TRACE(BROKER_ARG(clone) << BROKER_ARG(reason));
  open_handshakes.erase(clone);
  inputs.erase(clone);
}

void master_state::handshake_completed(producer_type*, const entity_id& clone) {
  BROKER_TRACE(BROKER_ARG(clone));
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

// -- master actor -------------------------------------------------------------

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           endpoint_id this_endpoint, caf::actor core,
                           std::string store_name,
                           master_state::backend_pointer backend,
                           endpoint::clock* clock) {
  // Setup.
  self->monitor(core);
  self->state.init(self, std::move(this_endpoint), std::move(store_name),
                   std::move(backend), std::move(core), clock);
  self->set_down_handler([self](const caf::down_msg& msg) {
    self->state.on_down_msg(msg.source, msg.reason);
  });
  // Schedule first tick.
  clock->send_later(self, self->state.tick_interval,
                    caf::make_message(atom::tick_v));
  return {
    // --- local communication -------------------------------------------------
    [=](atom::local, internal_command& cmd) {
      // Locally received message are already ordered and reliable. Hence, we
      // can process them immediately.
      auto tag = detail::tag_of(cmd);
      if (tag == command_tag::action) {
        if (auto ptr = get_if<put_unique_command>(&cmd.content);
            ptr && ptr->who) {
          if (auto rp = self->make_response_promise(); rp.pending()) {
            store_actor_state::local_request_key key{ptr->who, ptr->req_id};
            if (!self->state.local_requests.emplace(key, rp).second) {
              rp.deliver(ec::repeated_request_id);
              return;
            }
          }
        }
        auto f = [self](auto& cmd) { self->state.consume(cmd); };
        caf::visit(f, cmd.content);
      } else {
        BROKER_ERROR("received unexpected command locally: " << cmd);
      }
    },
    [=](atom::tick) {
      auto& st = self->state;
      st.tick();
      clock->send_later(self, self->state.tick_interval,
                        caf::make_message(atom::tick_v));
      if (!st.idle_callbacks.empty() && st.idle()) {
        for (auto& rp : st.idle_callbacks)
          rp.deliver(atom::ok_v);
        st.idle_callbacks.clear();
      }
    },
    [=](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point_v);
    },
    [=](atom::expire, data& key) { self->state.expire(key); },
    [=](atom::get, atom::keys) -> caf::result<data> {
      auto x = self->state.backend->keys();
      BROKER_INFO("KEYS ->" << x);
      return x;
    },
    [=](atom::get, atom::keys, request_id id) {
      auto x = self->state.backend->keys();
      BROKER_INFO("KEYS"
                  << "with id:" << id << "->" << x);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::exists, const data& key) -> caf::result<data> {
      auto x = self->state.backend->exists(key);
      BROKER_INFO("EXISTS" << key << "->" << x);
      return {data{std::move(*x)}};
    },
    [=](atom::exists, const data& key, request_id id) {
      auto x = self->state.backend->exists(key);
      BROKER_INFO("EXISTS" << key << "with id:" << id << "->" << x);
      return caf::make_message(data{std::move(*x)}, id);
    },
    [=](atom::get, const data& key) -> caf::result<data> {
      auto x = self->state.backend->get(key);
      BROKER_INFO("GET" << key << "->" << x);
      return x;
    },
    [=](atom::get, const data& key, const data& aspect) -> caf::result<data> {
      auto x = self->state.backend->get(key, aspect);
      BROKER_INFO("GET" << key << aspect << "->" << x);
      return x;
    },
    [=](atom::get, const data& key, request_id id) {
      auto x = self->state.backend->get(key);
      BROKER_INFO("GET" << key << "with id:" << id << "->" << x);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::get, const data& key, const data& value, request_id id) {
      auto x = self->state.backend->get(key, value);
      BROKER_INFO("GET" << key << "->" << value << "with id:" << id << "->"
                        << x);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::get, atom::name) { return self->state.store_name; },
    [=](atom::await, atom::idle) -> caf::result<atom::ok> {
      auto& st = self->state;
      if (st.idle())
        return atom::ok_v;
      auto rp = self->make_response_promise();
      st.idle_callbacks.emplace_back(std::move(rp));
      return caf::delegated<atom::ok>();
    },
    // --- stream handshake with core ------------------------------------------
    [=](const store::stream_type& in) {
      BROKER_DEBUG("received stream handshake from core");
      attach_stream_sink(
        self,
        // input stream
        in,
        // initialize state
        [](caf::unit_t&) {
          // nop
        },
        // processing step
        [=](caf::unit_t&, command_message msg) {
          // forward to state
          self->state.dispatch(msg);
        },
        // cleanup
        [](caf::unit_t&, const caf::error&) {
          // nop
        });
    }};
}

} // namespace detail
} // namespace broker
