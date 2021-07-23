#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/actor.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/behavior.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/make_message.hpp>
#include <caf/message.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/sum_type.hpp>
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/error.hh"
#include "broker/store.hh"
#include "broker/topic.hh"

#include "broker/detail/appliers.hh"
#include "broker/detail/clone_actor.hh"

#include <chrono>

namespace broker::detail {

static double now(endpoint::clock* clock) {
  auto d = clock->now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

// -- initialization -----------------------------------------------------------

clone_state::clone_state() : input(this) {
  // nop
}

void clone_state::init(caf::event_based_actor* ptr, endpoint_id this_endpoint,
                       std::string&& nm, caf::actor&& parent,
                       endpoint::clock* ep_clock) {
  super::init(ptr, std::move(this_endpoint), ep_clock, std::move(nm),
              std::move(parent));
  master_topic = store_name / topic::master_suffix();
  super::init(input);
}

void clone_state::forward(internal_command&& x) {
  self->send(core, atom::publish_v,
             make_command_message(master_topic, std::move(x)));
}

void clone_state::dispatch(command_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  // Here, we receive all command messages from the stream. The first step is
  // figuring out whether the received message stems from a writer or master.
  //
  // Clones can only send control messages (they are always consumers). Writers
  // can send us either actions or control messages (they are producers).
  auto& cmd = get_command(msg);
  auto seq = cmd.seq;
  auto tag = detail::tag_of(cmd);
  auto type = detail::type_of(cmd);
  if (input.initialized() && cmd.sender != input.producer()) {
    BROKER_WARNING(
      "received command message from unrecognized sender: " << cmd.sender);
    return;
  }
  switch (tag) {
    case command_tag::action: {
      // Action messages from the master.
      input.handle_event(seq, std::move(msg));
      break;
    }
    case command_tag::producer_control: {
      // Control messages from the master.
      switch (type) {
        case internal_command::type::ack_clone_command: {
          auto& inner = get<ack_clone_command>(cmd.content);
          if (input.handle_handshake(cmd.sender, inner.offset,
                                     inner.heartbeat_interval)) {
            BROKER_DEBUG("received ack_clone from" << cmd.sender);
            set_store(std::move(inner.state));
            if (output_ptr) {
              output_ptr->stalled = false;
              output_ptr->trigger_handshakes();
              for (auto& msg : output_ptr->buf())
                broadcast(output_ptr.get(), msg);
            }
          } else {
            BROKER_DEBUG("ignored repeated ack_clone from" << cmd.sender);
          }
          break;
        }
        case internal_command::type::keepalive_command: {
          if (!input.initialized()) {
            BROKER_DEBUG("ignored keepalive: input not initialized yet");
            break;
          }
          auto& inner = get<keepalive_command>(cmd.content);
          BROKER_DEBUG("keepalive from master:"
                       << BROKER_ARG2("input.next_seq", input.next_seq())
                       << BROKER_ARG2("input.last_seq", input.last_seq())
                       << BROKER_ARG2("cmd.seq", inner.seq));
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
          BROKER_ERROR("received unexpected producer control message:" << cmd);
        }
      }
      break;
    }
    default: {
      BROKER_ASSERT(tag == command_tag::consumer_control);
      if (!output_ptr) {
        BROKER_DEBUG("received control message for a non-existing channel");
        break;
      }
      // Control messages from the master for the writer.
      switch (type) {
        case internal_command::type::cumulative_ack_command: {
          auto& inner = get<cumulative_ack_command>(cmd.content);
          // We create the path with entity_id::nil(), but the channel still
          // cares about the handle. Hence, we need to set the actual handle
          // once once the master responds with an ACK.
          if (auto i = output_ptr->find_path(entity_id::nil());
              i != output_ptr->paths().end()) {
            i->hdl = cmd.sender;
            BROKER_DEBUG("received ACK from the master for the writer");
          }
          output_ptr->handle_ack(cmd.sender, inner.seq);
          break;
        }
        case internal_command::type::nack_command: {
          auto& inner = get<nack_command>(cmd.content);
          output_ptr->handle_nack(cmd.sender, inner.seqs);
          break;
        }
        default: {
          BROKER_ERROR("received bogus consumer control message:" << cmd);
        }
      }
    }
  }
}

void clone_state::tick() {
  BROKER_TRACE("");
  input.tick();
  if (output_ptr && !output_ptr->stalled)
    output_ptr->tick();
}

// -- callbacks for the consumer -----------------------------------------------

void clone_state::consume(consumer_type*, command_message& msg) {
  auto f = [this](auto& cmd) { consume(cmd); };
  caf::visit(f, get<1>(msg.unshared()).content);
}

void clone_state::consume(put_command& x) {
  BROKER_INFO("PUT" << x.key << "->" << x.value << "with expiry" << x.expiry);
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
  local_request_key key{cmd.who, cmd.req_id};
  if (auto i = local_requests.find(key); i != local_requests.end()) {
    i->second.deliver(data{cmd.inserted}, cmd.req_id);
    local_requests.erase(i);
  }
}

void clone_state::consume(erase_command& x) {
  BROKER_INFO("ERASE" << x.key);
  if (store.erase(x.key) != 0)
    emit_erase_event(x.key, x.publisher);
}

void clone_state::consume(expire_command& x) {
  BROKER_INFO("EXPIRE" << x.key);
  if (store.erase(x.key) != 0)
    emit_expire_event(x.key, x.publisher);
}

void clone_state::consume(clear_command& x) {
  BROKER_INFO("CLEAR");
  for (auto& kvp : store)
    emit_erase_event(kvp.first, x.publisher);
  store.clear();
}

error clone_state::consume_nil(consumer_type* src) {
  BROKER_ERROR("clone out of sync: lost message from the master!");
  // By returning an error, we cause the channel to abort and call `close`.
  return ec::broken_clone;
}

void clone_state::close(consumer_type* src, [[maybe_unused]] error reason) {
  BROKER_ERROR(BROKER_ARG(reason));
  // TODO: send some 'bye, bye' message to enable the master to remove this
  //       clone early, rather than waiting for timeout, see:
  //       https://github.com/zeek/broker/issues/142
}

void clone_state::send(consumer_type* ptr, channel_type::cumulative_ack ack) {
  BROKER_DEBUG(BROKER_ARG(ack));
  auto msg = make_command_message(
    master_topic, internal_command{0, id, cumulative_ack_command{ack.seq}});
  self->send(core, atom::publish_v, std::move(msg), ptr->producer().endpoint);
}

void clone_state::send(consumer_type* ptr, channel_type::nack nack) {
  BROKER_DEBUG(BROKER_ARG(nack));
  auto msg = make_command_message(
    master_topic, internal_command{0, id, nack_command{std::move(nack.seqs)}});
  if (ptr->initialized())
    self->send(core, atom::publish_v, std::move(msg), ptr->producer().endpoint);
  else
    self->send(core, atom::publish_v, std::move(msg));
}

// -- callbacks for the producer -----------------------------------------------

void clone_state::send(producer_type* ptr, const entity_id&,
                       const channel_type::event& what) {
  BROKER_TRACE(BROKER_ARG(what) << BROKER_ARG2("stalled", ptr->stalled));
  if (ptr->stalled)
    return;
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  self->send(core, atom::publish_v, what.content);
}

void clone_state::send(producer_type* ptr, const entity_id&,
                       channel_type::handshake what) {
  BROKER_TRACE(BROKER_ARG(what) << BROKER_ARG2("stalled", ptr->stalled));
  if (ptr->stalled)
    return;
  auto msg = make_command_message(
    master_topic,
    internal_command{
      0, id, attach_writer_command{what.offset, what.heartbeat_interval}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::send(producer_type* ptr, const entity_id&,
                       channel_type::retransmit_failed what) {
  BROKER_TRACE(BROKER_ARG(what) << BROKER_ARG2("stalled", ptr->stalled));
  if (ptr->stalled)
    return;
  auto msg = make_command_message(
    master_topic, internal_command{0, id, retransmit_failed_command{what.seq}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::broadcast(producer_type* ptr, channel_type::heartbeat what) {
  BROKER_TRACE(BROKER_ARG(what) << BROKER_ARG2("stalled", ptr->stalled));
  if (ptr->stalled)
    return;
  // Re-send handshakes as well. Usually, the keepalive message also acts as
  // handshake. However, the master did not open the channel in this case. We
  // first need to create it by sending `attach_writer_command`. Everything
  // received before this attach message is going to be ignored by the master.
  for (auto& path : ptr->paths()) {
    if (path.acked == 0) {
      BROKER_DEBUG("re-send attach_writer_command");
      send(ptr, path.hdl,
           channel_type::handshake{path.offset, ptr->heartbeat_interval()});
    }
  }
  auto msg = make_command_message(
    master_topic, internal_command{0, id, keepalive_command{what.seq}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::broadcast(producer_type* ptr,
                            const channel_type::event& what) {
  BROKER_TRACE(BROKER_ARG(what) << BROKER_ARG2("stalled", ptr->stalled));
  if (ptr->stalled)
    return;
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  self->send(core, atom::publish_v, what.content);
}

void clone_state::drop(producer_type*, const entity_id&,
                       [[maybe_unused]] ec reason) {
  BROKER_TRACE(BROKER_ARG(reason));
  // TODO: see comment in close()
}

void clone_state::handshake_completed(producer_type*, const entity_id&) {
  BROKER_DEBUG("completed handshake between writer and master for store"
               << store_name);
}

// -- properties ---------------------------------------------------------------

data clone_state::keys() const {
  set result;
  for (auto& kvp : store)
    result.emplace(kvp.first);
  return result;
}

clone_state::producer_type& clone_state::output() {
  // TODO: we only use a pointer here, because caf::optional lacks the `emplace`
  //       member function. Either add that member function upstream or use
  //       std::optional instead if all supported platforms support it.
  if (!output_ptr) {
    BROKER_DEBUG("add output channel to clone " << store_name);
    output_ptr.reset(new producer_type(this));
    super::init(*output_ptr);
    // Remove `stalled` flag immediately if the input is ready.
    if (input.initialized())
      output_ptr->stalled = false;
    // We reach the master by publishing to its topic. Hence, we actually don't
    // need a handle to it at all for the writer.
    output_ptr->add(entity_id::nil());
  }
  return *output_ptr;
}

void clone_state::set_store(std::unordered_map<data, data> x) {
  BROKER_INFO("SET" << x);
  // We consider the master the source of all updates.
  entity_id publisher = input.producer();
  // Short-circuit messages with an empty state.
  if (x.empty()) {
    if (!store.empty()) {
      clear_command cmd{std::move(publisher)};
      consume(cmd);
    }
    return;
  }
  if (store.empty()) {
    // Emit insert events.
    for (auto& [key, value] : x)
      emit_insert_event(key, value, nil, publisher);
  } else {
    // Emit erase and put events.
    std::vector<const data*> keys;
    keys.reserve(store.size());
    for (auto& kvp : store)
      keys.emplace_back(&kvp.first);
    auto is_erased = [&x](const data* key) { return x.count(*key) == 0; };
    auto p = std::partition(keys.begin(), keys.end(), is_erased);
    for (auto i = keys.begin(); i != p; ++i)
      emit_erase_event(**i, entity_id{});
    for (auto i = p; i != keys.end(); ++i) {
      const auto& value = x[**i];
      emit_update_event(**i, store[**i], value, nil, publisher);
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
        emit_insert_event(key, value, nil, publisher);
  }
  // Override local state.
  store = std::move(x);
}

bool clone_state::has_master() const noexcept {
  return input.initialized();
}

bool clone_state::idle() const noexcept {
  return input.idle() && (!output_ptr || output_ptr->idle());
}

// -- master actor -------------------------------------------------------------

caf::behavior clone_actor(caf::stateful_actor<clone_state>* self,
                          endpoint_id this_endpoint, caf::actor core,
                          std::string store_name, double resync_interval,
                          double stale_interval,
                          double mutation_buffer_interval,
                          endpoint::clock* clock) {
  // Setup.
  self->monitor(core);
  self->state.init(self, std::move(this_endpoint), std::move(store_name),
                   std::move(core), clock);
  self->set_down_handler([=](const caf::down_msg& msg) {
    self->state.on_down_msg(msg.source, msg.reason);
  });
  // Ask the master to add this clone.
  self->state.send(std::addressof(self->state.input),
                   clone_state::channel_type::nack{{0}});
  // Schedule first tick.
  clock->send_later(self, defaults::store::tick_interval,
                    caf::make_message(atom::tick_v));
  return self->state.make_behavior(
    // --- local communication -------------------------------------------------
    [=](atom::local, internal_command& cmd) {
      auto& st = self->state;
      if (auto inner = get_if<put_unique_command>(&cmd.content);
          inner && inner->who) {
        local_request_key key{inner->who, inner->req_id};
        st.local_requests.emplace(key, self->make_response_promise());
      }
      auto& out = st.output();
      cmd.seq = out.next_seq();
      cmd.sender = self->state.id;
      auto msg = make_command_message(st.master_topic, std::move(cmd));
      out.produce(std::move(msg));
    },
    [=](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point_v);
    },
    [=](atom::tick) {
      auto& st = self->state;
      st.tick();
      clock->send_later(self, defaults::store::tick_interval,
                        caf::make_message(atom::tick_v));
      if (!st.idle_callbacks.empty() && st.idle()) {
        for (auto& rp : st.idle_callbacks)
          rp.deliver(atom::ok_v);
        st.idle_callbacks.clear();
      }
    },
    [=](atom::get, atom::keys) -> caf::result<data> {
      if (!self->state.has_master())
        return {ec::stale_data};
      auto x = self->state.keys();
      BROKER_INFO("KEYS ->" << x);
      return {x};
    },
    [=](atom::get, atom::keys, request_id id) {
      if (!self->state.has_master())
        return caf::make_message(make_error(ec::stale_data), id);
      auto x = self->state.keys();
      BROKER_INFO("KEYS"
                  << "with id" << id << "->" << x);
      return caf::make_message(std::move(x), id);
    },
    [=](atom::exists, const data& key) -> caf::result<data> {
      if (!self->state.has_master())
        return {ec::stale_data};
      auto result = (self->state.store.find(key) != self->state.store.end());
      BROKER_INFO("EXISTS" << key << "->" << result);
      return data{result};
    },
    [=](atom::exists, const data& key, request_id id) {
      if (!self->state.has_master())
        return caf::make_message(make_error(ec::stale_data), id);
      auto r = (self->state.store.find(key) != self->state.store.end());
      auto result = caf::make_message(data{r}, id);
      BROKER_INFO("EXISTS" << key << "with id" << id << "->" << r);
      return result;
    },
    [=](atom::get, const data& key) -> caf::result<data> {
      if (!self->state.has_master())
        return {ec::stale_data};
      expected<data> result = ec::no_such_key;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        result = i->second;
      BROKER_INFO("GET" << key << "->" << result);
      return result;
    },
    [=](atom::get, const data& key, const data& aspect) -> caf::result<data> {
      if (!self->state.has_master())
        return {ec::stale_data};
      expected<data> result = ec::no_such_key;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        result = caf::visit(retriever{aspect}, i->second);
      BROKER_INFO("GET" << key << aspect << "->" << result);
      return result;
    },
    [=](atom::get, const data& key, request_id id) {
      if (!self->state.has_master())
        return caf::make_message(make_error(ec::stale_data), id);
      caf::message result;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end()) {
        result = caf::make_message(i->second, id);
        BROKER_INFO("GET" << key << "with id" << id << "->" << i->second);
      } else {
        result = caf::make_message(make_error(ec::no_such_key), id);
        BROKER_INFO("GET" << key << "with id" << id << "-> no_such_key");
      }
      return result;
    },
    [=](atom::get, const data& key, const data& aspect, request_id id) {
      if (!self->state.has_master())
        return caf::make_message(make_error(ec::stale_data), id);
      caf::message result;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end()) {
        auto x = caf::visit(retriever{aspect}, i->second);
        BROKER_INFO("GET" << key << aspect << "with id" << id << "->" << x);
        if (x)
          result = caf::make_message(*x, id);
        else
          result = caf::make_message(std::move(x.error()), id);
      } else {
        result = caf::make_message(make_error(ec::no_such_key), id);
        BROKER_INFO("GET" << key << aspect << "with id" << id
                          << "-> no_such_key");
      }
      return result;
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
    [=](store::stream_type in) {
      attach_stream_sink(
        self,
        // input stream
        in,
        // initialize state
        [](caf::unit_t&) {
          // nop
        },
        // processing step
        [=](caf::unit_t&, store::stream_type::value_type msg) {
          self->state.dispatch(msg);
        });
    });
}

} // namespace broker::detail
