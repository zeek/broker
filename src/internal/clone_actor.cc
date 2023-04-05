#include "broker/internal/clone_actor.hh"

#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/detail/appliers.hh"
#include "broker/detail/assert.hh"
#include "broker/error.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/type_id.hh"
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

clone_state::clone_state(caf::event_based_actor* ptr, endpoint_id this_endpoint,
                         std::string nm, caf::timespan master_timeout,
                         caf::actor parent, endpoint::clock* ep_clock,
                         caf::async::consumer_resource<command_message> in_res,
                         caf::async::producer_resource<command_message> out_res)
  : super(ptr), input(this), max_sync_interval(master_timeout) {
  super::init(this_endpoint, ep_clock, std::move(nm), std::move(parent),
              std::move(in_res), std::move(out_res));
  master_topic = store_name / topic::master_suffix();
  super::init(input);
  max_get_delay = caf::get_or(ptr->config(), "broker.store.max-get-delay",
                              defaults::store::max_get_delay);
  BROKER_INFO("attached clone" << id << "to" << store_name);
}

void clone_state::forward(internal_command&& x) {
  self->send(core, atom::publish_v,
             make_command_message(master_topic, std::move(x)));
}

void clone_state::dispatch(const command_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  // Here, we receive all command messages from the stream. The first step is
  // figuring out whether the received message stems from a writer or master.
  auto& cmd = get_command(msg);
  auto seq = cmd.seq;
  auto tag = detail::tag_of(cmd);
  auto type = detail::type_of(cmd);
  if (input.initialized() && cmd.sender != input.producer()) {
    BROKER_WARNING("received message from unrecognized sender:" << cmd.sender);
    return;
  }
  auto is_receiver = [this, &cmd] {
    if (cmd.receiver == id) {
      return true;
    } else {
      if (cmd.receiver) {
        BROKER_DEBUG("received message for" << cmd.receiver);
      } else {
        BROKER_DEBUG("received a broadcast command message");
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
              BROKER_DEBUG("drop repeated ack_clone from" << master_id);
            } else {
              BROKER_ERROR("received ack_clone from"
                           << cmd.sender << "but already attached to"
                           << master_id);
            }
            return;
          } else {
            master_id = cmd.sender;
          }
          auto& inner = get<ack_clone_command>(cmd.content);
          if (input.handle_handshake(cmd.sender, inner.offset,
                                     inner.heartbeat_interval)) {
            BROKER_DEBUG("received ack_clone from" << cmd.sender);
            if (!master_id)
              master_id = cmd.sender;
            set_store(inner.state);
            start_output();
          } else {
            BROKER_DEBUG("drop repeated ack_clone from" << cmd.sender);
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
      // Control messages must be 'unicast'.
      if (!is_receiver()) {
        BROKER_DEBUG("dropped consumer control message for different receiver"
                     << cmd.receiver);
        break;
      } else if (!output_opt) {
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
          if (auto i = output_opt->find_path(entity_id::nil());
              i != output_opt->paths().end()) {
            if (master_id) {
              if (master_id == cmd.sender) {
                i->hdl = cmd.sender;
                BROKER_DEBUG("received write ACK from master" << master_id);
              } else {
                BROKER_ERROR("received write ACK from unexpected source"
                             << cmd.sender);
                return;
              }
            } else {
              BROKER_DEBUG("received write ACK from master" << master_id);
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
          BROKER_ERROR("received bogus consumer control message:" << cmd);
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
  BROKER_TRACE("");
  input.tick();
  if (output_opt)
    output_opt->tick();
}

// -- callbacks for the consumer -----------------------------------------------

void clone_state::consume(consumer_type*, command_message& msg) {
  auto f = [this](auto& cmd) { consume(cmd); };
  std::visit(f, get<1>(msg.unshared()).content);
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
  BROKER_INFO("PUT_UNIQUE_RESULT" << key << cmd.req_id << "->" << cmd.inserted);
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

void clone_state::close(consumer_type* src,
                        [[maybe_unused]] const error& reason) {
  BROKER_ERROR(BROKER_ARG(reason));
  // TODO: send some 'bye, bye' message to enable the master to remove this
  //       clone early, rather than waiting for timeout, see:
  //       https://github.com/zeek/broker/issues/142
}

void clone_state::send(consumer_type* ptr, channel_type::cumulative_ack ack) {
  BROKER_DEBUG(BROKER_ARG(ack) << master_id << ptr->producer());
  BROKER_ASSERT(master_id);
  auto msg = make_command_message(
    master_topic,
    internal_command{0, id, master_id, cumulative_ack_command{ack.seq}});
  self->send(core, atom::publish_v, std::move(msg), ptr->producer().endpoint);
}

void clone_state::send(consumer_type* ptr, channel_type::nack nack) {
  BROKER_DEBUG(BROKER_ARG(nack) << master_id << ptr->producer());
  auto msg = make_command_message(
    master_topic,
    internal_command{0, id, master_id, nack_command{std::move(nack.seqs)}});
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
  BROKER_TRACE(BROKER_ARG(what));
  BROKER_DEBUG("send event with seq"
               << get_command(what.content).seq << "and type"
               << get_command(what.content).content.index() << "to" << dst);
  BROKER_ASSERT(dst == master_id);
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  if (get_command(what.content).receiver != dst) {
    // Technical debt: the event really should be internal_command_variant to
    // allow us to assemble the command_message here instead of altering it.
    get<1>(what.content.unshared()).receiver = dst;
  }
  self->send(core, atom::publish_v, what.content);
}

void clone_state::send(producer_type* ptr, const entity_id&,
                       channel_type::handshake what) {
  BROKER_TRACE(BROKER_ARG(what));
  BROKER_DEBUG("send attach_writer_command with offset" << what.offset);
  auto msg = make_command_message(
    master_topic,
    internal_command{0, id, master_id,
                     attach_writer_command{what.offset,
                                           what.heartbeat_interval}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::send(producer_type* ptr, const entity_id&,
                       channel_type::retransmit_failed what) {
  BROKER_TRACE(BROKER_ARG(what));
  auto msg = make_command_message(
    master_topic,
    internal_command{0, id, master_id, retransmit_failed_command{what.seq}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::broadcast(producer_type* ptr, channel_type::heartbeat what) {
  BROKER_TRACE(BROKER_ARG(what));
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
  BROKER_DEBUG("send heartbeat to master");
  auto msg = make_command_message(
    master_topic,
    internal_command{0, id, entity_id::nil(), keepalive_command{what.seq}});
  self->send(core, atom::publish_v, std::move(msg));
}

void clone_state::broadcast(producer_type* ptr,
                            const channel_type::event& what) {
  BROKER_TRACE(BROKER_ARG(what));
  BROKER_ASSERT(what.seq == get_command(what.content).seq);
  self->send(core, atom::publish_v, what.content);
}

void clone_state::drop(producer_type*, const entity_id&,
                       [[maybe_unused]] ec reason) {
  BROKER_DEBUG(BROKER_ARG(reason));
  // TODO: see comment in close()
}

void clone_state::handshake_completed(producer_type*, const entity_id&) {
  BROKER_DEBUG("completed producer handshake for store" << store_name);
}

// -- properties ---------------------------------------------------------------

data clone_state::keys() const {
  set result;
  for (auto& kvp : store)
    result.emplace(kvp.first);
  return result;
}

void clone_state::set_store(std::unordered_map<data, data> x) {
  BROKER_TRACE("");
  BROKER_INFO("SET" << x);
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
    auto p = std::partition(keys.begin(), keys.end(), is_erased);
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
    BROKER_WARNING("clone_state::start_output called multiple times");
    return;
  }
  BROKER_ASSERT(master_id);
  BROKER_DEBUG("clone" << id << "adds an output channel");
  output_opt.emplace(this);
  super::init(*output_opt);
  output_opt->add(master_id);
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
    BROKER_DEBUG("send command of type" << content.index());
    auto& out = *output_opt;
    auto msg = make_command_message(
      master_topic,
      internal_command{out.next_seq(), id, master_id, std::move(content)});
    out.produce(std::move(msg));
  } else {
    BROKER_DEBUG("add command of type" << content.index() << "to buffer");
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
    [=](atom::local, internal_command_variant& content) {
      if (auto inner = get_if<put_unique_command>(&content)) {
        if (inner->who) {
          BROKER_DEBUG("received put_unique with who"
                       << inner->who << "and req_id" << inner->req_id);
          local_request_key key{inner->who, inner->req_id};
          local_requests.emplace(key, self->make_response_promise());
        } else {
          BROKER_ERROR("received put_unique with invalid sender: DROP!");
          auto rp = self->make_response_promise();
          rp.deliver(caf::make_error(caf::sec::invalid_argument,
                                     "put_unique: invalid sender information"),
                     inner->req_id);
          return;
        }
      }
      send_to_master(std::move(content));
    },
    [=](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point_v);
    },
    [=](atom::tick) {
      tick();
      if (sync_timeout) {
        if (has_master()) {
          sync_timeout.reset();
        } else if (caf::make_timestamp() >= *sync_timeout) {
          BROKER_ERROR("unable to find a master for" << store_name);
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
    [=](atom::get, atom::keys) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp]() mutable {
        auto x = keys();
        BROKER_INFO("KEYS ->" << x);
        rp.deliver(std::move(x));
      });
      return rp;
    },
    [=](atom::get, atom::keys, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(
        rp,
        [this, rp, id]() mutable {
          auto x = keys();
          BROKER_INFO("KEYS"
                      << "with id" << id << "->" << x);
          rp.deliver(std::move(x), id);
        },
        id);
      return rp;
    },
    [=](atom::exists, data& key) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp, key{std::move(key)}]() mutable {
        auto result = this->store.count(key) != 0;
        BROKER_INFO("EXISTS" << key << "->" << result);
        rp.deliver(data{result});
      });
      return rp;
    },
    [=](atom::exists, data& key, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(
        rp,
        [this, rp, key{std::move(key)}, id]() mutable {
          auto result = this->store.count(key) != 0;
          BROKER_INFO("EXISTS" << key << "with id" << id << "->" << result);
          rp.deliver(data{result}, id);
        },
        id);
      return rp;
    },
    [=](atom::get, data& key) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp, key{std::move(key)}]() mutable {
        if (rp.pending()) {
          if (auto i = this->store.find(key); i != this->store.end()) {
            BROKER_INFO("GET" << key << "->" << i->second);
            rp.deliver(i->second);
          } else {
            BROKER_INFO("GET" << key << "-> no_such_key");
            rp.deliver(caf::make_error(ec::no_such_key));
          }
        }
      });
      return rp;
    },
    [=](atom::get, data& key, data& aspect) -> caf::result<data> {
      auto rp = self->make_response_promise();
      get_impl(rp, [this, rp, key{std::move(key)},
                    aspect{std::move(aspect)}]() mutable {
        if (auto i = this->store.find(key); i != this->store.end()) {
          BROKER_INFO("GET" << key << aspect << "->" << i->second);
          if (auto res = visit(detail::retriever{aspect}, i->second))
            rp.deliver(std::move(*res));
          else
            rp.deliver(native(res.error()));
        } else {
          BROKER_INFO("GET" << key << "-> no_such_key");
          rp.deliver(caf::make_error(ec::no_such_key));
        }
      });
      return rp;
    },
    [=](atom::get, data& key, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(
        rp,
        [this, rp, key{std::move(key)}, id]() mutable {
          if (auto i = this->store.find(key); i != this->store.end()) {
            BROKER_INFO("GET" << key << "with id" << id << "->" << i->second);
            rp.deliver(i->second, id);
          } else {
            BROKER_INFO("GET" << key << "with id" << id << "-> no_such_key");
            rp.deliver(caf::make_error(ec::no_such_key), id);
          }
        },
        id);
      return rp;
    },
    [=](atom::get, data& key, data& aspect, request_id id) {
      auto rp = self->make_response_promise();
      get_impl(
        rp,
        [this, rp, key{std::move(key)}, asp{std::move(aspect)}, id]() mutable {
          if (auto i = this->store.find(key); i != this->store.end()) {
            auto x = visit(detail::retriever{asp}, i->second);
            BROKER_INFO("GET" << key << asp << "with id" << id << "->" << x);
            if (x)
              rp.deliver(std::move(*x), id);
            else
              rp.deliver(std::move(native(x.error())), id);
          } else {
            BROKER_INFO("GET" << key << asp << "with id" << id
                              << "-> no_such_key");
            rp.deliver(caf::make_error(ec::no_such_key), id);
          }
        },
        id);
      return rp;
    },
    [=](atom::get, atom::name) { return store_name; },
    [=](atom::await, atom::idle) -> caf::result<atom::ok> {
      if (idle())
        return atom::ok_v;
      auto rp = self->make_response_promise();
      idle_callbacks.emplace_back(std::move(rp));
      return caf::delegated<atom::ok>();
    });
}

} // namespace broker::internal
