#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/event_based_actor.hpp>
#include <caf/actor.hpp>
#include <caf/make_message.hpp>
#include <caf/sum_type.hpp>
#include <caf/behavior.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/system_messages.hpp>
#include <caf/unit.hpp>
#include <caf/error.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/store.hh"
#include "broker/time.hh"
#include "broker/topic.hh"

#include "broker/detail/abstract_backend.hh"
#include "broker/detail/die.hh"
#include "broker/detail/master_actor.hh"

namespace broker {
namespace detail {

static inline optional<timestamp> to_opt_timestamp(timestamp ts,
                                                   optional<timespan> span) {
  return span ? ts + *span : optional<timestamp>();
}

const char* master_state::name = "master_actor";

master_state::master_state() : self(nullptr), clock(nullptr) {
  // nop
}

void master_state::init(caf::event_based_actor* ptr, std::string&& nm,
                        backend_pointer&& bp, caf::actor&& parent,
                        endpoint::clock* ep_clock) {
  BROKER_ASSERT(ep_clock != nullptr);
  self = ptr;
  id = std::move(nm);
  clones_topic = id / topics::clone_suffix;
  backend = std::move(bp);
  core = std::move(parent);
  clock = ep_clock;
  auto es = backend->expiries();
  if (!es)
    die("failed to get master expiries while initializing");
  for (auto& e : *es) {
    auto& key = e.first;
    auto& expire_time = e.second;
    auto n = clock->now();
    auto dur = expire_time - n;
    auto msg = caf::make_message(atom::expire::value, std::move(key));
    clock->send_later(self, dur, std::move(msg));
  }
}

void master_state::broadcast(internal_command&& x) {
  self->send(core, atom::publish::value, clones_topic, std::move(x));
}

void master_state::remind(timespan expiry, const data& key) {
  auto msg = caf::make_message(atom::expire::value, key);
  clock->send_later(self, expiry, std::move(msg));
}

void master_state::expire(data& key) {
  BROKER_INFO("EXPIRE" << key);
  auto result = backend->expire(key, clock->now());
  if (!result)
    BROKER_ERROR("failed to expire key:" << to_string(result.error()));
  else if (!*result)
    BROKER_WARNING("ignoring stale expiration reminder");
  else {
    broadcast_cmd_to_clones(erase_command{std::move(key)});
  }
}

void master_state::command(internal_command& cmd) {
  caf::visit(*this, cmd.content);
}

void master_state::operator()(none) {
  BROKER_INFO("received empty command");
}

void master_state::operator()(put_command& x) {
  BROKER_INFO("PUT" << x.key << "->" << x.value << "with expiry" << (x.expiry ? to_string(*x.expiry) : "none"));
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto result = backend->put(x.key, x.value, et);
  if (!result) {
    BROKER_WARNING("failed to put" << x.key << "->" << x.value);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (x.expiry)
    remind(*x.expiry, x.key);
  broadcast_cmd_to_clones(std::move(x));
}

void master_state::operator()(put_unique_command& x) {
  BROKER_INFO("PUT_UNIQUE" << x.key << "->" << x.value << "with expiry" << (x.expiry ? to_string(*x.expiry) : "none"));

  auto exists_result = backend->exists(x.key);

  if (!exists_result) {
    BROKER_WARNING("failed to put_unique existence check" << x.key << "->" << x.value);
    return; // TODO: propagate failure? to all clones? as status msg?
  }

  if (*exists_result) {
    // Note that we don't bother broadcasting this operation to clones since
    // no change took place.
    self->send(x.who, caf::make_message(data{false}, x.req_id));
    return;
  }

  self->send(x.who, caf::make_message(data{true}, x.req_id));
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto result = backend->put(x.key, x.value, et);

  if (!result) {
    BROKER_WARNING("failed to put_unique" << x.key << "->" << x.value);
    return; // TODO: propagate failure? to all clones? as status msg?
  }

  if (x.expiry)
    remind(*x.expiry, x.key);

  // Note that we could just broadcast a regular "put" command here instead
  // since clones shouldn't have to do their own existence check.
  broadcast_cmd_to_clones(std::move(x));
}

void master_state::operator()(erase_command& x) {
  BROKER_INFO("ERASE" << x.key);
  auto result = backend->erase(x.key);
  if (!result) {
    BROKER_WARNING("failed to erase" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  broadcast_cmd_to_clones(std::move(x));
}

void master_state::operator()(add_command& x) {
  BROKER_INFO("ADD" << x);
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto result = backend->add(x.key, x.value, x.init_type, et);
  if (!result) {
    BROKER_WARNING("failed to add" << x.value << "to" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (x.expiry)
    remind(*x.expiry, x.key);
  broadcast_cmd_to_clones(std::move(x));
}

void master_state::operator()(subtract_command& x) {
  BROKER_INFO("SUBTRACT" << x);
  auto et = to_opt_timestamp(clock->now(), x.expiry);
  auto result = backend->subtract(x.key, x.value, et);
  if (!result) {
    BROKER_WARNING("failed to substract" << x.value << "from" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (x.expiry)
    remind(*x.expiry, x.key);
  broadcast_cmd_to_clones(std::move(x));
}

void master_state::operator()(snapshot_command& x) {
  BROKER_INFO("SNAPSHOT from" << to_string(x.remote_core));
  if (x.remote_core == nullptr || x.remote_clone == nullptr) {
    BROKER_INFO("snapshot command with invalid address received");
    return;
  }
  auto ss = backend->snapshot();
  if (!ss)
    die("failed to snapshot master");
  self->monitor(x.remote_core);
  clones.emplace(x.remote_core->address(), x.remote_clone);

  // The snapshot gets sent over a different channel than updates,
  // so we send a "sync" point over the update channel that target clone
  // can use in order to apply any updates that arrived before it
  // received the now-outdated snapshot.
  broadcast_cmd_to_clones(snapshot_sync_command{x.remote_clone});

  // TODO: possible improvements to do here
  // (1) Use a separate *streaming* channel to send the snapshot.
  //     A benefit of that would potentially be less latent queries
  //     that go directly against the master store.
  // (2) Always keep an updated snapshot in memory on the master to
  //     avoid numerous expensive retrievals from persistent backends
  //     in quick succession (e.g. at startup).
  // (3) As an alternative to (2), give backends an API to stream
  //     key-value pairs without ever needing the full snapshot in
  //     memory.  Note that this would require halting the application
  //     of updates on the master while there are any snapshot streams
  //     still underway.
  self->send(x.remote_clone, set_command{std::move(*ss)});
}

void master_state::operator()(snapshot_sync_command&) {
  BROKER_ERROR("received a snapshot_sync_command in master actor");
}

void master_state::operator()(set_command& x) {
  BROKER_ERROR("received a set_command in master actor");
}

void master_state::operator()(clear_command& x) {
  BROKER_INFO("CLEAR" << x);
  auto res = backend->clear();
  if (!res)
    die("failed to clear master");
  broadcast_cmd_to_clones(std::move(x));
}

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string id,
                           master_state::backend_pointer backend,
                           endpoint::clock* clock) {
  self->monitor(core);
  self->state.init(self, std::move(id), std::move(backend),
                   std::move(core), clock);
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      if (msg.source == core) {
        BROKER_INFO("core is down, kill master as well");
        self->quit(msg.reason);
      } else {
        BROKER_INFO("lost a clone");
        self->state.clones.erase(msg.source);
      }
    }
  );
  return {
    // --- local communication -------------------------------------------------
    [=](atom::local, internal_command& x) {
      // treat locally and remotely received commands in the same way
      self->state.command(x);
    },
    [=](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point::value);
    },
    [=](atom::expire, data& key) {
      self->state.expire(key);
    },
    [=](atom::get, atom::keys) -> expected<data> {
      auto x = self->state.backend->keys();
      BROKER_INFO("KEYS ->" << x);
      return x;
    },
    [=](atom::get, atom::keys, request_id id) {
      auto x = self->state.backend->keys();
      BROKER_INFO("KEYS" << "with id:" << id << "->" << x);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::exists, const data& key) -> expected<data> {
      auto x = self->state.backend->exists(key);
      BROKER_INFO("EXISTS" << key << "->" << x);
      return {data{std::move(*x)}};
    },
    [=](atom::exists, const data& key, request_id id) {
      auto x = self->state.backend->exists(key);
      BROKER_INFO("EXISTS" << key << "with id:" << id << "->" << x);
      return caf::make_message(data{std::move(*x)}, id);
    },
    [=](atom::get, const data& key) -> expected<data> {
      auto x = self->state.backend->get(key);
      BROKER_INFO("GET" << key << "->" << x);
      return x;
    },
    [=](atom::get, const data& key, const data& aspect) -> expected<data> {
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
      BROKER_INFO("GET" << key << "->" << value << "with id:" << id << "->" << x);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::get, atom::name) {
      return self->state.id;
    },
    // --- stream handshake with core ------------------------------------------
    [=](const store::stream_type& in) {
      BROKER_DEBUG("received stream handshake from core");
      self->make_sink(
        // input stream
        in,
        // initialize state
        [](caf::unit_t&) {
          // nop
        },
        // processing step
        [=](caf::unit_t&, store::stream_type::value_type y) {
          self->state.command(y.second);
        },
        // cleanup
        [](caf::unit_t&, const caf::error&) {
          // nop
        }
      );
    }
  };
}

} // namespace detail
} // namespace broker
