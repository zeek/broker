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
#include "broker/error.hh"
#include "broker/store.hh"
#include "broker/topic.hh"

#include "broker/detail/appliers.hh"
#include "broker/detail/clone_actor.hh"

#include <chrono>

namespace broker {
namespace detail {

static double now(endpoint::clock* clock) {
  auto d = clock->now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

void clone_state::init(caf::event_based_actor* ptr, std::string&& nm,
                       caf::actor&& parent, endpoint::clock* ep_clock) {
  super::init(ptr, ep_clock, std::move(nm), std::move(parent));
  master_topic = id / topics::master_suffix;
}

void clone_state::forward(internal_command&& x) {
  self->send(core, atom::publish_v,
             make_command_message(master_topic, std::move(x)));
}

void clone_state::command(internal_command::variant_type& cmd) {
  caf::visit(*this, cmd);
}

void clone_state::command(internal_command& cmd) {
  command(cmd.content);
}

void clone_state::operator()(none) {
  BROKER_WARNING("received empty command");
}

void clone_state::operator()(put_command& x) {
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

void clone_state::operator()(put_unique_command& x) {
  BROKER_ERROR("clone received put_unique_command");
}

void clone_state::operator()(erase_command& x) {
  BROKER_INFO("ERASE" << x.key);
  if (store.erase(x.key) != 0)
    emit_erase_event(x.key, x.publisher);
}

void clone_state::operator()(expire_command& x) {
  BROKER_INFO("EXPIRE" << x.key);
  if (store.erase(x.key) != 0)
    emit_expire_event(x.key, x.publisher);
}

void clone_state::operator()(add_command&) {
  BROKER_ERROR("clone received add_command");
}

void clone_state::operator()(subtract_command&) {
  BROKER_ERROR("clone received subtract_command");
}

void clone_state::operator()(snapshot_command&) {
  BROKER_ERROR("received SNAPSHOT in a clone");
}

void clone_state::operator()(snapshot_sync_command& x) {
  if (x.remote_clone == self)
    awaiting_snapshot_sync = false;
}

void clone_state::operator()(set_command& x) {
  BROKER_INFO("SET" << x.state);
  // We consider the master the source of all updates.
  publisher_id publisher{master.node(), master.id()};
  // Short-circuit messages with an empty state.
  if (x.state.empty()) {
    if (!store.empty()) {
      clear_command cmd{publisher};
      (*this)(cmd);
    }
    return;
  }
  if (store.empty()) {
    // Emit insert events.
    for (auto& [key, value] : x.state)
      emit_insert_event(key, value, nil, publisher);
  } else {
    // Emit erase and put events.
    std::vector<const data*> keys;
    keys.reserve(store.size());
    for (auto& kvp : store)
      keys.emplace_back(&kvp.first);
    auto is_erased = [&x](const data* key) { return x.state.count(*key) == 0; };
    auto p = std::partition(keys.begin(), keys.end(), is_erased);
    for (auto i = keys.begin(); i != p; ++i)
      emit_erase_event(**i, publisher_id{});
    for (auto i = p; i != keys.end(); ++i) {
      const auto& value = x.state[**i];
      emit_update_event(**i, store[**i], value, nil, publisher);
    }
    // Emit insert events.
    auto is_new = [&keys](const data& key) {
      for (const auto key_ptr : keys)
        if (*key_ptr == key)
          return false;
      return true;
    };
    for (const auto& [key, value] : x.state)
      if (is_new(key))
        emit_insert_event(key, value, nil, publisher);
  }
  // Override local state.
  store = std::move(x.state);
}

void clone_state::operator()(clear_command& x) {
  BROKER_INFO("CLEAR");
  for (auto& kvp : store)
    emit_erase_event(kvp.first, x.publisher);
  store.clear();
}

data clone_state::keys() const {
  set result;
  for (auto& kvp : store)
    result.emplace(kvp.first);
  return result;
}

caf::behavior clone_actor(caf::stateful_actor<clone_state>* self,
                          caf::actor core, std::string id,
                          double resync_interval, double stale_interval,
                          double mutation_buffer_interval,
                          endpoint::clock* clock) {
  self->monitor(core);
  self->state.init(self, std::move(id), std::move(core), clock);
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      if (msg.source == core) {
        BROKER_INFO("core is down, kill clone as well");
        self->quit(msg.reason);
      } else {
        BROKER_INFO("lost master");
        self->state.master = nullptr;
        self->state.awaiting_snapshot = true;
        self->state.awaiting_snapshot_sync = true;
        self->state.pending_remote_updates.clear();
        self->state.pending_remote_updates.shrink_to_fit();
        self->send(self, atom::master_v, atom::resolve_v);

        if ( stale_interval >= 0 )
          {
          self->state.stale_time = now(clock) + stale_interval;
          auto si = std::chrono::duration<double>(stale_interval);
          auto ts = std::chrono::duration_cast<timespan>(si);
          auto msg = caf::make_message(atom::tick_v,
                                       atom::stale_check_v);
          clock->send_later(self, ts, std::move(msg));
          }

        if ( mutation_buffer_interval > 0 )
          {
          self->state.unmutable_time = now(clock) + mutation_buffer_interval;
          auto si = std::chrono::duration<double>(mutation_buffer_interval);
          auto ts = std::chrono::duration_cast<timespan>(si);
          auto msg = caf::make_message(atom::tick_v,
                                       atom::mutable_check_v);
          clock->send_later(self, ts, std::move(msg));
          }
      }
    }
  );

  if ( mutation_buffer_interval > 0 )
    {
    self->state.unmutable_time = now(clock) + mutation_buffer_interval;
    auto si = std::chrono::duration<double>(mutation_buffer_interval);
    auto ts = std::chrono::duration_cast<timespan>(si);
    auto msg = caf::make_message(atom::tick_v,
                                 atom::mutable_check_v);
    clock->send_later(self, ts, std::move(msg));
    }

  self->send(self, atom::master_v, atom::resolve_v);

  return {
    // --- local communication -------------------------------------------------
    [=](atom::local, internal_command& x) {
      if ( self->state.master )
        {
        // forward all commands to the master
        self->state.forward(std::move(x));
        return;
      }

      if ( mutation_buffer_interval <= 0 )
        return;

      if ( now(clock) >= self->state.unmutable_time )
        return;

      self->state.mutation_buffer.emplace_back(std::move(x));
    },
    [=](set_command& x) {
      self->state(x);
      self->state.awaiting_snapshot = false;

      if ( ! self->state.awaiting_snapshot_sync ) {
        for ( auto& update : self->state.pending_remote_updates )
          self->state.command(update);

        self->state.pending_remote_updates.clear();
        self->state.pending_remote_updates.shrink_to_fit();
      }
    },
    [=](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point_v);
    },
    [=](atom::master, atom::resolve) {
      if ( self->state.master )
        return;

      BROKER_INFO("request master resolve");
      self->send(self->state.core, atom::store_v, atom::master_v,
                 atom::resolve_v, self->state.id, self);
      auto ri = std::chrono::duration<double>(resync_interval);
      auto ts = std::chrono::duration_cast<timespan>(ri);
      auto msg = caf::make_message(atom::master_v, atom::resolve_v);
      clock->send_later(self, ts, std::move(msg));
    },
    [=](atom::master, caf::actor& master) {
      if ( self->state.master )
        return;

      BROKER_INFO("resolved master");
      self->state.master = std::move(master);
      self->state.is_stale = false;
      self->state.stale_time = -1.0;
      self->state.unmutable_time = -1.0;
      self->monitor(self->state.master);

      for ( auto& cmd : self->state.mutation_buffer )
        self->state.forward(std::move(cmd));

      self->state.mutation_buffer.clear();
      self->state.mutation_buffer.shrink_to_fit();

      self->send(self->state.core, atom::store_v, atom::master_v,
                 atom::snapshot_v, self->state.id, self);
    },
    [=](atom::master, caf::error err) {
      if ( self->state.master )
        return;

      BROKER_INFO("error resolving master " << caf::to_string(err));
    },
    [=](atom::tick, atom::stale_check) {
      if ( self->state.stale_time < 0 )
        return;

      // Checking the timestamp is needed in the case there are multiple
      // connects/disconnects within a short period of time (we don't want
      // to go stale too early).
      if ( now(clock) < self->state.stale_time )
        return;

      self->state.is_stale = true;
    },
    [=](atom::tick, atom::mutable_check) {
      if ( self->state.unmutable_time < 0 )
        return;

      if ( now(clock) < self->state.unmutable_time )
        return;

      self->state.mutation_buffer.clear();
      self->state.mutation_buffer.shrink_to_fit();
    },
    [=](atom::get, atom::keys) -> caf::result<data> {
      if ( self->state.is_stale )
        return {ec::stale_data};
      auto x = self->state.keys();
      BROKER_INFO("KEYS ->" << x);
      return {x};
    },
    [=](atom::get, atom::keys, request_id id) {
      if ( self->state.is_stale )
        return caf::make_message(make_error(ec::stale_data), id);

      auto x = self->state.keys();
      BROKER_INFO("KEYS" << "with id" << id << "->" << x);
      return caf::make_message(std::move(x), id);
    },
    [=](atom::exists, const data& key) -> caf::result<data> {
      if (self->state.is_stale)
        return {ec::stale_data};
      auto result = (self->state.store.find(key) != self->state.store.end());
      BROKER_INFO("EXISTS" << key << "->" << result);
      return data{result};
    },
    [=](atom::exists, const data& key, request_id id) {
      if (self->state.is_stale)
        return caf::make_message(make_error(ec::stale_data), id);

      auto r = (self->state.store.find(key) != self->state.store.end());
      auto result = caf::make_message(data{r}, id);
      BROKER_INFO("EXISTS" << key << "with id" << id << "->" << r);
      return result;
    },
    [=](atom::get, const data& key) -> caf::result<data> {
      if (self->state.is_stale)
        return {ec::stale_data};
      expected<data> result = ec::no_such_key;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        result = i->second;
      BROKER_INFO("GET" << key << "->" << result);
      return result;
    },
    [=](atom::get, const data& key, const data& aspect) -> caf::result<data> {
      if (self->state.is_stale)
        return {ec::stale_data};
      expected<data> result = ec::no_such_key;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        result = caf::visit(retriever{aspect}, i->second);
      BROKER_INFO("GET" << key << aspect << "->" << result);
      return result;
    },
    [=](atom::get, const data& key, request_id id) {
      if (self->state.is_stale)
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
      if ( self->state.is_stale )
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
    [=](atom::get, atom::name) { return self->state.id; },
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
        [=](caf::unit_t&, store::stream_type::value_type y) {
          // TODO: our operator() overloads require mutable references, but
          //       only a fraction actually benefit from it.
          auto cmd = move_command(y);
          if (caf::holds_alternative<snapshot_sync_command>(cmd)) {
            self->state.command(cmd);
            return;
          }

          if ( self->state.awaiting_snapshot_sync )
            return;

          if ( self->state.awaiting_snapshot ) {
            self->state.pending_remote_updates.emplace_back(std::move(cmd));
            return;
          }

          self->state.command(cmd);
        });
    }};
}

} // namespace detail
} // namespace broker
