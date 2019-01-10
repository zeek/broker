#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/event_based_actor.hpp>
#include <caf/actor.hpp>
#include <caf/error.hpp>
#include <caf/message.hpp>
#include <caf/unit.hpp>
#include <caf/sum_type.hpp>
#include <caf/behavior.hpp>
#include <caf/make_message.hpp>
#include <caf/system_messages.hpp>
#include <caf/stateful_actor.hpp>

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

static double now(endpoint::clock* clock)
  {
  auto d = clock->now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
  }

clone_state::clone_state() : self(nullptr), name(), master_topic(), core(),
  master(), store(), is_stale(), stale_time(), unmutable_time(),
  mutation_buffer(), pending_remote_updates(), awaiting_snapshot(),
  awaiting_snapshot_sync(), clock() {
  // nop
}

void clone_state::init(caf::event_based_actor* ptr, std::string&& nm,
                       caf::actor&& parent, endpoint::clock* ep_clock) {

  self = ptr;
  name = std::move(nm);
  master_topic = name / topics::master_suffix;
  core = std::move(parent);
  master = nullptr;
  is_stale = true;
  stale_time = -1.0;
  unmutable_time = -1.0;
  clock = ep_clock;
  awaiting_snapshot = true;
  awaiting_snapshot_sync = true;
}

void clone_state::forward(internal_command&& x) {
  self->send(core, atom::publish::value, master_topic, std::move(x));
}

void clone_state::command(internal_command& cmd) {
  caf::visit(*this, cmd.content);
}

void clone_state::operator()(none) {
  BROKER_WARNING("received empty command");
}

void clone_state::operator()(put_command& x) {
  BROKER_INFO("PUT" << x.key << "->" << x.value << "with expiry" << x.expiry);
  auto i = store.find(x.key);
  if (i != store.end())
    i->second = std::move(x.value);
  else
    store.emplace(std::move(x.key), std::move(x.value));
}

void clone_state::operator()(put_unique_command& x) {
  BROKER_INFO("PUT_UNIQUE" << x.key << "->" << x.value << "with expiry" << x.expiry);
  store.emplace(std::move(x.key), std::move(x.value));
}

void clone_state::operator()(erase_command& x) {
  BROKER_INFO("ERASE" << x.key);
  store.erase(x.key);
}

void clone_state::operator()(add_command& x) {
  BROKER_INFO("ADD" << x.key << "->" << x.value);
  auto i = store.find(x.key);
  if (i == store.end())
    i = store.emplace(std::move(x.key), data::from_type(x.init_type)).first;
  caf::visit(adder{x.value}, i->second);
}

void clone_state::operator()(subtract_command& x) {
  BROKER_INFO("SUBTRACT" << x.key << "->" << x.value);
  auto i = store.find(x.key);
  if (i != store.end()) {
    caf::visit(remover{x.value}, i->second);
  } else {
    // can happen if we joined a stream but did not yet receive set_command
    BROKER_WARNING("received substract_command for unknown key");
  }
}

void clone_state::operator()(snapshot_command&) {
  BROKER_ERROR("received SNAPSHOT");
}

void clone_state::operator()(snapshot_sync_command& x) {
  if ( x.remote_clone == self ) {
    awaiting_snapshot_sync = false;
  }
}

void clone_state::operator()(set_command& x) {
  BROKER_INFO("SET" << x.state);
  store = std::move(x.state);
}

void clone_state::operator()(clear_command&) {
  BROKER_INFO("CLEAR");
  store.clear();
}

data clone_state::keys() const {
  set result;
  for (auto& kvp : store)
    result.emplace(kvp.first);
  return result;
}

caf::behavior clone_actor(caf::stateful_actor<clone_state>* self,
                          caf::actor core, std::string name,
                          double resync_interval, double stale_interval,
                          double mutation_buffer_interval,
                          endpoint::clock* clock) {
  self->monitor(core);
  self->state.init(self, std::move(name), std::move(core), clock);
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
        self->send(self, atom::master::value, atom::resolve::value);

        if ( stale_interval >= 0 )
          {
          self->state.stale_time = now(clock) + stale_interval;
          auto si = std::chrono::duration<double>(stale_interval);
          auto ts = std::chrono::duration_cast<timespan>(si);
          auto msg = caf::make_message(atom::tick::value,
                                       atom::stale_check::value);
          clock->send_later(self, ts, std::move(msg));
          }

        if ( mutation_buffer_interval > 0 )
          {
          self->state.unmutable_time = now(clock) + mutation_buffer_interval;
          auto si = std::chrono::duration<double>(mutation_buffer_interval);
          auto ts = std::chrono::duration_cast<timespan>(si);
          auto msg = caf::make_message(atom::tick::value,
                                       atom::mutable_check::value);
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
    auto msg = caf::make_message(atom::tick::value,
                                 atom::mutable_check::value);
    clock->send_later(self, ts, std::move(msg));
    }

  self->send(self, atom::master::value, atom::resolve::value);

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
      self->state.store = std::move(x.state);
      self->state.awaiting_snapshot = false;

      if ( ! self->state.awaiting_snapshot_sync ) {
        for ( auto& update : self->state.pending_remote_updates )
          self->state.command(update);

        self->state.pending_remote_updates.clear();
        self->state.pending_remote_updates.shrink_to_fit();
      }
    },
    [=](atom::sync_point, caf::actor& who) {
      self->send(who, atom::sync_point::value);
    },
    [=](atom::master, atom::resolve) {
      if ( self->state.master )
        return;

      BROKER_INFO("request master resolve");
      self->send(self->state.core, atom::store::value, atom::master::value,
                 atom::resolve::value, self->state.name, self);
      auto ri = std::chrono::duration<double>(resync_interval);
      auto ts = std::chrono::duration_cast<timespan>(ri);
      auto msg = caf::make_message(atom::master::value, atom::resolve::value);
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

      self->send(self->state.core, atom::store::value, atom::master::value,
                 atom::snapshot::value, self->state.name, self);
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
    [=](atom::get, atom::keys) -> expected<data> {
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
    [=](atom::exists, const data& key) -> expected<data> {
      if ( self->state.is_stale )
        return {ec::stale_data};

      auto result = (self->state.store.find(key) != self->state.store.end());
      BROKER_INFO("EXISTS" << key << "->" << result);
      return {result};
    },
    [=](atom::exists, const data& key, request_id id) {
      if ( self->state.is_stale )
        return caf::make_message(make_error(ec::stale_data), id);

      auto r = (self->state.store.find(key) != self->state.store.end());
      auto result = caf::make_message(data{r}, id);
      BROKER_INFO("EXISTS" << key << "with id" << id << "->" << result.take(1));
      return result;
    },
    [=](atom::get, const data& key) -> expected<data> {
      if ( self->state.is_stale )
        return {ec::stale_data};

      expected<data> result = ec::no_such_key;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        result = i->second;
      BROKER_INFO("GET" << key << "->" << result);
      return result;
    },
    [=](atom::get, const data& key, const data& aspect) -> expected<data> {
      if ( self->state.is_stale )
        return {ec::stale_data};

      expected<data> result = ec::no_such_key;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        result = caf::visit(retriever{aspect}, i->second);
      BROKER_INFO("GET" << key << aspect << "->" << result);
      return result;
    },
    [=](atom::get, const data& key, request_id id) {
      if ( self->state.is_stale )
        return caf::make_message(make_error(ec::stale_data), id);

      caf::message result;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        result = caf::make_message(i->second, id);
      else
        result = caf::make_message(make_error(ec::no_such_key), id);
      BROKER_INFO("GET" << key << "with id" << id << "->" << result.take(1));
      return result;
    },
    [=](atom::get, const data& key, const data& aspect, request_id id) {
      if ( self->state.is_stale )
        return caf::make_message(make_error(ec::stale_data), id);

      caf::message result;
      auto i = self->state.store.find(key);
      if (i != self->state.store.end()) {
        auto x = caf::visit(retriever{aspect}, i->second);
        if (x)
          result = caf::make_message(*x, id);
        else
          result = caf::make_message(std::move(x.error()), id);
      }
      else
        result = caf::make_message(make_error(ec::no_such_key), id);
      BROKER_INFO("GET" << key << aspect << "with id" << id << "->" << result.take(1));
      return result;
    },
    [=](atom::get, atom::name) {
      return self->state.name;
    },
    // --- stream handshake with core ------------------------------------------
    [=](const store::stream_type& in) {
      self->make_sink(
        // input stream
        in,
        // initialize state
        [](caf::unit_t&) {
          // nop
        },
        // processing step
        [=](caf::unit_t&, store::stream_type::value_type y) {
          if ( y.second.content.is<snapshot_sync_command>() ) {
            self->state.command(y.second);
            return;
          }

          if ( self->state.awaiting_snapshot_sync )
            return;

          if ( self->state.awaiting_snapshot ) {
            self->state.pending_remote_updates.emplace_back(std::move(y.second));
            return;
          }

          self->state.command(y.second);
        }
      );
    }
  };
}

} // namespace detail
} // namespace broker

