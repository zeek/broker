#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/all.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/snapshot.hh"
#include "broker/store.hh"
#include "broker/topic.hh"

#include "broker/detail/appliers.hh"
#include "broker/detail/clone_actor.hh"
#include "broker/detail/filter_type.hh"

#include <chrono>

namespace broker {
namespace detail {

clone_state::clone_state() : self(nullptr) {
  // nop
}

void clone_state::init(caf::event_based_actor* ptr, std::string&& nm,
                       caf::actor&& parent) {

  self = ptr;
  name = std::move(nm);
  master_topic = name / topics::reserved / topics::master;
  core = std::move(parent);
  master = nullptr;
  is_stale = true;
  stale_time = -1.0;
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

void clone_state::operator()(erase_command& x) {
  BROKER_INFO("ERASE" << x.key);
  store.erase(x.key);
}

void clone_state::operator()(add_command& x) {
  BROKER_INFO("ADD" << x.key << "->" << x.value);
  auto i = store.find(x.key);
  if (i == store.end())
    store.emplace(std::move(x.key), std::move(x.value));
  else
    visit(adder{x.value}, i->second);
}

void clone_state::operator()(subtract_command& x) {
  BROKER_INFO("SUBTRACT" << x.key << "->" << x.value);
  auto i = store.find(x.key);
  if (i != store.end()) {
    visit(remover{x.value}, i->second);
  } else {
    // can happen if we joined a stream but did not yet receive set_command
    BROKER_WARNING("received substract_command for unknown key");
  }
}

void clone_state::operator()(snapshot_command&) {
  BROKER_ERROR("received SNAPSHOT");
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

static double now()
  {
  using namespace std::chrono;
  auto d = system_clock::now().time_since_epoch();
  return duration_cast<duration<double>>(d).count();
  }

caf::behavior clone_actor(caf::stateful_actor<clone_state>* self,
                          caf::actor core, std::string name,
                          double resync_interval, double stale_interval) {
  self->monitor(core);
  self->state.init(self, std::move(name), std::move(core));
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      if (msg.source == core) {
        BROKER_INFO("core is down, kill clone as well");
        self->quit(msg.reason);
      } else {
        BROKER_INFO("lost master");
        self->state.master = nullptr;
        self->send(self, atom::master::value, atom::resolve::value);

        if ( stale_interval < 0 )
          return;

        self->state.stale_time = now() + stale_interval;
        auto si = std::chrono::duration<double>(stale_interval);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(si);
        self->delayed_send(self, ms, atom::tick::value);
      }
    }
  );
  self->send(self, atom::master::value, atom::resolve::value);
  return {
    // --- local communication -------------------------------------------------
    [=](atom::local, internal_command& x) {
      // forward all commands to the master
      self->state.forward(std::move(x));
    },
    [=](atom::master, atom::resolve) {
      if ( self->state.master )
        return;

      BROKER_INFO("request master resolve");
      self->send(self->state.core, atom::store::value, atom::master::value,
                 atom::resolve::value, self->state.name, self);
      auto ri = std::chrono::duration<double>(resync_interval);
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(ri);
      self->delayed_send(self, ms, atom::master::value, atom::resolve::value);
    },
    [=](atom::master, caf::actor& master) {
      if ( self->state.master )
        return;

      BROKER_INFO("resolved master");
      self->state.master = std::move(master);
      self->state.is_stale = false;
      self->state.stale_time = -1.0;
      self->monitor(self->state.master);
      self->send(self->state.core, atom::store::value, atom::master::value,
                 atom::snapshot::value, self->state.name);
    },
    [=](atom::master, caf::error err) {
      if ( self->state.master )
        return;

      BROKER_INFO("error resolving master " << caf::to_string(err));
    },
    [=](atom::tick) {
      if ( self->state.stale_time < 0 )
        return;

      // Checking the timestamp is needed in the case there are multiple
      // connects/disconnects within a short period of time (we don't want
      // to go stale too early).
      if ( now() < self->state.stale_time )
        return;

      self->state.is_stale = true;
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
      return caf::make_message(x, id);
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
        result = visit(retriever{aspect}, i->second);
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
        auto x = visit(retriever{aspect}, i->second);
        if (x)
          result = caf::make_message(*x, id);
        else
          result = caf::make_message(std::move(x.error()), id);
      }
      else
        result = caf::make_message(make_error(ec::no_such_key), id);
      BROKER_INFO("GET" << key << aspect << "with id" << id
                  << "->" << result.take(1));
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
          self->state.command(y.second);
        },
        // cleanup and produce result message
        [](caf::unit_t&) {
          // nop
        }
      );
    }
  };
}

} // namespace detail
} // namespace broker

