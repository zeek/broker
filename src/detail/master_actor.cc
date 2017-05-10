#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/all.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/topic.hh"
#include "broker/snapshot.hh"
#include "broker/time.hh"

#include "broker/detail/abstract_backend.hh"
#include "broker/detail/aliases.hh"
#include "broker/detail/die.hh"
#include "broker/detail/master_actor.hh"
#include "broker/detail/type_traits.hh"

namespace broker {
namespace detail {

master_state::master_state() : self(nullptr) {
  // nop
}

void master_state::init(caf::event_based_actor* ptr, std::string&& nm,
                        backend_pointer&& bp, caf::actor&& parent) {

  self = ptr;
  name = std::move(nm);
  clones_topic = name / topics::reserved / topics::clone;
  backend = std::move(bp);
  core = std::move(parent);
}

void master_state::broadcast(data&& x) {
  self->send(core, clones_topic, std::move(x));
}

void master_state::remind(timespan expiry, const data& key) {
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(expiry);
  self->delayed_send(self, us, atom::expire::value, key);
}

void master_state::expire(data& key) {
  BROKER_DEBUG("expiring key" << key);
  auto result = backend->expire(key);
  if (!result)
    BROKER_ERROR("failed to expire key:" << to_string(result.error()));
  else if (!*result)
    BROKER_WARNING("ignoring stale expiration reminder");
  else {
    erase_command cmd{std::move(key)};
    broadcast_from(cmd);
  }
}

void master_state::command(internal_command& cmd) {
  visit(*this, cmd.exclusive().xs);
}

void master_state::operator()(none) {
  BROKER_DEBUG("received empty command");
}

void master_state::operator()(detail::put_command& x) {
  BROKER_DEBUG("put" << x.key << "->" << x.value);
  auto result = backend->put(x.key, x.value, x.expiry);
  if (!result) {
    BROKER_WARNING("failed to put" << x.key << "->" << x.value);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (x.expiry)
    remind(*x.expiry, x.key);
  broadcast_from(x);
}

void master_state::operator()(detail::erase_command& x) {
  BROKER_DEBUG("erase" << x.key);
  auto result = backend->erase(x.key);
  if (!result) {
    BROKER_WARNING("failed to erase" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  broadcast_from(x);
}

void master_state::operator()(detail::add_command& x) {
  BROKER_DEBUG("add" << x.key);
  auto result = backend->add(x.key, x.value, x.expiry);
  if (!result) {
    BROKER_WARNING("failed to add" << x.value << "to" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (x.expiry)
    remind(*x.expiry, x.key);
  broadcast_from(x);
}

void master_state::operator()(detail::subtract_command& x) {
  BROKER_DEBUG("subtract" << x.key);
  auto result = backend->subtract(x.key, x.value, x.expiry);
  if (!result) {
    BROKER_WARNING("failed to add" << x.value << "to" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (x.expiry)
    remind(*x.expiry, x.key);
  broadcast_from(x);
}

void master_state::operator()(detail::snapshot_command& x) {
  BROKER_DEBUG("got snapshot request from" << to_string(x.clone));
  auto ss = backend->snapshot();
  if (!ss)
    die("failed to snapshot master");
  self->send(x.clone, std::move(*ss));
  self->monitor(x.clone);
  clones.insert(x.clone->address());
}

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string name,
                           master_state::backend_pointer backend) {
  self->state.init(self, std::move(name), std::move(backend), std::move(core));
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      BROKER_DEBUG("lost connection to clone" << to_string(msg.source));
      self->state.clones.erase(msg.source);
    }
  );
  return {
    // --- stream handshake with core ------------------------------------------
    [=](const stream_type& in) {
      self->add_sink(
        // input stream
        in,
        // initialize state
        [](caf::unit_t&) {
          // nop
        },
        // processing step
        [=](caf::unit_t&, element_type y) {
          auto ptr = get_if<internal_command>(y.second);
          if (ptr) {
            self->state.command(*ptr);
          } else {
            BROKER_DEBUG("received non-command message:" << y);
          }
        },
        // cleanup and produce result message
        [](caf::unit_t&) {
          // nop
        }
      );
    },
    // --- local communication -------------------------------------------------
    [=](atom::expire, data& key) {
      self->state.expire(key);
    },
    /* TODO: reimplement
    [=](atom::clear) {
      BROKER_DEBUG("clear");
      auto result = self->state.backend->clear();
      if (!result) {
        BROKER_WARNING("failed to clear");
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::clear::value));
    },
    */
    [=](put_command& x) {
      self->state(x);
    },
    [=](erase_command& x) {
      self->state(x);
    },
    [=](add_command& x) {
      self->state(x);
    },
    [=](subtract_command& x) {
      self->state(x);
    },
    [=](snapshot_command& x) {
      self->state(x);
    },
    [=](atom::get, const data& key) -> expected<data> {
      BROKER_DEBUG("GET" << key);
      return self->state.backend->get(key);
    },
    [=](atom::get, const data& key, const data& value) -> expected<data> {
      BROKER_DEBUG("GET" << key << "->" << value);
      return self->state.backend->get(key, value);
    },
    [=](atom::get, const data& key, request_id id) {
      BROKER_DEBUG("GET" << key << "with id:" << id);
      auto x = self->state.backend->get(key);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::get, const data& key, const data& value, request_id id) {
      BROKER_DEBUG("GET" << key << "->" << value << "with id:" << id);
      auto x = self->state.backend->get(key, value);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::get, atom::name) {
      return self->state.name;
    },
    [=](atom::keys) -> expected<data> {
      BROKER_DEBUG("KEYS");
      return self->state.backend->keys();
    },
    [=](atom::keys, request_id id) {
      BROKER_DEBUG("KEYS" << "with id:" << id);
      auto x = self->state.backend->keys();
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
  };
}

} // namespace detail
} // namespace broker
