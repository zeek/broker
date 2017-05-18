#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/all.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/snapshot.hh"
#include "broker/store.hh"
#include "broker/time.hh"
#include "broker/topic.hh"

#include "broker/detail/abstract_backend.hh"
#include "broker/detail/die.hh"
#include "broker/detail/filter_type.hh"
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

void master_state::broadcast(internal_command&& x) {
  self->send(core, atom::publish::value, clones_topic, std::move(x));
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
  caf::visit(*this, cmd.content);
}

void master_state::operator()(none) {
  BROKER_DEBUG("received empty command");
}

void master_state::operator()(put_command& x) {
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

void master_state::operator()(erase_command& x) {
  BROKER_DEBUG("erase" << x.key);
  auto result = backend->erase(x.key);
  if (!result) {
    BROKER_WARNING("failed to erase" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  broadcast_from(x);
}

void master_state::operator()(add_command& x) {
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

void master_state::operator()(subtract_command& x) {
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

void master_state::operator()(snapshot_command& x) {
  BROKER_DEBUG("got snapshot request from" << to_string(x.clone));
  if (x.clone == nullptr) {
    BROKER_DEBUG("snapshot command with invalid address received");
    return;
  }
  auto ss = backend->snapshot();
  if (!ss)
    die("failed to snapshot master");
  self->monitor(x.clone);
  clones.insert(x.clone->address());
  // TODO: While sending the full state to all replicas is wasteful, it is the
  //       only way to make sure new replicas reach a consistent state. This is
  //       because sending an asynchronous message here uses a separate channel
  //       to clone. Since ordering is of upmost importance in our protocol, we
  //       cannot have new commands arrive before the snapshot or vice versa. A
  //       network-friendlier protocol would require to annotate each command
  //       with a version in order to allow clones to figure out the correct
  //       ordering of events.
  set_command cmd{std::move(*ss)};
  broadcast_from(cmd);
}

void master_state::operator()(set_command& x) {
  BROKER_ERROR("received a set_command in master actor");
}

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string name,
                           master_state::backend_pointer backend) {
  self->monitor(core);
  self->state.init(self, std::move(name), std::move(backend), std::move(core));
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      if (msg.source == core) {
        BROKER_DEBUG("core is down, kill master as well");
        self->quit(msg.reason);
      } else {
        BROKER_DEBUG("lost a clone");
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
    */
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
    // --- stream handshake with core ------------------------------------------
    [=](const store::stream_type& in) {
      self->add_sink(
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
