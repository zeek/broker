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

const char* master_state::name = "master_actor";

master_state::master_state() : self(nullptr) {
  // nop
}

void master_state::init(caf::event_based_actor* ptr, std::string&& nm,
                        backend_pointer&& bp, caf::actor&& parent) {

  self = ptr;
  id = std::move(nm);
  clones_topic = id / topics::reserved / topics::clone;
  backend = std::move(bp);
  core = std::move(parent);

  auto es = backend->expiries();

  if (!es)
    die("failed to get master expiries while initializing");

  for (auto& e : *es) {
    auto& key = e.first;
    auto& expire_time = e.second;
    auto n = broker::now();
    auto dur = expire_time - n;
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(dur);
    self->delayed_send(self, us, atom::expire::value, std::move(key));
  }
}

void master_state::broadcast(internal_command&& x) {
  self->send(core, atom::publish::value, clones_topic, std::move(x));
}

void master_state::remind(timespan expiry, const data& key) {
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(expiry);
  self->delayed_send(self, us, atom::expire::value, key);
}

void master_state::expire(data& key) {
  BROKER_INFO("EXPIRE" << key);
  auto result = backend->expire(key);
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
  auto result = backend->put(x.key, x.value, x.expiry);
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
  auto result = backend->put(x.key, x.value, x.expiry);

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
  auto result = backend->add(x.key, x.value, x.expiry);
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
  auto result = backend->subtract(x.key, x.value, x.expiry);
  if (!result) {
    BROKER_WARNING("failed to substract" << x.value << "from" << x.key);
    return; // TODO: propagate failure? to all clones? as status msg?
  }
  if (x.expiry)
    remind(*x.expiry, x.key);
  broadcast_cmd_to_clones(std::move(x));
}

void master_state::operator()(snapshot_command& x) {
  BROKER_INFO("SNAPSHOT from" << to_string(x.clone));
  if (x.clone == nullptr) {
    BROKER_INFO("snapshot command with invalid address received");
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
  broadcast_cmd_to_clones(set_command{std::move(*ss)});
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
                           master_state::backend_pointer backend) {
  self->monitor(core);
  self->state.init(self, std::move(id), std::move(backend), std::move(core));
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
