#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/all.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/topic.hh"
#include "broker/snapshot.hh"
#include "broker/time.hh"

#include "broker/detail/abstract_backend.hh"
#include "broker/detail/die.hh"
#include "broker/detail/master_actor.hh"
#include "broker/detail/type_traits.hh"

namespace broker {
namespace detail {

// TODO: The following aspects still need to be thought through:
// - Error handling when asynchronous operations fail.

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string name,
                           std::unique_ptr<abstract_backend> backend) {
  self->state.backend = std::move(backend);
  auto broadcast = [=](caf::message&& msg) {
    auto t = name / topics::reserved / topics::clone;
    self->send(core, std::move(t), std::move(msg), core);
  };
  auto remind = [=](time::point expiry, const data& key) {
    auto delta = expiry - time::now();
    BROKER_ASSERT(delta > time::duration::zero());
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(delta);
    self->delayed_send(self, us, atom::expire::value, key);
  };
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      BROKER_DEBUG("lost connection to clone" << to_string(msg.source));
      self->state.clones.erase(msg.source);
    }
  );
  auto commands = caf::message_handler{
    [=](atom::put, data& key, data& value, optional<time::point> expiry) {
      BROKER_DEBUG("put" << key << "->" << value);
      auto result = self->state.backend->put(key, value, expiry);
      if (!result) {
        BROKER_WARNING("failed to put" << key << "->" << value);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (expiry)
        remind(*expiry, key);
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::put::value, std::move(key),
                                    std::move(value)));
    },
    [=](atom::add, data& key, data& value, optional<time::point> expiry) {
      BROKER_DEBUG("add" << key);
      auto result = self->state.backend->add(key, value, expiry);
      if (!result) {
        BROKER_WARNING("failed to add" << value << "to" << key);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (expiry)
        remind(*expiry, key);
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::add::value, std::move(key),
                                    std::move(value)));
    },
    [=](atom::remove, data& key, data& value, optional<time::point> expiry) {
      BROKER_DEBUG("remove" << key);
      auto result = self->state.backend->remove(key, value, expiry);
      if (!result) {
        BROKER_WARNING("failed to add" << value << "to" << key);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (expiry)
        remind(*expiry, key);
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::remove::value, std::move(key),
                                    std::move(value)));
    },
    [=](atom::erase, data& key) {
      BROKER_DEBUG("erase" << key);
      auto result = self->state.backend->erase(key);
      if (!result) {
        BROKER_WARNING("failed to erase" << key);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::erase::value, std::move(key)));
    },
    [=](atom::snapshot, const caf::actor& clone) {
      BROKER_DEBUG("got snapshot request from" << to_string(clone));
      auto ss = self->state.backend->snapshot();
      if (!ss)
        die("failed to snapshot master");
      self->send(clone, std::move(*ss));
      self->monitor(clone);
      self->state.clones.insert(clone->address());
    },
  };
  auto dispatch = caf::message_handler{
    [=](topic& t, caf::message& msg, const caf::actor& source) mutable {
      BROKER_DEBUG("dispatching message with topic" << t << "from core"
                   << to_string(source));
      commands(msg);
    }
  };
  auto expiration = caf::message_handler{
    [=](atom::expire, data& key) {
      BROKER_DEBUG("expiring key" << key);
      auto result = self->state.backend->expire(key);
      if (!result)
        BROKER_ERROR("failed to expire key:" << to_string(result.error()));
      else if (!*result)
        BROKER_WARNING("ignoring stale expiration reminder");
      else if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::erase::value, std::move(key)));
    }
  };
  auto query = caf::message_handler{
    [=](atom::get, const data& key) -> expected<data> {
      BROKER_DEBUG("GET" << key);
      return self->state.backend->get(key);
    },
    [=](atom::get, const data& key, const data& value) -> expected<data> {
      BROKER_DEBUG("GET" << key << "->" << value);
      return self->state.backend->get(key, value);
    },
    [=](atom::get, atom::name) {
      return name;
    },
  };
  return dispatch.or_else(expiration).or_else(commands).or_else(query);
}

} // namespace detail
} // namespace broker
