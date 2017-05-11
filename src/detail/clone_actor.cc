#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/all.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/snapshot.hh"
#include "broker/topic.hh"

#include "broker/detail/clone_actor.hh"
#include "broker/detail/appliers.hh"

namespace broker {
namespace detail {

caf::behavior clone_actor(caf::stateful_actor<clone_state>* self,
                          caf::actor core, caf::actor master,
                          std::string name) {
  auto forward = [=](const caf::message& msg) {
    auto t = name / topics::reserved / topics::master;
    self->send(master, std::move(t), msg, core);
  };
  auto relay = caf::message_handler{
    [=](atom::put, data& key, data& value, optional<timespan> expiry) {
      forward(caf::make_message(atom::put::value, std::move(key),
                                std::move(value), expiry));
    },
    [=](atom::add, data& key, data& value, optional<timespan> expiry) {
      forward(caf::make_message(atom::add::value, std::move(key),
                                std::move(value), expiry));
    },
    [=](atom::erase, data& key) {
      forward(caf::make_message(atom::erase::value, std::move(key)));
    },
    [=](atom::clear) {
      forward(caf::make_message(atom::clear::value));
    },
    [=](atom::subtract, data& key, data& value, optional<timespan> expiry) {
      forward(caf::make_message(atom::subtract::value, std::move(key),
                                std::move(value), expiry));
    },
  };
  auto update = caf::message_handler{
    [=](atom::put, data& key, data& value) {
      BROKER_DEBUG("put" << key << "->" << value);
      auto i = self->state.store.find(key);
      if (i != self->state.store.end())
        i->second = std::move(value);
      else
        self->state.store.emplace(std::move(key), std::move(value));
    },
    [=](atom::erase, data& key) {
      BROKER_DEBUG("erase" << key);
      self->state.store.erase(key);
    },
    [=](atom::add, data& key, data& value) {
      BROKER_DEBUG("add" << key << "->" << value);
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        self->state.store.emplace(std::move(key), std::move(value));
      else
        visit(adder{value}, i->second);
    },
    [=](atom::subtract, data& key, data& value) {
      BROKER_DEBUG("subtract" << key << "->" << value);
      auto i = self->state.store.find(key);
      BROKER_ASSERT(i != self->state.store.end());
      visit(remover{value}, i->second);
    },
  };
  auto local = caf::message_handler{
    [=](atom::get, const data& key) -> expected<data> {
      BROKER_DEBUG("GET" << key);
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return ec::no_such_key;
      return i->second;
    },
    [=](atom::get, const data& key, const data& value) -> expected<data> {
      BROKER_DEBUG("GET" << key << "->" << value);
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return ec::no_such_key;
      return visit(retriever{value}, i->second);
    },
    [=](atom::get, const data& key, request_id id) {
      BROKER_DEBUG("GET" << key << "with id:" << id);
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return caf::make_message(make_error(ec::no_such_key), id);
      return caf::make_message(i->second, id);
    },
    [=](atom::get, const data& key, const data& value, request_id id) {
      BROKER_DEBUG("GET" << key << "->" << value << "with id:" << id);
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return caf::make_message(make_error(ec::no_such_key), id);
      auto x = visit(retriever{value}, i->second);
      if (x)
        return caf::make_message(std::move(*x), id);
      return caf::make_message(std::move(x.error()), id);
    },
    [=](atom::get, atom::name) {
      return name;
    },
    [=](atom::keys) -> expected<data> {
      BROKER_DEBUG("KEYS");
      set keys;
      for ( auto i = self->state.store.begin(); i != self->state.store.end(); i++ )
        keys.insert(i->first);
      return keys;
    },
    [=](atom::keys, request_id id) {
      BROKER_DEBUG("KEYS" << "with id:" << id);
      set keys;
      for ( auto i = self->state.store.begin(); i != self->state.store.end(); i++ )
        keys.insert(i->first);
      return caf::make_message(std::move(keys), id);
    }
  };
  auto dispatch = caf::message_handler{
    [=](topic& t, caf::message& msg, const caf::actor& source) mutable {
      BROKER_DEBUG("dispatching message with topic" << t << "from core"
                   << to_string(source));
      update(msg);
    }
  };
  auto direct = caf::message_handler{
    [=](snapshot& ss) {
      BROKER_DEBUG("got snapshot with" << ss.entries.size() << "entries");
      self->state.store = ss.entries;
    },
  };
  return dispatch.or_else(relay).or_else(update).or_else(local).or_else(direct);
}

} // namespace detail
} // namespace broker

