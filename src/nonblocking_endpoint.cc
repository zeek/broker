#include "broker/logger.hh"

#include <caf/all.hpp>

#include "broker/convert.hh"
#include "broker/nonblocking_endpoint.hh"

#include "broker/detail/core_actor.hh"
#include "broker/detail/radix_tree.hh"

namespace broker {
namespace {

struct subscriber_state {
  caf::actor core;
  optional<caf::actor> status_subscriber;
  detail::radix_tree<caf::actor> data_subscribers;
  const char* name = "nonblocking_subscriber";
};

// This actor manages the data and status subscribes, which are separate actors
// for the non-blocking endpoint.
caf::behavior subscriber(caf::stateful_actor<subscriber_state>* self) {
  auto operating = caf::behavior{
    [=](status& s) {
      if (self->state.status_subscriber)
        self->send(*self->state.status_subscriber, std::move(s));
    },
    [=](atom::subscribe, topic& t, caf::actor& subscriber) {
      BROKER_DEBUG("subscribing actor for topic" << t);
      // Shutdown existing actor, if a subscription exists already.
      auto i = self->state.data_subscribers.find(t.string());
      if (i == self->state.data_subscribers.end()) {
        self->state.data_subscribers.insert({t.string(), subscriber});
      } if (i != self->state.data_subscribers.end()) {
        BROKER_DEBUG("terminating previous subscriber");
        self->send_exit(i->second, caf::exit_reason::user_shutdown);
        i->second = subscriber;
      }
      // Relay subscribe request to core.
      std::vector<topic> ts;
      ts.push_back(std::move(t));
      self->delegate(self->state.core, atom::subscribe::value, std::move(ts),
                     std::move(subscriber));
    },
    [=](atom::unsubscribe, topic& t) {
      BROKER_DEBUG("unsubscribing actor from topic" << t);
      auto i = self->state.data_subscribers.find(t.string());
      if (i == self->state.data_subscribers.end()) {
        BROKER_WARNING("no such topic:" << t);
        return;
      }
      self->send_exit(i->second, caf::exit_reason::user_shutdown);
      // Relay unsubscribe request to core.
      self->delegate(self->state.core, atom::unsubscribe::value, std::move(t),
                     i->second);
    },
    [=](atom::status, const caf::actor& actor) {
      BROKER_DEBUG("registering status actor");
      if (self->state.status_subscriber)
        self->send_exit(*self->state.status_subscriber,
                        caf::exit_reason::user_shutdown);
      self->state.status_subscriber = actor;
      return atom::ok::value;
    }
  };
  return [=](caf::actor& core) {
    self->state.core = std::move(core);
    self->become(operating);
  };
}

} // namespace <anonymous>

void nonblocking_endpoint::unsubscribe(topic t) {
  caf::scoped_actor self{core()->home_system()};
  self->request(subscriber_, timeout::core, atom::unsubscribe::value,
                std::move(t)).receive(
    []() {
      // nop
    },
    [&](const caf::error& e) {
      detail::die("failed to unsubscribe:", to_string(e));
    }
  );
}

nonblocking_endpoint::nonblocking_endpoint(caf::actor_system& sys) {
  subscriber_ = sys.spawn(subscriber);
  init_core(sys.spawn(detail::core_actor, subscriber_));
  caf::anon_send(subscriber_, core());
}

} // namespace broker
