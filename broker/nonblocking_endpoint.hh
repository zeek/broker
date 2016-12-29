#ifndef BROKER_NONBLOCKING_ENDPOINT_HH
#define BROKER_NONBLOCKING_ENDPOINT_HH

#include <type_traits>
#include <vector>

#include <caf/scoped_actor.hpp>

#include "broker/api_flags.hh"
#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"

#include "broker/detail/die.hh"
#include "broker/detail/type_traits.hh"

namespace broker {

/// An endpoint with an asynchronous (nonblocking) messaging API.
class nonblocking_endpoint : public endpoint {
  friend context; // construction

public:
  /// Subscribes to a topic.
  /// @param t The topic to subscribe to.
  /// @param on_msg The callback to invoke for messages prefix-matching *t*.
  template <class OnMessage>
  void subscribe(topic t, OnMessage on_msg) {
    detail::verify_message_callback<OnMessage>();
    auto handler = subscriber_->home_system().spawn(
      [=]() -> caf::behavior {
        return {
          [=](const topic& top, const caf::message& msg, const caf::actor&) {
            on_msg(top, msg.get_as<data>(0));
          }
        };
      }
    );
    caf::scoped_actor self{core()->home_system()};
    self->request(subscriber_, timeout::core, atom::subscribe::value,
                  std::move(t), std::move(handler)).receive(
      []() {
        // nop
      },
      [&](const caf::error& e) {
        detail::die("failed to subscribe to data messages:", to_string(e));
      }
    );
  };

  /// Subscribes to status messages. If a handler exists already, the previous
  /// handler will get removed first, i.e., there will always be at most one
  /// status handler active.
  /// @param on_status The callback to execute for status messages.
  template <class OnStatus>
  void subscribe(OnStatus on_status) {
    detail::verify_status_callback<OnStatus>();
    auto handler = subscriber_->home_system().spawn(
      [=]() -> caf::behavior {
        return on_status;
      }
    );
    caf::scoped_actor self{core()->home_system()};
    self->request(subscriber_, timeout::core, atom::status::value,
                  std::move(handler)).receive(
      [](atom::ok) {
        // nop
      },
      [&](caf::error& e) {
        detail::die("failed to subscribe to status messages");
      }
    );
  }

  /// Unsubscribes from a topic.
  /// @param t The topic to unsubscribe from.
  void unsubscribe(topic t);

private:
  nonblocking_endpoint(caf::actor_system& sys, api_flags flags);
};

} // namespace broker

#endif // BROKER_NONBLOCKING_ENDPOINT_HH
