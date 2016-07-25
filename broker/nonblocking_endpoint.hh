#ifndef BROKER_NONBLOCKING_ENDPOINT_HH
#define BROKER_NONBLOCKING_ENDPOINT_HH

#include <type_traits>
#include <vector>

#include <caf/scoped_actor.hpp>
#include <caf/detail/type_traits.hpp>

#include "broker/atoms.hh"
#include "broker/endpoint.hh"
#include "broker/message.hh"
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
  /// @param on_data The callback to invoke for messages prefix-matching *t*.
  template <class OnData>
  void subscribe(topic t, OnData on_data) {
    using on_data_type = caf::detail::get_callable_trait<OnData>;
    using arg_types = typename on_data_type::arg_types;
    using first = typename caf::detail::tl_head<arg_types>::type;
    //using second = typename caf::detail::tl_tail<arg_types>::type;
    static_assert(std::is_same<void, typename on_data_type::result_type>{},
                  "data callback must not have a return value");
    static_assert(caf::detail::tl_size<arg_types>::value == 2,
                  "data callback must have two arguments");
    static_assert(std::is_same<detail::decay_t<first>, topic>::value,
                  "first argument must be of type broker::topic");
    //static_assert(std::is_same<detail::decay_t<second>, data>::value,
    //              "second argument must be of type broker::data");
    auto actor = subscriber_->home_system().spawn(
      [=]() -> caf::behavior {
        return [=](const topic& t, const message& msg, const caf::actor&) {
          on_data(t, msg);
        };
      }
    );
    caf::scoped_actor self{core()->home_system()};
    self->request(subscriber_, timeout::core, atom::subscribe::value,
                  std::move(t), std::move(actor)).receive(
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
    using on_data_type = caf::detail::get_callable_trait<OnStatus>;
    static_assert(std::is_same<void, typename on_data_type::result_type>{},
                  "status callback must not have a return value");
    using on_status_args = typename on_data_type::arg_types;
    static_assert(caf::detail::tl_size<on_status_args>::value == 1,
                  "status callback can have only one argument");
    using on_status_args = typename caf::detail::tl_head<on_status_args>::type;
    static_assert(std::is_same<detail::decay_t<on_status_args>, status>::value,
                  "status callback must have broker::status as argument type");
    auto actor = subscriber_->home_system().spawn(
      [=]() -> caf::behavior { return on_status; }
    );
    caf::scoped_actor self{core()->home_system()};
    self->request(subscriber_, timeout::core, atom::status::value,
                  std::move(actor)).receive(
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
  nonblocking_endpoint(caf::actor_system& sys);
};

} // namespace broker

#endif // BROKER_NONBLOCKING_ENDPOINT_HH
