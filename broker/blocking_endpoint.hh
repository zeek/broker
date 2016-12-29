#ifndef BROKER_BLOCKING_ENDPOINT_HH
#define BROKER_BLOCKING_ENDPOINT_HH

#include <caf/blocking_actor.hpp>

#include "broker/api_flags.hh"
#include "broker/endpoint.hh"
#include "broker/message.hh"
#include "broker/status.hh"

#include "broker/detail/type_traits.hh"

namespace broker {

namespace detail {
class flare_actor;
} // namsespace detail

/// A proxy object that represents the mailbox of a blocking endpoint.
struct mailbox {
  friend blocking_endpoint; // construction

public:
  /// Retrieves a descriptor that indicates whether a message can be received
  /// without blocking.
  int descriptor();

  /// Checks whether the mailbox is empty.
  bool empty();

  /// Counts the number of messages in the mailbox, up to given maximum
  /// @warn This is not a constant-time operations, hence the name `count`
  ///       as opposed to `size`. The function takes time *O(n)* where *n*
  ///       is the size of the mailbox.
  size_t count(size_t max = std::numeric_limits<size_t>::max());

private:
  explicit mailbox(detail::flare_actor* actor);

  detail::flare_actor* actor_;
};

/// An endpoint with a synchronous (blocking) messaging API.
class blocking_endpoint : public endpoint {
  friend context; // construction

public:
  /// Subscribes to a topic.
  /// @param t The topic to subscribe to.
  void subscribe(topic t);

  /// Unsubscribes from a topic.
  /// @param t The topic to unsubscribe from.
  void unsubscribe(topic t);

  /// Consumes the next data message blocks until one arrives.
  /// @returns The next data message in the mailbox.
  template <class T>
  detail::enable_if_t<std::is_same<T, message>::value, T>
  receive() {
    caf::message msg;
    auto subscriber = caf::actor_cast<caf::blocking_actor*>(subscriber_);
    subscriber->receive(
      [&](const topic&, const caf::message&, const caf::actor&) mutable {
        msg = subscriber->current_mailbox_element()->move_content_to_message();
      }
    );
    return message{msg};
  }

  /// Consumes the next ::status message or blocks until one arrives.
  /// @returns The next status message in the mailbox.
  template <class T>
  detail::enable_if_t<std::is_same<T, status>::value, T>
  receive() {
    status result;
    caf::actor_cast<caf::blocking_actor*>(subscriber_)->receive(
      [&](const status& s) mutable {
        result = s;
      }
    );
    return result;
  }

  /// Consumes one message that matches the given handler.
  template <class F>
  detail::enable_if_t<detail::is_message_callback<F>::value>
  receive(F f) {
    auto subscriber = caf::actor_cast<caf::blocking_actor*>(subscriber_);
    subscriber->receive(
      [&](const topic& t, const caf::message& msg, const caf::actor&) {
        f(t, msg.get_as<data>(0));
      }
    );
  }

  /// Consumes one message that matches the given handler.
  template <class F>
  detail::enable_if_t<detail::is_status_callback<F>::value>
  receive(F f) {
    auto subscriber = caf::actor_cast<caf::blocking_actor*>(subscriber_);
    subscriber->receive(f);
  }

  /// Consumes one message that matches the given handler.
  template <class OnMessage, class OnStatus>
  void receive(OnMessage on_msg, OnStatus on_status) {
    detail::verify_message_callback<OnMessage>();
    detail::verify_status_callback<OnStatus>();
    auto subscriber = caf::actor_cast<caf::blocking_actor*>(subscriber_);
    subscriber->receive(
      [&](const topic& t, const caf::message& msg, const caf::actor&) {
        on_msg(t, msg.get_as<data>(0));
      },
      on_status
    );
  }

  /// Access the endpoint's mailbox, which provides the following
  /// introspection functions:
  ///
  /// - `int descriptor()`: Retrieves a descriptor that indicates whether a
  ///   message can be received without blocking.
  ///
  /// - `bool empty()`: Checks whether the endpoint's message mailbox is empty.
  ///
  /// - `size_t count(size_t max)`: Counts the number of messages in the
  ///   mailbox in time that is a linear function of the mailbox size. The
  ///   parameter `max` allows for specifying an upper bound when to stop
  ///   counting.
  ///
  /// @returns A proxy object to introspect the endpoint's mailbox.
  broker::mailbox mailbox();

private:
  blocking_endpoint(caf::actor_system& sys, api_flags flags);
};

} // namespace broker

#endif // BROKER_BLOCKING_ENDPOINT_HH
