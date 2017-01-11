#ifndef BROKER_STORE_HH
#define BROKER_STORE_HH

#include <string>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/detail/type_traits.hpp>

#include "broker/api_flags.hh"
#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/expected.hh"
#include "broker/fwd.hh"
#include "broker/mailbox.hh"
#include "broker/optional.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"

namespace broker {

class endpoint;

/// A key-value store (either a *master* or *clone*) that supports modifying
/// and querying contents.
class store {
  friend endpoint; // construction

public:
  template <class T>
  class continuation {
  public:
    template <class... Ts>
    continuation(caf::actor requester, Ts&&... xs)
      : frontend_{std::move(requester)},
        msg_{caf::make_message(std::forward<Ts>(xs)...)} {
    }

    template <class OnData, class OnStatus>
    void then(OnData on_data, OnStatus on_status) {
      using on_data_type = caf::detail::get_callable_trait<OnData>;
      using on_status_type = caf::detail::get_callable_trait<OnStatus>;
      static_assert(std::is_same<void, typename on_data_type::result_type>{},
                    "data callback must not have a return value");
      static_assert(std::is_same<void, typename on_status_type::result_type>{},
                    "error callback must not have a return value");
      using on_data_args = typename on_data_type::arg_types;
      using on_status_args = typename on_status_type::arg_types;
      static_assert(caf::detail::tl_size<on_data_args>::value == 1,
                    "data callback can have only one argument");
      static_assert(caf::detail::tl_size<on_status_args>::value == 1,
                    "error callback can have only one argument");
      using on_data_arg = typename caf::detail::tl_head<on_data_args>::type;
      using on_status_arg = typename caf::detail::tl_head<on_status_args>::type;
      static_assert(std::is_same<detail::decay_t<on_data_arg>, T>::value,
                    "data callback must have valid type for operation");
      static_assert(std::is_same<detail::decay_t<on_status_arg>, status>::value,
                    "error callback must have broker::status as argument type");
      // Explicitly capture *this members. Initialized lambdas are C++14. :-/
      auto frontend = frontend_;
      auto msg = msg_;
      frontend_->home_system().spawn(
        [=](caf::event_based_actor* self) {
          self->request(frontend, timeout::frontend, std::move(msg)).then(
            on_data,
            [=](caf::error& e) {
              if (e == caf::sec::request_timeout) {
                auto desc = "store operation timed out";
                on_status(make_status<sc::request_timeout>(desc));
              } else if (e.category() == caf::atom("broker")) {
                on_status(make_status(std::move(e)));
              } else {
                auto desc = self->system().render(e);
                on_status(make_status<sc::unspecified>(std::move(desc)));
              }
            }
          );
        }
      );
    }

  private:
    caf::actor frontend_;
    caf::message msg_;
  };

  class proxy;

  /// A response to a lookup request issued by a ::proxy.
  class response {
    friend proxy; // construction

  public:
    /// Default-constructs an (invalid) response.
    response();

    /// Checks whether a response contains data or a status.
    explicit operator bool() const;

    /// @pre `static_cast<bool>(*this)`
    data& operator*();

    /// @pre `static_cast<bool>(*this)`
    data* operator->();

    /// @pre `!*this`
    broker::status status();

    /// @returns the ID corresponding to the request.
    request_id id() const;

  private:
    expected<data> data_;
    request_id id_ = 0;
  };

  /// A utility to decouple store lookups and corresponding response
  /// processing.
  class proxy {
  public:
    /// Constructs a proxy for a given store.
    /// @param s The store to create a proxy for.
    proxy(store& s);

    /// Performs a request to retrieve a value.
    /// @param key The key of the value to retrieve.
    /// @returns A unique identifier for this request that is also present in
    ///          the corresponding response.
    request_id get(data key);

    /// Retrieves the proxy's mailbox that reflects query responses.
    broker::mailbox mailbox();

    /// Consumes the next response or blocks until one arrives.
    /// @returns The next response in the proxy's mailbox.
    response receive();

  private:
    request_id id_ = 0;
    caf::actor frontend_;
    caf::actor proxy_;
  };

  // --- inspectors -----------------------------------------------------------

  /// Retrieves the name of the store.
  /// @returns The store name.
  std::string name() const;

  /// Retrieves a value.
  /// @param key The key of the value to retrieve.
  /// @returns The value under *key* or an error.
  template <api_flags Flags = blocking>
  detail::enable_if_t<Flags == blocking, expected<data>>
  get(data key) const {
    return request<data>(atom::get::value, std::move(key));
  }

  /// Retrieves a value.
  /// @param key The key of the value to retrieve.
  /// @returns The value under *key* or an error.
  template <api_flags Flags>
  detail::enable_if_t<Flags == nonblocking, continuation<data>>
  get(data key) const {
    return {frontend_, atom::get::value, std::move(key)};
  }

  /// Retrieves a specific aspect of a value.
  /// @param key The key of the value to retrieve.
  /// @param aspect The aspect of the value.
  /// @returns The value under *key* or an error.
  template <api_flags Flags = blocking>
  detail::enable_if_t<Flags == blocking, expected<data>>
  lookup(data key, data value) const {
    return request<data>(atom::get::value, std::move(key), std::move(value));
  }

  // --- modifiers -----------------------------------------------------------

  /// Inserts or updates a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional expiration time for *key*.
  void put(data key, data value, optional<timestamp> expiry = {}) const;

  /// Adds a value to another one.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void add(data key, data value, optional<timestamp> expiry = {}) const;

  /// Removes a value from another one.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void remove(data key, data value, optional<timestamp> expiry = {}) const;

  /// Removes the value associated with a given key.
  /// @param key The key to remove from the store.
  void erase(data key) const;

  /// Increments a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void increment(data key, data value, optional<timestamp> expiry = {}) const;

  /// Decrements a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void decrement(data key, data value, optional<timestamp> expiry = {}) const;

private:
  store(caf::actor actor);

  template <class T, class... Ts>
  expected<T> request(Ts&&... xs) const {
    expected<T> result{sc::unspecified};
    caf::scoped_actor self{frontend_->home_system()};
    auto msg = caf::make_message(std::forward<Ts>(xs)...);
    self->request(frontend_, timeout::frontend, std::move(msg)).receive(
      [&](T& x) {
        result = std::move(x);
      },
      [&](caf::error& e) {
        result = std::move(e);
      }
    );
    return result;
  }

  caf::actor frontend_;
};

} // namespace broker

#endif // BROKER_STORE_HH
