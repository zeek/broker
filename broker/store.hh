#ifndef BROKER_STORE_HH
#define BROKER_STORE_HH

#include <string>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/detail/type_traits.hpp>

#include "broker/api_flags.hh"
#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/optional.hh"
#include "broker/timeout.hh"

namespace broker {

class endpoint;

/// A key-value store (either a *master* or *clone*) that supports modifying
/// and querying contents.
class store {
  friend endpoint; // construction

public:
  class response_proxy {
  public:
    response_proxy(caf::actor frontend, data key)
      : frontend_{std::move(frontend)}, key_{std::move(key)} {
    }

    template <class OnData, class OnError>
    void then(OnData on_data, OnError on_error) {
      using on_data_type = caf::detail::get_callable_trait<OnData>;
      using on_error_type = caf::detail::get_callable_trait<OnError>;
      static_assert(std::is_same<void, typename on_data_type::result_type>{},
                    "data callback must not have a return value");
      static_assert(std::is_same<void, typename on_error_type::result_type>{},
                    "error callback must not have a return value");
      using on_data_args = typename on_data_type::arg_types;
      using on_error_args = typename on_error_type::arg_types;
      static_assert(caf::detail::tl_size<on_data_args>::value == 1,
                    "data callback can have only one argument");
      static_assert(caf::detail::tl_size<on_error_args>::value == 1,
                    "error callback can have only one argument");
      using on_data_arg = typename caf::detail::tl_head<on_data_args>::type;
      using on_error_arg = typename caf::detail::tl_head<on_error_args>::type;
      static_assert(std::is_same<detail::decay_t<on_data_arg>, data>::value,
                    "data callback must have broker::data as argument type");
      static_assert(std::is_same<detail::decay_t<on_error_arg>, error>::value,
                    "data callback must have broker::errror as argument type");
      frontend_->home_system().spawn(
        [=](caf::event_based_actor* self) mutable {
          auto msg = caf::make_message(atom::get::value, std::move(key_));
          self->request(frontend_, timeout::frontend, std::move(msg)).then(
            on_data,
            on_error
          );
        }
      );
    }

  private:
    caf::actor frontend_;
    data key_;
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
  detail::enable_if_t<Flags == nonblocking, response_proxy>
  get(data key) const {
    return {frontend_, std::move(key)};
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
  void put(data key, data value, optional<time::point> expiry = {}) const;

  /// Adds a value to another one.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void add(data key, data value, optional<time::point> expiry = {}) const;

  /// Removes a value from another one.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void remove(data key, data value, optional<time::point> expiry = {}) const;

  /// Removes the value associated with a given key.
  /// @param key The key to remove from the store.
  void erase(data key) const;

  /// Increments a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void increment(data key, data value, optional<time::point> expiry = {}) const;

  /// Decrements a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void decrement(data key, data value, optional<time::point> expiry = {}) const;

private:
  store(caf::actor actor);

  template <class T, class... Ts>
  expected<T> request(Ts&&... xs) const {
    expected<T> result{ec::unspecified};
    caf::scoped_actor self{frontend_->home_system()};
    auto msg = caf::make_message(std::forward<Ts>(xs)...);
    self->request(frontend_, timeout::frontend, std::move(msg)).receive(
      [&](T& x) {
        result = std::move(x);
      },
      [&](error& e) {
        result = std::move(e);
      }
    );
    return result;
  }

  caf::actor frontend_;
};

} // namespace broker

#endif // BROKER_STORE_HH
