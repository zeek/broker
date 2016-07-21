#ifndef BROKER_STORE_HH
#define BROKER_STORE_HH

#include <string>

#include <caf/scoped_actor.hpp>

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
