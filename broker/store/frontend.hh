#ifndef BROKER_STORE_FRONTEND_HH
#define BROKER_STORE_FRONTEND_HH

#include <string>

#include <caf/scoped_actor.hpp>

#include "broker/api_flags.hh"
#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/timeout.hh"

namespace broker {

class endpoint;

namespace store {

/// Distinguishes the two frontend types.
enum frontend_type {
  /// A clone of a master data store.  The clone automatically synchronizes to
  /// the master version by receiving updates made to the master and applying
  /// them locally.
  clone,
  /// This type of store is authoritative over its contents. A master directly
  /// applies mutable operations to its backend and then broadcasts the update
  /// to its clones.
  master
};

/// A frontend interface of a data store (either a *master* or *clone*) that
/// supports modifying and querying contents.
class frontend {
  friend endpoint; // construction

public:
  // --- inspectors -----------------------------------------------------------

  /// Retrieves the name of the frontend.
  /// @returns The frontend name.
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
  void put(data key, data value) const;

  /// Removes the value associated with a given key.
  /// @param key The key to remove from the store.
  void erase(data key) const;

  /// Increments a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  void increment(data key, data value) const;

  /// Decrements a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  void decrement(data key, data value) const;

  /// Adds a value to another one.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  void add(data key, data value) const;

  /// Removes a value from another one.
  void remove(data key, data value) const;

private:
  frontend(caf::actor actor);

  template <class T, class... Ts>
  expected<T> request(Ts&&... xs) const {
    expected<T> result{ec::unspecified};
    caf::scoped_actor self{frontend_->home_system()};
    self->request(frontend_, timeout::frontend,
                  std::forward<Ts>(xs)...).receive(
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

} // namespace store
} // namespace broker

#endif // BROKER_STORE_FRONTEND_HH
