#ifndef BROKER_STORE_HH
#define BROKER_STORE_HH

#include <string>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/detail/type_traits.hpp>

#include "broker/api_flags.hh"
#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/mailbox.hh"
#include "broker/optional.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"

namespace broker {

class endpoint;

/// A key-value store (either a *master* or *clone*) that supports modifying
/// and querying contents.
class store {
  friend endpoint; // construction

public:
  /// A response to a lookup request issued by a ::proxy.
  struct response {
    expected<data> answer;
    request_id id;
  };

  /// A utility to decouple store request from response processing.
  class proxy {
  public:
    proxy() = default;

    /// Constructs a proxy for a given store.
    /// @param s The store to create a proxy for.
    explicit proxy(store& s);

    /// Performs a request to retrieve a value.
    /// @param key The key of the value to retrieve.
    /// @returns A unique identifier for this request to correlate it with a
    /// response.
    request_id get(data key);

    /// Performs a request to retrieve a store's keys.
    /// @returns A unique identifier for this request to correlate it with a
    /// response.
    request_id keys();

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

  /// Default-constructs an uninitialized store.
  store() = default;

  // --- inspectors -----------------------------------------------------------

  /// Retrieves the name of the store.
  /// @returns The store name.
  std::string name() const;

  /// Retrieves a value.
  /// @param key The key of the value to retrieve.
  /// @returns The value under *key* or an error.
  expected<data> get(data key) const;

  /// Retrieves a specific aspect of a value.
  /// @param key The key of the value to retrieve.
  /// @param aspect The aspect of the value.
  /// @returns The value under *key* or an error.
  expected<data> get(data key, data aspect) const;

  /// Retrieves a copy of the store's current keys, returned as a set.
  expected<data> keys() const;

  // --- modifiers -----------------------------------------------------------

  /// Inserts or updates a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional expiration time for *key*.
  void put(data key, data value, optional<timespan> expiry = {}) const;

  /// Removes the value associated with a given key.
  /// @param key The key to remove from the store.
  void erase(data key) const;

  /// Empties out the store.
  void clear() const;

  /// Adds a value to another one.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void add(data key, data value, optional<timespan> expiry = {}) const;

  /// Subtracts a value from another one.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void subtract(data key, data value, optional<timespan> expiry = {}) const;

private:
  store(caf::actor actor);

  template <class T, class... Ts>
  expected<T> request(Ts&&... xs) const {
    if (!frontend_)
      return make_error(ec::unspecified, "store not initialized");
    expected<T> res{ec::unspecified};
    caf::scoped_actor self{frontend_->home_system()};
    auto msg = caf::make_message(std::forward<Ts>(xs)...);
    self->request(frontend_, timeout::frontend, std::move(msg)).receive(
      [&](T& x) {
        res = std::move(x);
      },
      [&](caf::error& e) {
        res = std::move(e);
      }
    );
    return res;
  }

  caf::actor frontend_;
};

} // namespace broker

#endif // BROKER_STORE_HH
