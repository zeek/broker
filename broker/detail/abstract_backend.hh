#ifndef BROKER_DETAIL_ABSTRACT_BACKEND_HH
#define BROKER_DETAIL_ABSTRACT_BACKEND_HH

#include "broker/data.hh"
#include "broker/optional.hh"
#include "broker/result.hh"
#include "broker/snapshot.hh"

namespace broker {
namespace detail {

/// Abstract base class for a key-value storage backend.
class abstract_backend {
public:
  abstract_backend() = default;

  virtual ~abstract_backend() = default;

  // --- modifiers ------------------------------------------------------------

  /// Inserts or updates a key-value pair.
  /// @param key The key to update/insert.
  /// @param value The value associated with *key*.
  /// @param expiry An optional expiration time for the entry.
  /// @returns `nil` on success.
  virtual result<void> put(const data& key, data value,
                           optional<timestamp> expiry = {}) = 0;

  /// Adds one value to another value.
  /// @param key The key associated with the existing value to add to.
  /// @param value The value to add on top of the existing value at *key*.
  /// @param t The point in time this modification took place.
  /// @returns `nil` on success.
  virtual result<void> add(const data& key, const data& value,
                           optional<timestamp> expiry = {});

  /// Removes one value from another value.
  /// @param key The key associated with the existing value to remove from.
  /// @param value The value to remove from the existing value at *key*.
  /// @param t The point in time this modification took place.
  /// @returns `nil` on success.
  virtual result<void> remove(const data& key, const data& value,
                              optional<timestamp> expiry = {});

  /// Removes a key and its associated value from the store, if it exists.
  /// @param key The key to use.
  /// @returns `nil` if *key* was removed successfully or if *key* did not
  /// exist.
  virtual result<void> erase(const data& key) = 0;

  /// Removes a key and its associated value from the store, if it exists and
  /// has an expiration in the past.
  /// @param key The key to expire.
  /// @returns `true` if *key* was expired (and deleted) successfully, and
  /// `false` if the value cannot be expired yet, i.e., the existing expiry
  /// time lies in the future.
  virtual result<bool> expire(const data& key) = 0;

  // --- inspectors -----------------------------------------------------------

  /// Retrieves the value associated with a given key.
  /// @param key The key to use.
  /// @returns The value associated with *key*.
  virtual result<data> get(const data& key) const = 0;

  /// Retrieves a specific aspect of a value for a given key.
  /// @param key The key to use.
  /// @param aspect The aspect of the value at *key* to lookup.
  /// @returns The *aspect* of the value at *key*.
  virtual result<data> get(const data& key, const data& value) const;

  /// Checks if a key exists.
  /// @param key The key to check.
  /// @returns `true` if the *key* exists and `false` if it doesn't.
  /// the query.
  virtual result<bool> exists(const data& key) const = 0;

  /// Retrieves the number of entries in the store.
  /// @returns The number of key-value pairs in the store.
  virtual result<uint64_t> size() const = 0;

  /// Retrieves all key-value pairs.
  /// @returns A snapshot of the store that includes its content.
  virtual result<broker::snapshot> snapshot() const = 0;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_ABSTRACT_BACKEND_HH
