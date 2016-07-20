#ifndef BROKER_DETAIL_ABSTRACT_BACKEND_HH
#define BROKER_DETAIL_ABSTRACT_BACKEND_HH

#include "broker/data.hh"
#include "broker/expected.hh"
#include "broker/optional.hh"
#include "broker/snapshot.hh"

namespace broker {
namespace detail {

/// Abstract base class for a key-value storage backend.
class abstract_backend {
public:
  abstract_backend() = default;

  virtual ~abstract_backend() = default;

  // --- modifiers ------------------------------------------------------------

  /// Inserts or updates a key-value pair in to the store.
  /// @param key The key to use.
  /// @param value The value associated with the key.
  /// @param expiry An optional expiration time for the entry.
  /// @returns `true` on success.
  virtual expected<void> put(const data& key, data value,
                             optional<time::point> expiry = {}) = 0;

  /// Removes a key and its associated value from the store, if it exists.
  /// @param key The key to use.
  /// @returns `true` if the key didn't exist or was removed successfully.
  virtual expected<void> erase(const data& key) = 0;

  /// Adds one value to another value.
  /// @param key The key associated with the existing value to add to.
  /// @param value The value to add on top of the existing value at *key*.
  /// @param t The point in time this modification took place.
  /// @returns The result of the modification.
  virtual expected<void> add(const data& key, const data& value,
                             time::point t = {}) = 0;

  /// Removes one value from another value.
  /// @param key The key associated with the existing value to remove from.
  /// @param value The value to remove from the existing value at *key*.
  /// @param t The point in time this modification took place.
  /// @returns The result of the modification.
  virtual expected<void> remove(const data& key, const data& value,
                                time::point t = {}) = 0;

  /// Removes a key and its associated value from the store, if it exists and
  /// the expiration value is the same.
  /// @param key The key to use.
  /// @param expiration The expiration value which must still match, otherwise
  /// this expiry operation is ignored.
  /// @returns `true` If the key didn't exist or was removed successfully.
  virtual expected<void> expire(const data& key, time::point expiry) = 0;

  // --- inspectors -----------------------------------------------------------

  /// Retrieves the value associated with a given key.
  /// @param key The key to use.
  /// @returns The value associated with *key*.
  virtual expected<data> get(const data& key) const = 0;

  /// Retrieves a specific aspected associated with a given key.
  /// @param key The key to use.
  /// @param aspect The aspect of the value at *key* to lookup.
  /// @returns The *aspect* of the value at *key*.
  virtual expected<data> get(const data& key, const data& value) const = 0;

  /// Check if a given key exists.
  /// @param key The key to use.
  /// @returns `true` If the provided key exists or nil on failing to perform
  /// the query.
  virtual expected<bool> exists(const data& key) const = 0;

  /// @returns The number of key-value pairs in the store.
  virtual expected<uint64_t> size() const = 0;

  /// @returns A snapshot of the store that includes its content.
  virtual expected<broker::snapshot> snapshot() const = 0;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_ABSTRACT_BACKEND_HH
