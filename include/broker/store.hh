#pragma once

#include <memory>
#include <string>
#include <vector>

#include <caf/actor.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/error.hpp>
#include <caf/make_message.hpp>
#include <caf/stream.hpp>

#include "broker/api_flags.hh"
#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/fwd.hh"
#include "broker/mailbox.hh"
#include "broker/message.hh"
#include "broker/optional.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"

namespace broker {

class endpoint;

/// A key-value store (either a *master* or *clone*) that supports modifying
/// and querying contents.
class store {
public:
  // -- friends ----------------------------------------------------------------

  friend class endpoint;

  // -- member types -----------------------------------------------------------

  using stream_type = caf::stream<command_message>;

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

    /// Performs a request to check existence of a value.
    /// @returns A unique identifier for this request to correlate it with a
    /// response.
    request_id exists(data key);

    /// Performs a request to retrieve a value.
    /// @param key The key of the value to retrieve.
    /// @returns A unique identifier for this request to correlate it with a
    /// response.
    request_id get(data key);

    /// Inserts a value if the key does not already exist.
    /// @param key The key of the key-value pair.
    /// @param value The value of the key-value pair.
    /// @param expiry An optional expiration time for *key*.
    /// @returns A unique identifier for this request to correlate it with a
    /// response.
    request_id put_unique(data key, data value,
                          optional<timespan> expiry = {});

    /// For containers values, retrieves a specific index from the value. This
    /// is supported for sets, tables, and vectors.
    /// @param key The key of the container value to retrieve from.
    /// @param key The index of the value to retrieve.
    /// @returns A unique identifier for this request to correlate it with a
    /// response.
    request_id get_index_from_value(data key, data index);

    /// Performs a request to retrieve a store's keys.
    /// @returns A unique identifier for this request to correlate it with a
    /// response.
    request_id keys();

    /// Retrieves the proxy's mailbox that reflects query responses.
    broker::mailbox mailbox();

    /// Consumes the next response or blocks until one arrives.
    /// @returns The next response in the proxy's mailbox.
    response receive();

    /// Consumes the next N responses or blocks until N responses arrive.
    /// @returns The next N responses in the proxy's mailbox.
    std::vector<response> receive(size_t n);

    /// Returns a globally unique identifier for the frontend actor.
    entity_id frontend_id() const noexcept {
      return {frontend_.node(), frontend_.id()};
    }

  private:
    request_id id_ = 0;
    caf::actor frontend_;
    caf::actor proxy_;
  };

  // -- constructors, destructors, and assignment operators --------------------

  store();

  store(store&&);

  store(const store&);

  store& operator=(store&&);

  store& operator=(const store&);

  ~store();

  // --- inspectors -----------------------------------------------------------

  /// Retrieves the name of the store.
  /// @returns A copy of the store name or an empty string when calling this
  ///          function on an invalid object.
  std::string name() const;

  /// Checks whether a key exists in the store.
  /// @returns A boolean that's if the key exists.
  expected<data> exists(data key) const;

  /// Retrieves a value.
  /// @param key The key of the value to retrieve.
  /// @returns The value under *key* or an error.
  expected<data> get(data key) const;

  /// Inserts a value if the key does not already exist.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional expiration time for *key*.
  /// @returns A true data value if inserted or false if key already existed.
  expected<data> put_unique(data key, data value,
                            optional<timespan> expiry = {});

  /// For containers values, retrieves a specific index from the value. This
  /// is supported for sets, tables, and vectors.
  /// @param key The key of the value to retrieve the index from.
  /// @param index The index of the value to retrieve.
  /// @returns For tables and vector, the value under *index* or an error.
  /// For sets, a boolean indicating whether the set contains the index.
  /// Always returns an error if the store does not have the key.
  expected<data> get_index_from_value(data key, data index) const;

  /// Retrieves a copy of the store's current keys, returned as a set.
  expected<data> keys() const;

  /// Returns whether the store was fully initialized
  bool initialized() const noexcept;

  /// Returns whether the store was fully initialized
  explicit operator bool() const noexcept {
    return initialized();
  }

  /// Retrieves the frontend.
  /// @pre `initialized()`
  caf::actor frontend() const;

  /// Returns a globally unique identifier for the frontend actor.
  entity_id frontend_id() const;

  /// Returns the topic for sending messages to the master.
  caf::actor self_hdl() const;

  /// Returns the topic for sending messages to the master.
  entity_id self_id() const;

  // --- modifiers -----------------------------------------------------------

  /// Inserts or updates a value.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional expiration time for *key*.
  void put(data key, data value, optional<timespan> expiry = {});

  /// Removes the value associated with a given key.
  /// @param key The key to remove from the store.
  void erase(data key);

  /// Empties out the store.
  void clear();

  /// Increments a value by a given amount. This is supported for all
  /// numerical types as well as for timestamps.
  /// @param key The key of the value to increment.
  /// @param value The amount to increment the value.
  /// @param expiry An optional new expiration time for *key*.
  void increment(data key, data amount, optional<timespan> expiry = {}) {
    auto init_type = data::type::none;

    switch ( amount.get_type() ) {
      case data::type::count:
        init_type = data::type::count;
        break;
      case data::type::integer:
        init_type = data::type::integer;
        break;
      case data::type::real:
        init_type = data::type::real;
        break;
      case data::type::timespan:
        init_type = data::type::timestamp;
        break;
      default:
        break;
    }

    add(std::move(key), std::move(amount), init_type, std::move(expiry));
  }

  /// Decrements a value by a given amount. This is supported for all
  /// numerical types as well as for timestamps.
  /// @param key The key of the value to increment.
  /// @param value The amount to decrement the value.
  /// @param expiry An optional new expiration time for *key*.
  void decrement(data key, data amount, optional<timespan> expiry = {}) {
    subtract(std::move(key), std::move(amount), std::move(expiry));
  }

  /// Appends a string to another one.
  /// @param key The key of the string to which to append.
  /// @param str The string to append.
  /// @param expiry An optional new expiration time for *key*.
  void append(data key, data str, optional<timespan> expiry = {}) {
    add(std::move(key), std::move(str), data::type::string, std::move(expiry));
  }

  /// Inserts an index into a set.
  /// @param key The key of the set into which to insert the value.
  /// @param index The index to insert.
  /// @param expiry An optional new expiration time for *key*.
  void insert_into(data key, data index, optional<timespan> expiry = {}) {
    add(std::move(key), std::move(index), data::type::set, std::move(expiry));
  }

  /// Inserts an index into a table.
  /// @param key The key of the table into which to insert the value.
  /// @param index The index to insert.
  /// @param value The value to associated with the inserted index. For sets, this is ignored.
  /// @param expiry An optional new expiration time for *key*.
  void insert_into(data key, data index, data value,
                   optional<timespan> expiry = {}) {
    add(std::move(key), vector({std::move(index), std::move(value)}),
        data::type::table, std::move(expiry));
  }

  /// Removes am index from a set or table.
  /// @param key The key of the set/table from which to remove the value.
  /// @param index The index to remove.
  /// @param expiry An optional new expiration time for *key*.
  void remove_from(data key, data index, optional<timespan> expiry = {}) {
    subtract(std::move(key), std::move(index), std::move(expiry));
  }

  /// Appends a value to a vector.
  /// @param key The key of the vector to which to append the value.
  /// @param value The value to append.
  /// @param expiry An optional new expiration time for *key*.
  void push(data key, data value, optional<timespan> expiry = {}) {
    add(std::move(key), std::move(value), data::type::vector,
        std::move(expiry));
  }

  /// Removes the last value of a vector.
  /// @param key The key of the vector from which to remove the last value.
  /// @param expiry An optional new expiration time for *key*.
  void pop(data key, optional<timespan> expiry = {}) {
    subtract(key, key, std::move(expiry));
  }

  // --await-idle-start
  /// Blocks execution of the current thread until the frontend actor reached an
  /// IDLE state. On a master, this means that all clones have caught up with
  /// the master and have ACKed the most recent command. On a clone, this means
  /// that the master has ACKed any pending put commands from this store and
  /// that the clone is not waiting on any out-of-order messages from the
  /// master.
  /// @param timeout The maximum amount of time this function may block.
  /// @returns `true` if the frontend actor responded before the timeout,
  ///          `false` otherwise.
  [[nodiscard]] bool await_idle(timespan timeout
                                = defaults::store::await_idle_timeout);

  /// Asynchronously runs `callback(true)` when the frontend actor reached an
  /// IDLE state or `callback(false)` if the optional timeout triggered first
  /// (or in case of an error).
  /// @param timeout The maximum amount of time this function may block.
  /// @param callback A function object wrapping code for asynchronous
  ///                 execution. The argument for the callback is `true` if the
  ///                 frontend actor responded before the timeout, `false`
  ///                 otherwise.
  void await_idle(std::function<void(bool)> callback,
                  timespan timeout = defaults::store::await_idle_timeout);
  // --await-idle-end

  /// Release any state held by the object, rendering it invalid.
  /// @warning Performing *any* action on this object afterwards invokes
  ///          undefined behavior, except:
  ///          - Destroying the object by calling the destructor.
  ///          - Using copy- or move-assign from a valid `store` to "revive"
  ///            this object.
  ///          - Calling `reset` again (multiple invocations are no-ops).
  /// @note This member function specifically targets the Python bindings. When
  ///       writing Broker applications using the native C++ API, there's no
  ///       point in calling this member function.
  void reset();

private:
  store(caf::actor actor, std::string name);

  /// Adds a value to another one, with a type-specific meaning of
  /// "add". This is the backend for a number of the modifiers methods.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param init_type The type of data to initialize when the key does not exist.
  /// @param expiry An optional new expiration time for *key*.
  void add(data key, data value, data::type init_type,
           optional<timespan> expiry = {});

  /// Subtracts a value from another one, with a type-specific meaning of
  /// "substract". This is the backend for a number of the modifiers methods.
  /// @param key The key of the key-value pair.
  /// @param value The value of the key-value pair.
  /// @param expiry An optional new expiration time for *key*.
  void subtract(data key, data value, optional<timespan> expiry = {});

  // -- member variables -------------------------------------------------------

  // If we would only consider the native C++ API, we could store a regular
  // shared pointer here and rely on scoping to make sure that store objects get
  // destroyed before the broker::endpoint shuts down (and takes the frontend
  // actor down with it). However, Zeek scripts in particular commonly declare
  // store objects as global variables. Hence, we need a way to invalidate store
  // objects once the frontend actor shuts down. We achieve this by storing only
  // a weak pointer to the state here and have the frontend actor keeping this
  // state alive by holding on to a strong reference. Once the frontend actor
  // terminates, the state becomes invalid since no other objects holds a strong
  // reference to the state.
  detail::weak_store_state_ptr state_;
};

} // namespace broker
