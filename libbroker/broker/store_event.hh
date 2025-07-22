#pragma once

#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/entity_id.hh"
#include "broker/fwd.hh"

#include <cstdint>
#include <string>

namespace broker {

/// Wraps view types for convenient handling of store events.
class store_event {
public:
  enum class type : uint8_t {
    insert,
    update,
    erase,
    expire,
  };

  /// A view into a ::data object representing an `insert` event.
  /// Broker encodes `insert` events as:
  ///
  /// ```
  /// [
  ///   "insert",
  ///   store_id: string,
  ///   key: data,
  ///   value: data,
  ///   expiry: optional<timespan>,
  ///   publisher_endpoint: endpoint_id,
  ///   publisher_object: uint64_t
  /// ]
  /// ```
  ///
  /// Whereas the `publisher_endpoint` and the `publisher_object` encode a
  /// @ref entity_id.
  class insert {
  public:
    insert(const insert&) noexcept = default;

    insert& operator=(const insert&) noexcept = default;

    static insert make(const data& src) noexcept {
      if (auto xs = get_if<vector>(src))
        return make(*xs);
      return insert{nullptr};
    }

    static insert make(const vector& xs) noexcept;

    explicit operator bool() const noexcept {
      return xs_ != nullptr;
    }

    const std::string& store_id() const {
      return get<std::string>((*xs_)[1]);
    }

    const data& key() const noexcept {
      return (*xs_)[2];
    }

    const data& value() const noexcept {
      return (*xs_)[3];
    }

    std::optional<timespan> expiry() const noexcept {
      if (auto value = get_if<timespan>((*xs_)[4]))
        return *value;
      else
        return {};
    }

    entity_id publisher() const {
      if (auto value = to<endpoint_id>((*xs_)[5])) {
        return {.endpoint = *value, .object = get<uint64_t>((*xs_)[6])};
      }
      return {};
    }

  private:
    explicit insert(const vector* xs) noexcept : xs_(xs) {
      // nop
    }

    const vector* xs_;
  };

  /// A view into a ::data object representing a `update` event.
  /// Broker encodes `update` events as
  /// ```
  /// [
  ///   "update",
  ///   store_id: string,
  ///   key: data,
  ///   value: data,
  ///   expiry: optional<timespan>
  ///   publisher_endpoint: endpoint_id,
  ///   publisher_object: uint64_t
  /// ].
  /// ```
  class update {
  public:
    update(const update&) noexcept = default;

    update& operator=(const update&) noexcept = default;

    static update make(const data& src) noexcept {
      if (auto xs = get_if<vector>(src))
        return make(*xs);
      return update{nullptr};
    }

    static update make(const vector& xs) noexcept;

    explicit operator bool() const noexcept {
      return xs_ != nullptr;
    }

    const std::string& store_id() const {
      return get<std::string>((*xs_)[1]);
    }

    const data& key() const noexcept {
      return (*xs_)[2];
    }

    const data& old_value() const noexcept {
      return (*xs_)[3];
    }

    const data& new_value() const noexcept {
      return (*xs_)[4];
    }

    std::optional<timespan> expiry() const noexcept {
      if (auto value = get_if<timespan>((*xs_)[5]))
        return *value;
      else
        return {};
    }

    entity_id publisher() const {
      if (auto value = to<endpoint_id>((*xs_)[6]))
        return {.endpoint = *value, .object = get<uint64_t>((*xs_)[7])};
      else
        return {};
    }

  private:
    explicit update(const vector* xs) noexcept : xs_(xs) {
      // nop
    }

    const vector* xs_;
  };

  /// A view into a ::data object representing an `erase` event.
  /// Broker encodes `erase` events as
  /// ```
  /// [
  ///   "erase",
  ///   store_id: string,
  ///   key: data,
  ///   publisher_endpoint: endpoint_id,
  ///   publisher_object: uint64_t
  /// ]
  /// ```
  class erase {
  public:
    erase(const erase&) noexcept = default;

    erase& operator=(const erase&) noexcept = default;

    static erase make(const data& src) noexcept {
      if (auto xs = get_if<vector>(src))
        return make(*xs);
      return erase{nullptr};
    }

    static erase make(const vector& xs) noexcept;

    explicit operator bool() const noexcept {
      return xs_ != nullptr;
    }

    const std::string& store_id() const {
      return get<std::string>((*xs_)[1]);
    }

    const data& key() const noexcept {
      return (*xs_)[2];
    }

    entity_id publisher() const {
      if (auto value = to<endpoint_id>((*xs_)[3])) {
        return {.endpoint = *value, .object = get<uint64_t>((*xs_)[4])};
      }
      return {};
    }

  private:
    explicit erase(const vector* xs) noexcept : xs_(xs) {
      // nop
    }

    const vector* xs_;
  };

  /// A view into a ::data object representing an `expire` event.
  /// Broker encodes `expire` events as
  /// ```
  /// [
  ///   "expire",
  ///   store_id: string,
  ///   key: data,
  ///   publisher_endpoint: endpoint_id,
  ///   publisher_object: uint64_t
  /// ]
  /// ```
  class expire {
  public:
    expire(const expire&) noexcept = default;

    expire& operator=(const expire&) noexcept = default;

    static expire make(const data& src) noexcept {
      if (auto xs = get_if<vector>(src))
        return make(*xs);
      return expire{nullptr};
    }

    static expire make(const vector& xs) noexcept;

    explicit operator bool() const noexcept {
      return xs_ != nullptr;
    }

    const std::string& store_id() const {
      return get<std::string>((*xs_)[1]);
    }

    const data& key() const noexcept {
      return (*xs_)[2];
    }

    entity_id publisher() const {
      if (auto value = to<endpoint_id>((*xs_)[3]))
        return {.endpoint = *value, .object = get<uint64_t>((*xs_)[4])};
      else
        return {};
    }

  private:
    explicit expire(const vector* xs) noexcept : xs_(xs) {
      // nop
    }

    const vector* xs_;
  };
};

BROKER_CONVERT_AND_TO_STRING(store_event::type)
BROKER_CONVERT_AND_TO_STRING(store_event::insert)
BROKER_CONVERT_AND_TO_STRING(store_event::update)
BROKER_CONVERT_AND_TO_STRING(store_event::erase)

/// @relates store_event::type
bool convert(const std::string& src, store_event::type& dst) noexcept;

/// @relates store_event::type
bool convert(const data& src, store_event::type& dst) noexcept;

/// @relates store_event::type
bool convertible_to_store_event_type(const data& src) noexcept;

template <>
struct can_convert_predicate<store_event::type> {
  static bool check(const data& src) noexcept {
    return convertible_to_store_event_type(src);
  }
};

} // namespace broker
