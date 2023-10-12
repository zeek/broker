#pragma once

#include "broker/data.hh"
#include "broker/data_envelope.hh"
#include "broker/fwd.hh"
#include "broker/variant_data.hh"
#include "broker/variant_tag.hh"

#include <cstdint>
#include <string>
#include <string_view>

namespace broker {

/// Evaluates to `true` if `T` represents a primitive type.
template <class T>
inline constexpr bool is_primtivie_data_v =
  detail::is_one_of_v<T, none, boolean, count, integer, real, std::string,
                      address, subnet, port, timestamp, timespan, enum_value>;

/// Represents a value of any of the following types:
/// - @ref none
/// - @ref boolean
/// - @ref count
/// - @ref integer
/// - @ref real
/// - @ref std::string
/// - @ref address
/// - @ref subnet
/// - @ref port
/// - @ref timestamp
/// - @ref timespan
/// - @ref enum_value
/// - @ref variant_set
/// - @ref variant_table
/// - @ref variant_list
class variant {
public:
  // -- constructors, destructors, and assignment operators --------------------

  variant() : raw_(variant_data::nil()) {}

  variant(variant&&) = default;

  variant(const variant&) = default;

  variant& operator=(variant&&) = default;

  variant& operator=(const variant&) = default;

  variant(const variant_data* value, data_envelope_ptr ptr) noexcept
    : raw_(value), envelope_(std::move(ptr)) {
    // nop
  }

  // -- properties -------------------------------------------------------------

  /// Returns a string representation of the stored type.
  const char* get_type_name() const {
    return detail::cpp_type_name(get_tag());
  }

  /// Checks whether this object is the root object in its envelope.
  bool is_root() const noexcept;

  /// Returns the type of the contained data.
  variant_tag get_tag() const noexcept {
    return raw_->get_tag();
  }

  /// Checks whether this view contains the `nil` value.
  bool is_none() const noexcept {
    return get_tag() == variant_tag::none;
  }

  /// Checks whether this view contains a boolean.
  bool is_boolean() const noexcept {
    return get_tag() == variant_tag::boolean;
  }

  /// Checks whether this view contains a count.
  bool is_count() const noexcept {
    return get_tag() == variant_tag::count;
  }

  /// Checks whether this view contains a integer.
  bool is_integer() const noexcept {
    return get_tag() == variant_tag::integer;
  }

  /// Checks whether this view contains a real.
  bool is_real() const noexcept {
    return get_tag() == variant_tag::real;
  }

  /// Checks whether this view contains a count.
  bool is_string() const noexcept {
    return get_tag() == variant_tag::string;
  }

  /// Checks whether this view contains a count.
  bool is_address() const noexcept {
    return get_tag() == variant_tag::address;
  }

  /// Checks whether this view contains a count.
  bool is_subnet() const noexcept {
    return get_tag() == variant_tag::subnet;
  }

  /// Checks whether this view contains a count.
  bool is_port() const noexcept {
    return get_tag() == variant_tag::port;
  }

  /// Checks whether this view contains a count.
  bool is_timestamp() const noexcept {
    return get_tag() == variant_tag::timestamp;
  }

  /// Checks whether this view contains a count.
  bool is_timespan() const noexcept {
    return get_tag() == variant_tag::timespan;
  }

  /// Checks whether this view contains a count.
  bool is_enum_value() const noexcept {
    return get_tag() == variant_tag::enum_value;
  }

  /// Checks whether this view contains a set.
  bool is_set() const noexcept {
    return get_tag() == variant_tag::set;
  }

  /// Checks whether this view contains a table.
  bool is_table() const noexcept {
    return get_tag() == variant_tag::table;
  }

  /// Checks whether this view contains a list.
  bool is_list() const noexcept {
    return get_tag() == variant_tag::vector;
  }

  /// Alias for @ref is_list.
  bool is_vector() const noexcept {
    return is_list();
  }

  // -- conversions ------------------------------------------------------------

  /// Converts this view into a @c data object.
  data to_data() const;

  /// Retrieves the @c boolean value or returns @p fallback if this object does
  /// not contain a @c boolean.
  bool to_boolean(bool fallback = false) const noexcept {
    if (auto* val = std::get_if<boolean>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c count value or returns @p fallback if this object does
  /// not contain a @c count.
  count to_count(count fallback = 0) const noexcept {
    if (auto* val = std::get_if<count>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c integer value or returns @p fallback if this object does
  /// not contain a @c integer.
  integer to_integer(integer fallback = 0) const noexcept {
    if (auto* val = std::get_if<integer>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c real value or returns @p fallback if this object does
  /// not contain a @c real.
  real to_real(real fallback = 0) const noexcept {
    if (auto* val = std::get_if<real>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the string value or returns an empty string if this object does
  /// not contain a string.
  std::string_view to_string() const noexcept {
    if (auto* val = std::get_if<std::string_view>(&raw_->value))
      return *val;
    return std::string_view{};
  }

  /// Retrieves the string value or returns @p fallback if this object does
  /// not contain a string.
  template <class StringView>
  std::enable_if_t<std::is_same_v<std::string_view, StringView>, StringView>
  to_string(StringView fallback) const noexcept {
    // Note: we use enable_if to block implicit conversions. Otherwise, users
    // could pass in a `std::string` rvalue and we would return a dangling
    // reference.
    if (auto* val = std::get_if<std::string_view>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c address value or returns @p fallback if this object does
  /// not contain a @c address.
  address to_address(const address& fallback = {}) const noexcept {
    if (auto* val = std::get_if<address>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c subnet value or returns @p fallback if this object does
  /// not contain a @c subnet.
  subnet to_subnet(const subnet& fallback = {}) const noexcept {
    if (auto* val = std::get_if<subnet>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c port value or returns @p fallback if this object does
  /// not contain a @c port.
  port to_port(port fallback = {}) const noexcept {
    if (auto* val = std::get_if<port>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c timestamp value or returns @p fallback if this object
  /// does not contain a @c timestamp.
  timestamp to_timestamp(timestamp fallback = {}) const noexcept {
    if (auto* val = std::get_if<timestamp>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the @c timespan value or returns @p fallback if this object does
  /// not contain a @c timespan.
  timespan to_timespan(timespan fallback = {}) const noexcept {
    if (auto* val = std::get_if<timespan>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Retrieves the enum_value value or returns @p fallback if this object does
  /// not contain a enum_value.
  enum_value_view to_enum_value(enum_value_view fallback = {}) const noexcept {
    if (auto* val = std::get_if<enum_value_view>(&raw_->value))
      return *val;
    return fallback;
  }

  /// Returns the contained values as a @ref variant_set or an empty set if this
  /// object does not contain a set.
  variant_set to_set() const noexcept;

  /// Returns the contained values as a @ref variant_table or an empty table if
  /// this object does not contain a table.
  variant_table to_table() const noexcept;

  /// Returns the contained values as a @ref variant_list or an empty list if
  /// this object does not contain a list.
  variant_list to_list() const noexcept;

  /// Alias for @ref to_list.
  variant_list to_vector() const noexcept;

  // -- accessors --------------------------------------------------------------

  /// Grants access to the managed @ref variant_data object.
  const auto* raw() const noexcept {
    return raw_;
  }

  /// Returns a reference to the `std::variant` stored in this object.
  const auto& stl_value() const noexcept {
    return raw_->value;
  }

  /// Returns a shared pointer to the @ref envelope that owns the stored data.
  auto shared_envelope() const noexcept {
    return envelope_;
  }

  // -- operators --------------------------------------------------------------

  /// Returns a pointer to the underlying data.
  const variant* operator->() const noexcept {
    // Note: this is only implemented for "drill-down" access from iterators.
    return this;
  }

private:
  /// Pointer to the implementation.
  const variant_data* raw_;

  /// The envelope that holds (owns) the data.
  data_envelope_ptr envelope_;
};

/// Checks whether a data object can be converted to `T`.
template <class T>
bool exact_match_or_can_convert_to(const variant& x) {
  if constexpr (detail::data_tag_oracle<T>::specialized) {
    return x.get_tag() == detail::data_tag_oracle<T>::value;
  } else if constexpr (std::is_same_v<any_type, T>) {
    return true;
  } else {
    return can_convert_to<T>(x);
  }
}

// -- conversions --------------------------------------------------------------

/// Converts `what` to a string.
void convert(const variant& what, std::string& out);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const variant& what);

// -- comparison operators -----------------------------------------------------

inline bool operator<(const variant& x, const variant& y) noexcept {
  return x.raw()->value < y.raw()->value;
}

inline bool operator<=(const variant& x, const variant& y) noexcept {
  return x.raw()->value <= y.raw()->value;
}

inline bool operator>(const variant& x, const variant& y) noexcept {
  return x.raw()->value > y.raw()->value;
}

inline bool operator>=(const variant& x, const variant& y) noexcept {
  return x.raw()->value >= y.raw()->value;
}

inline bool operator==(const variant& x, const variant& y) noexcept {
  return x.raw()->value == y.raw()->value;
}

inline bool operator!=(const variant& x, const variant& y) noexcept {
  return x.raw()->value != y.raw()->value;
}

template <class Data>
std::enable_if_t<std::is_same_v<Data, data>, bool>
operator==(const Data& lhs, const variant& rhs) noexcept {
  return lhs == *rhs.raw();
}

template <class Data>
std::enable_if_t<std::is_same_v<Data, data>, bool>
operator!=(const Data& lhs, const variant& rhs) noexcept {
  return !(lhs == *rhs.raw());
}

template <class Data>
std::enable_if_t<std::is_same_v<Data, data>, bool>
operator==(const variant& lhs, const Data& rhs) noexcept {
  return *lhs.raw() == rhs;
}

template <class Data>
std::enable_if_t<std::is_same_v<Data, data>, bool>
operator!=(const variant& lhs, const Data& rhs) noexcept {
  return !(*lhs.raw() == rhs);
}

} // namespace broker
