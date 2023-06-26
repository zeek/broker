#pragma once

#include "broker/address.hh"
#include "broker/detail/bbf.hh"
#include "broker/detail/type_traits.hh"
#include "broker/enum_value.hh"
#include "broker/fwd.hh"
#include "broker/none.hh"
#include "broker/port.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"

#include <cstddef>
#include <iterator>
#include <string_view>
#include <vector>

namespace broker{

using builder_buffer = std::vector<std::byte>;

class set_builder;
class table_builder;
class vector_builder;

} // namespace broker

namespace broker::detail {

template <class T>
constexpr decltype(auto) promote(std::remove_reference_t<T>& val) noexcept {
  using val_t = std::decay_t<T>;
  if constexpr (std::is_integral_v<val_t> && !std::is_same_v<val_t, bool>) {
    if constexpr (std::is_signed_v<val_t>)
      return static_cast<integer>(val);
    else
      return static_cast<count>(val);
  } else if constexpr (std::is_same_v<val_t, std::string>) {
    return std::string_view{val};
  } else if constexpr (std::is_same_v<val_t, enum_value>) {
    return enum_value_view{val.name};
  } else {
    return static_cast<T&&>(val);
  }
}

template <class T>
constexpr decltype(auto) promote(std::remove_reference_t<T>&& val) noexcept {
  using val_t = std::decay_t<T>;
  if constexpr (std::is_integral_v<val_t> && !std::is_same_v<val_t, bool>) {
    if constexpr (std::is_signed_v<val_t>)
      return static_cast<integer>(val);
    else
      return static_cast<count>(val);
  } else if constexpr (std::is_same_v<val_t, std::string>) {
    return std::string_view{val};
  } else if constexpr (std::is_same_v<val_t, enum_value>) {
    return enum_value_view{val.name};
  } else {
    static_assert(!std::is_lvalue_reference_v<T>,
                  "cannot forward an rvalue as an lvalue");
    return static_cast<T&&>(val);
  }
}

template <class T>
struct is_builder : std::false_type {};

template <>
struct is_builder<set_builder> : std::true_type {};

template <>
struct is_builder<table_builder> : std::true_type {};

template <>
struct is_builder<vector_builder> : std::true_type {};

template <class T>
inline constexpr bool is_builder_v = is_builder<T>::value;

struct builder_access {
  template <class Builder, class T>
  static Builder& add(Builder& builder, T&& value) {
    if constexpr (is_builder_v<std::decay_t<T>>) {
      auto [first, last] = value.encoded_values();
      detail::bbf::write_sequence(value.tag(), value.num_values(), first, last,
                                  builder.adder());
    } else {
      bbf::encode(std::forward<T>(value), builder.adder());
    }
    return builder;
  }
};

} // namespace broker::detail

namespace broker {

// -- vector_builder -----------------------------------------------------------

/// Evaluates to `true` if `T` is a primitive type that can be passed to a
/// `vector_builder` by value.
template <class T>
inline constexpr bool is_builder_value_type_v =
  detail::is_one_of_v<T, none, boolean, count, integer, real, std::string_view,
                      address, subnet, port, timestamp, timespan,
                      enum_value_view>;

// -- set_builder --------------------------------------------------------------

/// A builder for constructing vectors.
class set_builder {
public:
  // -- friend types -----------------------------------------------------------

  friend struct detail::builder_access;

  // -- constructors, destructors, and assignment operators --------------------

  set_builder();

  set_builder(set_builder&&) noexcept = default;

  set_builder(const set_builder&) = default;

  set_builder& operator=(set_builder&&) noexcept = default;

  set_builder& operator=(const set_builder&) = default;

  // -- properties -------------------------------------------------------------

  /// The type of the sequence this builder is constructing.
  static constexpr data::type tag() noexcept {
    return data::type::set;
  }

  /// Returns the number of elements in the sequence.
  size_t num_values() const noexcept {
    return size_;
  }

  /// Returns the values in the builder as encoded bytes.
  std::pair<const std::byte*, const std::byte*>
  encoded_values() const noexcept {
    return detail::bbf::encoded_values(bytes_);
  }

  // -- adders ----------------------------------------------------------------

  template <class T>
  set_builder& add(T&& value) & {
    auto&& pval = detail::promote<T>(value);
    using val_t = std::decay_t<decltype(pval)>;
    static_assert(is_builder_value_type_v<val_t> || detail::is_builder_v<val_t>,
                  "value must be a valid data type or a builder");
    ++size_;
    return detail::builder_access::add(*this, pval);
  }

  template <class T>
  set_builder&& add(T&& value) && {
    return std::move(add(std::forward<T>(value)));
  }

  /// Adds all elements as a nested vector.
  template <class... Ts>
  set_builder& add_vector(Ts&&... xs) &;

  template <class... Ts>
  set_builder&& add_vector(Ts&&... xs) && {
    return std::move(add_vector(std::forward<Ts>(xs)...));
  }

  /// Adds all elements as a nested set.
  /// @pre The elements must be unique.
  template <class... Ts>
  set_builder& add_set(Ts&&... xs) &;

  template <class... Ts>
  set_builder&& add_set(Ts&&... xs) && {
    return std::move(add_set(std::forward<Ts>(xs)...));
  }

  // -- modifiers --------------------------------------------------------------

  /// Converts the sequence into a @ref data_view. The builder becomes invalid
  /// after calling this function.
  data_view build() &&;

protected:
  auto adder() {
    return std::back_inserter(bytes_);
  }

  void start_inline_vector(size_t num_elements) {
    ++size_;
    auto out = detail::bbf::write_unsigned(data::type::vector, adder());
    detail::bbf::write_varbyte(num_elements, out);
  }

  void start_inline_set(size_t num_elements) {
    ++size_;
    auto out = detail::bbf::write_unsigned(data::type::set, adder());
    detail::bbf::write_varbyte(num_elements, out);
  }

  size_t size_ = 0;
  builder_buffer bytes_;
};

// -- table_builder ------------------------------------------------------------

/// A builder for constructing vectors.
class table_builder {
public:
  // -- friend types -----------------------------------------------------------

  friend struct detail::builder_access;

  // -- constructors, destructors, and assignment operators --------------------

  table_builder();

  table_builder(table_builder&&) noexcept = default;

  table_builder(const table_builder&) = default;

  table_builder& operator=(table_builder&&) noexcept = default;

  table_builder& operator=(const table_builder&) = default;

  // -- properties -------------------------------------------------------------

  /// The type of the sequence this builder is constructing.
  static constexpr data::type tag() noexcept {
    return data::type::table;
  }

  /// Returns the number of elements in the sequence.
  size_t num_values() const noexcept {
    return size_;
  }

  /// Returns the values in the builder as encoded bytes.
  std::pair<const std::byte*, const std::byte*>
  encoded_values() const noexcept {
    return detail::bbf::encoded_values(bytes_);
  }

  // -- adders ----------------------------------------------------------------

  template <class Key, class Val>
  table_builder& add(Key&& key_arg, Val&& val_arg) & {
    auto&& key = detail::promote<Key>(key_arg);
    auto&& val = detail::promote<Val>(val_arg);
    using detail::is_builder_v;
    using key_t = std::decay_t<decltype(key)>;
    using val_t = std::decay_t<decltype(val)>;
    static_assert(is_builder_value_type_v<key_t> || is_builder_v<key_t>,
                  "key must be a valid data type or a builder");
    static_assert(is_builder_value_type_v<val_t> || is_builder_v<val_t>,
                  "value must be a valid data type or a builder");
    ++size_;
    detail::builder_access::add(*this, key);
    detail::builder_access::add(*this, val);
    return *this;
  }

  // -- rvalue overloads -------------------------------------------------------

  template <class Key, class Value>
  table_builder&& add(Key&& key, Value&& value) && {
    return std::move(add(std::forward<Key>(key), std::forward<Value>(value)));
  }

  /// Converts the sequence into a @ref data_view. The builder becomes invalid
  /// after calling this function.
  data_view build() &&;

protected:
  auto adder() {
    return std::back_inserter(bytes_);
  }

  size_t size_ = 0;
  builder_buffer bytes_;
};

/// A builder for constructing vectors.
class vector_builder {
public:
  // -- friend types -----------------------------------------------------------

  friend struct detail::builder_access;

  // -- constructors, destructors, and assignment operators --------------------

  vector_builder();

  vector_builder(vector_builder&&) noexcept = default;

  vector_builder(const vector_builder&) = default;

  vector_builder& operator=(vector_builder&&) noexcept = default;

  vector_builder& operator=(const vector_builder&) = default;

  // -- properties -------------------------------------------------------------

  /// The type of the sequence this builder is constructing.
  static constexpr data::type tag() noexcept {
    return data::type::vector;
  }

  /// Returns the number of elements in the sequence.
  size_t num_values() const noexcept {
    return size_;
  }

  /// Returns the values in the builder as encoded bytes.
  std::pair<const std::byte*, const std::byte*>
  encoded_values() const noexcept {
    return detail::bbf::encoded_values(bytes_);
  }

  // -- adders ----------------------------------------------------------------

  template <class T>
  vector_builder& add(T&& value) & {
    auto&& pval = detail::promote<T>(value);
    using val_t = std::decay_t<decltype(pval)>;
    static_assert(is_builder_value_type_v<val_t> || detail::is_builder_v<val_t>,
                  "value must be a valid data type or a builder");
    ++size_;
    return detail::builder_access::add(*this, pval);
  }

  template <class T>
  vector_builder&& add(T&& value) && {
    return std::move(add(std::forward<T>(value)));
  }

  /// Adds all elements as a nested vector.
  template <class... Ts>
  vector_builder& add_vector(Ts&&... xs) &;

  template <class... Ts>
  vector_builder&& add_vector(Ts&&... xs) && {
    return std::move(add_vector(std::forward<Ts>(xs)...));
  }

  /// Adds all elements as a nested set.
  /// @pre The elements must be unique.
  template <class... Ts>
  vector_builder& add_set(Ts&&... xs) &;

  template <class... Ts>
  vector_builder&& add_set(Ts&&... xs) && {
    return std::move(add_set(std::forward<Ts>(xs)...));
  }

  // -- modifiers --------------------------------------------------------------

  /// Converts the sequence into a @ref data_view. The builder becomes invalid
  /// after calling this function.
  data_view build() &&;

protected:
  auto adder() {
    return std::back_inserter(bytes_);
  }

  void start_inline_vector(size_t num_elements) {
    ++size_;
    auto out = detail::bbf::write_unsigned(data::type::vector, adder());
    detail::bbf::write_varbyte(num_elements, out);
  }

  void start_inline_set(size_t num_elements) {
    ++size_;
    auto out = detail::bbf::write_unsigned(data::type::set, adder());
    detail::bbf::write_varbyte(num_elements, out);
  }


  size_t size_ = 0;
  builder_buffer bytes_;
};

template <class... Ts>
set_builder& set_builder::add_vector(Ts&&... xs) & {
  start_inline_vector(sizeof...(xs));
  (detail::builder_access::add(*this, detail::promote<Ts>(xs)), ...);
  return *this;
}

template <class... Ts>
set_builder& set_builder::add_set(Ts&&... xs) & {
  start_inline_set(sizeof...(xs));
  (detail::builder_access::add(*this, detail::promote<Ts>(xs)), ...);
  return *this;
}

template <class... Ts>
vector_builder& vector_builder::add_vector(Ts&&... xs) & {
  start_inline_vector(sizeof...(xs));
  (detail::builder_access::add(*this, detail::promote<Ts>(xs)), ...);
  return *this;
}

template <class... Ts>
vector_builder& vector_builder::add_set(Ts&&... xs) & {
  start_inline_set(sizeof...(xs));
  (detail::builder_access::add(*this, detail::promote<Ts>(xs)), ...);
  return *this;
}

} // namespace broker
