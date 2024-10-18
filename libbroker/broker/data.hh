#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "broker/address.hh"
#include "broker/bad_variant_access.hh"
#include "broker/convert.hh"
#include "broker/detail/type_traits.hh"
#include "broker/enum_value.hh"
#include "broker/fwd.hh"
#include "broker/none.hh"
#include "broker/port.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"
#include "broker/variant_tag.hh"

namespace broker {

class data;

/// A container of sequential data.
using vector = std::vector<data>;

/// @relates vector
void convert(const vector& v, std::string& str);

/// An associative, ordered container of unique keys.
using set = std::set<data>;

/// @relates set
void convert(const set& s, std::string& str);

/// An associative, ordered container that maps unique keys to values.
using table = std::map<data, data>;

/// @relates table
void convert(const table& t, std::string& str);

using data_variant = std::variant<none, boolean, count, integer, real,
                                  std::string, address, subnet, port, timestamp,
                                  timespan, enum_value, set, table, vector>;

/// A variant class that may store the data associated with one of several
/// different primitive or compound types.
class data {
public:
  using type = variant_tag;

  template <class T>
  static constexpr auto tag_of() {
    if constexpr (std::is_floating_point_v<T>) {
      return detail::tag<real>{};
    } else if constexpr (std::is_same_v<T, bool>) {
      return detail::tag<boolean>{};
    } else if constexpr (std::is_unsigned_v<T>) {
      return detail::tag<count>{};
    } else if constexpr (std::is_signed_v<T>) {
      return detail::tag<integer>{};
    } else if constexpr (std::is_same_v<T, std::string>
                         || std::is_same_v<T, std::string_view>
                         || std::is_same_v<T, char*>
                         || std::is_same_v<T, const char*>) {
      return detail::tag<std::string>{};
    } else if constexpr (std::is_same_v<T, timestamp>      //
                         || std::is_same_v<T, timespan>    //
                         || std::is_same_v<T, enum_value>  //
                         || std::is_same_v<T, address>     //
                         || std::is_same_v<T, subnet>      //
                         || std::is_same_v<T, port>        //
                         || std::is_same_v<T, broker::set> //
                         || std::is_same_v<T, table>       //
                         || std::is_same_v<T, vector>) {
      return detail::tag<T>{};
    } else {
      // Return 'void'.
    }
  }

  /// SFINAE utility.
  template <class T>
  using from = typename decltype(tag_of<T>())::type;

  data() = default;

  data(data&&) = default;

  data(const data&) = default;

  /// Constructs an empty data value in `none` state.
  data(none) {
    // nop
  }

  explicit data(enum_value_view value) {
    data_ = enum_value{std::string{value.name}};
  }

  /// Constructs a data value from one of the possible data types.
  template <class T, class Converted = from<T>>
  data(T x) {
    if constexpr (std::is_same_v<T, Converted>) {
      data_ = std::move(x);
    } else {
      data_ = Converted{std::move(x)};
    }
  }

  data& operator=(data&&) = default;

  data& operator=(const data&) = default;

  /// Constructs a data value from one of the possible data types.
  template <class T, class Converted = from<T>>
  data& operator=(T x) {
    if constexpr (std::is_same_v<T, Converted>) {
      data_ = std::move(x);
    } else {
      data_ = Converted{std::move(x)};
    }
    return *this;
  }

  // -- properties -------------------------------------------------------------

  /// Returns a string representation of the stored type.
  const char* get_type_name() const {
    return detail::cpp_type_name(get_type());
  }

  /// Returns the type tag of the stored type.
  type get_type() const;

  /// Returns the type tag of the stored type.
  type get_tag() const {
    return get_type();
  }

  static data from_type(type);

  /// Needed by `get` function overloads.
  [[nodiscard]] data_variant& get_data() noexcept {
    return data_;
  }

  /// Needed by `get` function overloads.
  [[nodiscard]] const data_variant& get_data() const noexcept {
    return data_;
  }

  /// Checks whether this view contains the `nil` value.
  bool is_none() const noexcept {
    return get_type() == type::none;
  }

  /// Checks whether this view contains a boolean.
  bool is_boolean() const noexcept {
    return get_type() == type::boolean;
  }

  /// Checks whether this view contains a count.
  bool is_count() const noexcept {
    return get_type() == type::count;
  }

  /// Checks whether this view contains a integer.
  bool is_integer() const noexcept {
    return get_type() == type::integer;
  }

  /// Checks whether this view contains a real.
  bool is_real() const noexcept {
    return get_type() == type::real;
  }

  /// Checks whether this view contains a count.
  bool is_string() const noexcept {
    return get_type() == type::string;
  }

  /// Checks whether this view contains a count.
  bool is_address() const noexcept {
    return get_type() == type::address;
  }

  /// Checks whether this view contains a count.
  bool is_subnet() const noexcept {
    return get_type() == type::subnet;
  }

  /// Checks whether this view contains a count.
  bool is_port() const noexcept {
    return get_type() == type::port;
  }

  /// Checks whether this view contains a count.
  bool is_timestamp() const noexcept {
    return get_type() == type::timestamp;
  }

  /// Checks whether this view contains a count.
  bool is_timespan() const noexcept {
    return get_type() == type::timespan;
  }

  /// Checks whether this view contains a count.
  bool is_enum_value() const noexcept {
    return get_type() == type::enum_value;
  }

  /// Checks whether this view contains a set.
  bool is_set() const noexcept {
    return get_type() == type::set;
  }

  /// Checks whether this view contains a table.
  bool is_table() const noexcept {
    return get_type() == type::table;
  }

  /// Checks whether this view contains a list.
  bool is_list() const noexcept {
    return get_type() == type::vector;
  }

  // -- conversions ------------------------------------------------------------

  /// Retrieves the @c boolean value or returns @p fallback if this object does
  /// not contain a @c boolean.
  bool to_boolean(bool fallback = false) const noexcept {
    if (auto* val = std::get_if<boolean>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the @c count value or returns @p fallback if this object does
  /// not contain a @c count.
  count to_count(count fallback = 0) const noexcept {
    if (auto* val = std::get_if<count>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the @c integer value or returns @p fallback if this object does
  /// not contain a @c integer.
  integer to_integer(integer fallback = 0) const noexcept {
    if (auto* val = std::get_if<integer>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the @c real value or returns @p fallback if this object does
  /// not contain a @c real.
  real to_real(real fallback = 0) const noexcept {
    if (auto* val = std::get_if<real>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the string value or returns an empty string if this object does
  /// not contain a string.
  std::string_view to_string() const noexcept {
    if (auto* val = std::get_if<std::string>(&data_))
      return *val;
    return std::string_view{};
  }

  /// Retrieves the @c address value or returns @p fallback if this object does
  /// not contain a @c address.
  address to_address(const address& fallback = {}) const noexcept {
    if (auto* val = std::get_if<address>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the @c subnet value or returns @p fallback if this object does
  /// not contain a @c subnet.
  subnet to_subnet(const subnet& fallback = {}) const noexcept {
    if (auto* val = std::get_if<subnet>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the @c port value or returns @p fallback if this object does
  /// not contain a @c port.
  port to_port(port fallback = {}) const noexcept {
    if (auto* val = std::get_if<port>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the @c timestamp value or returns @p fallback if this object
  /// does not contain a @c timestamp.
  timestamp to_timestamp(timestamp fallback = {}) const noexcept {
    if (auto* val = std::get_if<timestamp>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the @c timespan value or returns @p fallback if this object does
  /// not contain a @c timespan.
  timespan to_timespan(timespan fallback = {}) const noexcept {
    if (auto* val = std::get_if<timespan>(&data_))
      return *val;
    return fallback;
  }

  /// Retrieves the enum_value value or returns @p fallback if this object does
  /// not contain a enum_value.
  const enum_value& to_enum_value() const noexcept;

  /// Converts the stored data as a list (`vector`). If the stored data is
  /// not a list, the result is an empty list.
  [[nodiscard]] const vector& to_list() const;

  /// Returns a reference to the `std::variant` stored in this object.
  const auto& stl_value() const noexcept {
    return data_;
  }

  /// Deserializes the data from a binary representation.
  bool deserialize(const std::byte* payload, size_t payload_size);

private:
  data_variant data_;
};

namespace detail {

template <data::type Value>
struct data_tag_oracle_impl {
  static constexpr bool specialized = true;

  static constexpr data::type value = Value;
};

template <class T>
struct data_tag_oracle {
  static constexpr bool specialized = false;
};

template <>
struct data_tag_oracle<std::string> : data_tag_oracle_impl<data::type::string> {
};

template <>
struct data_tag_oracle<std::string_view>
  : data_tag_oracle_impl<data::type::string> {};

template <>
struct data_tag_oracle<enum_value_view>
  : data_tag_oracle_impl<data::type::enum_value> {};

template <>
struct data_tag_oracle<variant_list> : data_tag_oracle_impl<data::type::list> {
};

template <>
struct data_tag_oracle<variant_set> : data_tag_oracle_impl<data::type::set> {};

template <>
struct data_tag_oracle<variant_table>
  : data_tag_oracle_impl<data::type::table> {};

#define DATA_TAG_ORACLE(type_name)                                             \
  template <>                                                                  \
  struct data_tag_oracle<type_name>                                            \
    : data_tag_oracle_impl<data::type::type_name> {}

DATA_TAG_ORACLE(none);
DATA_TAG_ORACLE(boolean);
DATA_TAG_ORACLE(count);
DATA_TAG_ORACLE(integer);
DATA_TAG_ORACLE(real);
DATA_TAG_ORACLE(address);
DATA_TAG_ORACLE(subnet);
DATA_TAG_ORACLE(port);
DATA_TAG_ORACLE(timestamp);
DATA_TAG_ORACLE(timespan);
DATA_TAG_ORACLE(enum_value);
DATA_TAG_ORACLE(set);
DATA_TAG_ORACLE(table);
DATA_TAG_ORACLE(vector);

#undef DATA_TAG_ORACLE

} // namespace detail

/// Alias for `detail::data_tag_oracle<T>::value`.
template <class T>
inline constexpr data::type data_tag_v = detail::data_tag_oracle<T>::value;

/// Returns the `data::type` tag for `T`.
/// @relates data
template <class T>
constexpr data::type data_tag() noexcept {
  return detail::data_tag_oracle<T>::value;
}

template <class Inspector>
bool inspect(Inspector& f, data::type& x) {
  auto get = [&] { return static_cast<uint8_t>(x); };
  auto set = [&](uint8_t val) {
    if (val <= static_cast<uint8_t>(data::type::vector)) {
      x = static_cast<data::type>(val);
      return true;
    } else {
      return false;
    }
  };
  return f.apply(get, set);
}

/// @relates data
template <class Inspector>
bool inspect(Inspector& f, data& x) {
  return f.object(x).fields(f.field("data", x.get_data()));
}

namespace detail {

struct kvp_view {
  broker::data* key;
  broker::data* value;
};

template <class Inspector>
bool inspect(Inspector& f, kvp_view& x) {
  return f.object(x).fields(f.field("key", *x.key), f.field("value", *x.value));
}

} // namespace detail

/// Custom inspection for Broker tables. The default inspect does not work when
/// serializing to JSON, because JSON only supports string keys. We get around
/// this problem by presenting a table as a list of key-value pairs to the
/// inspector.
/// @relates data
template <class Inspector>
bool inspect(Inspector& f, broker::table& tbl) {
  if constexpr (Inspector::is_loading) {
    size_t n = 0;
    auto load_values = [&] {
      tbl.clear();
      for (size_t i = 0; i < n; ++i) {
        broker::data key;
        broker::data value;
        broker::detail::kvp_view view{&key, &value};
        if (!f.apply(view))
          return false;
        if (!tbl.emplace(std::move(key), std::move(value)).second)
          return false;
      }
      return true;
    };
    return f.begin_sequence(n) && load_values() && f.end_sequence();
  } else {
    auto save_values = [&] {
      for (auto& kvp : tbl) {
        detail::kvp_view view{&const_cast<broker::data&>(kvp.first),
                              &kvp.second};
        if (!f.apply(view))
          return false;
      }
      return true;
    };
    return f.begin_sequence(tbl.size()) && save_values() && f.end_sequence();
  }
}

/// @relates data
void convert(const data& x, std::string& str);

/// @relates data
bool convert(const data& x, endpoint_id& node);

/// @relates data
bool convert(const endpoint_id& node, data& x);

/// @relates data
std::string to_string(const expected<data>& x);

inline bool operator<(const data& x, const data& y) {
  return x.get_data() < y.get_data();
}

inline bool operator<=(const data& x, const data& y) {
  return x.get_data() <= y.get_data();
}

inline bool operator>(const data& x, const data& y) {
  return x.get_data() > y.get_data();
}

inline bool operator>=(const data& x, const data& y) {
  return x.get_data() >= y.get_data();
}

inline bool operator==(const data& x, const data& y) {
  return x.get_data() == y.get_data();
}

inline bool operator!=(const data& x, const data& y) {
  return x.get_data() != y.get_data();
}

// --- compatibility/wrapper functionality (may be removed later) --------------

template <class T>
bool is(const data& x) {
  return std::holds_alternative<T>(x.get_data());
}

template <class T>
bool holds_alternative(const data& x) {
  return std::holds_alternative<T>(x.get_data());
}

template <class T>
T* get_if(data* x) {
  return std::get_if<T>(std::addressof(x->get_data()));
}

template <class T>
T* get_if(data& x) {
  return std::get_if<T>(std::addressof(x.get_data()));
}

template <class T>
const T* get_if(const data* x) {
  return std::get_if<T>(std::addressof(x->get_data()));
}
template <class T>
const T* get_if(const data& x) {
  return std::get_if<T>(std::addressof(x.get_data()));
}

template <class T>
T& get(data& x) {
  if (auto ptr = get_if<T>(&x))
    return *ptr;
  else
    throw bad_variant_access{};
}

template <class T>
const T& get(const data& x) {
  if (auto ptr = get_if<T>(&x))
    return *ptr;
  else
    throw bad_variant_access{};
}

template <class Visitor>
decltype(auto) visit(Visitor&& visitor, data& x) {
  return std::visit(std::forward<Visitor>(visitor), x.get_data());
}

template <class Visitor>
decltype(auto) visit(Visitor&& visitor, const data& x) {
  return std::visit(std::forward<Visitor>(visitor), x.get_data());
}

template <class Visitor>
decltype(auto) visit(Visitor&& visitor, data&& x) {
  return std::visit(std::forward<Visitor>(visitor), std::move(x.get_data()));
}

// --- convenience functions ---------------------------------------------------

/// Wildcard for `contains` to skip type check at a specific location.
struct any_type {};

template <class T>
bool exact_match_or_can_convert_to(const data& x) {
  if constexpr (detail::data_tag_oracle<T>::specialized) {
    return is<T>(x);
  } else if constexpr (std::is_same_v<any_type, T>) {
    return true;
  } else {
    return can_convert_to<T>(x);
  }
}

template <class... Ts, size_t... Is>
bool contains_impl(const vector& xs, std::index_sequence<Is...>) {
  return xs.size() == sizeof...(Ts)
         && (exact_match_or_can_convert_to<Ts>(xs[Is]) && ...);
}

/// Checks whether `xs` contains values of types `Ts...`. Performs "fuzzy"
/// matching by calling `can_convert_to<T>` for any `T` that is not part of the
/// variant.
template <class... Ts>
bool contains(const vector& xs) {
  return contains_impl<Ts...>(xs, std::make_index_sequence<sizeof...(Ts)>{});
}

template <class... Ts>
bool contains(const data& x) {
  if (auto xs = get_if<vector>(x))
    return contains<Ts...>(*xs);
  else
    return false;
}
} // namespace broker

namespace broker::detail {

size_t fnv_hash(const broker::data& x);

size_t fnv_hash(const broker::set& x);

size_t fnv_hash(const broker::vector& x);

size_t fnv_hash(const broker::table::value_type& x);

size_t fnv_hash(const broker::table& x);

} // namespace broker::detail

// --- implementations of std::hash --------------------------------------------

namespace std {

template <>
struct hash<broker::data> {
  size_t operator()(const broker::data& x) const {
    return broker::detail::fnv_hash(x);
  }
};

template <>
struct hash<broker::set> {
  size_t operator()(const broker::set& x) const {
    return broker::detail::fnv_hash(x);
  }
};

template <>
struct hash<broker::vector> {
  size_t operator()(const broker::vector& x) const {
    return broker::detail::fnv_hash(x);
  }
};

template <>
struct hash<broker::table::value_type> {
  size_t operator()(const broker::table::value_type& x) const {
    return broker::detail::fnv_hash(x);
  }
};

template <>
struct hash<broker::table> {
  size_t operator()(const broker::table& x) const {
    return broker::detail::fnv_hash(x);
  }
};

} // namespace std
