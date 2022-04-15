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

namespace broker {

class data;

/// A container of sequential data.
using vector = std::vector<data>;

/// @relates vector
bool convert(const vector& v, std::string& str);

/// An associative, ordered container of unique keys.
using set = std::set<data>;

/// @relates set
bool convert(const set& s, std::string& str);

/// An associative, ordered container that maps unique keys to values.
using table = std::map<data, data>;

/// @relates table
bool convert(const table& t, std::string& str);

using data_variant = std::variant<
  none,
  boolean,
  count,
  integer,
  real,
  std::string,
  address,
  subnet,
  port,
  timestamp,
  timespan,
  enum_value,
  set,
  table,
  vector
>;

/// A variant class that may store the data associated with one of several
/// different primitive or compound types.
class data {
public:
  // Warning: *must* have the same order as `data_variant`, because the integer
  // value for this tag must be equal to `get_data().index()`.
  enum class type : uint8_t {
    none,
    boolean,
    count,
    integer,
    real,
    string,
    address,
    subnet,
    port,
    timestamp,
    timespan,
    enum_value,
    set,
    table,
    vector,
  };

	template <class T>
	using from = detail::conditional_t<
        std::is_floating_point<T>::value,
        real,
        detail::conditional_t<
          std::is_same<T, bool>::value,
          boolean,
          detail::conditional_t<
            std::is_unsigned<T>::value,
            count,
            detail::conditional_t<
              std::is_signed<T>::value,
              integer,
              detail::conditional_t<
                std::is_convertible<T, std::string>::value,
                std::string,
                detail::conditional_t<
                  std::is_same<T, timestamp>::value
                    || std::is_same<T, timespan>::value
                    || std::is_same<T, enum_value>::value
	                  || std::is_same<T, address>::value
                    || std::is_same<T, subnet>::value
                    || std::is_same<T, port>::value
                    || std::is_same<T, broker::set>::value
                    || std::is_same<T, table>::value
                    || std::is_same<T, vector>::value,
                  T,
                  std::false_type
                >
              >
            >
          >
        >
      >;

  /// Default-constructs an empty data value in `none` state.
  data(none = nil) {
    // nop
  }

  /// Constructs a data value from one of the possible data types.
	template <
	  class T,
	  class = detail::disable_if_t<
              detail::is_same_or_derived<data, T>::value
                || std::is_same<
                     from<detail::decay_t<T>>,
                     std::false_type
                   >::value
            >
	>
	data(T&& x) : data_(from<detail::decay_t<T>>(std::forward<T>(x))) {
	  // nop
	}

  /// Returns a string representation of the stored type.
  const char* get_type_name() const;

  /// Returns the type tag of the stored type.
  type get_type() const;

  static data from_type(type);

  /// Needed by `get` function overloads.
  [[nodiscard]] data_variant& get_data() noexcept {
    return data_;
  }

  /// Needed by `get` function overloads.
  [[nodiscard]] const data_variant& get_data() const noexcept {
    return data_;
  }

private:
  data_variant data_;
};

namespace detail {

template <data::type Value>
using data_tag_token = std::integral_constant<data::type, Value>;

template <class T>
struct data_tag_oracle;

template <>
struct data_tag_oracle<std::string> : data_tag_token<data::type::string> {};

#define DATA_TAG_ORACLE(type_name)                                             \
  template <>                                                                  \
  struct data_tag_oracle<type_name> : data_tag_token<data::type::type_name> {}

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

/// Returns the `data::type` tag for `T`.
/// @relates data
template <class T>
constexpr data::type data_tag() noexcept {
  return detail::data_tag_oracle<T>::value;
}

/// Checks whether `data_tag` is defined for `T`.
/// @relates data
template <class T>
constexpr bool has_data_tag() {
  return detail::is_complete<detail::data_tag_oracle<T>>;
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
bool convert(const data& x, std::string& str);

/// @relates data
bool convert(const data& x, endpoint_id& node);

/// @relates data
bool convert(const endpoint_id& node, data& x);

/// @relates data
inline std::string to_string(const data& x) {
  std::string str;
  convert(x, str);
  return str;
}

/// @relates data
std::string to_string(const expected<data>& x);

/// @relates data
std::string to_string(const vector& x);

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
  if constexpr (detail::is_complete<detail::data_tag_oracle<T>>)
    return is<T>(x);
  else if constexpr (std::is_same<any_type, T>::value)
    return true;
  else
    return can_convert_to<T>(x);
}

template <size_t... Is, class... Ts>
bool contains_impl(const vector& xs, std::index_sequence<Is...>,
                   detail::type_list<Ts...>) {
  return xs.size() == sizeof...(Ts)
         && (exact_match_or_can_convert_to<Ts>(xs[Is]) && ...);
}

/// Checks whether `xs` contains values of types `Ts...`. Performs "fuzzy"
/// matching by calling `can_convert_to<T>` for any `T` that is not part of the
/// variant.
template <class...Ts>
bool contains(const vector& xs) {
  return contains_impl(xs, std::make_index_sequence<sizeof...(Ts)>{},
                       detail::type_list<Ts...>{});
}

template <class...Ts>
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
  size_t operator()(const broker::set& x) const{
    return broker::detail::fnv_hash(x);
  }
};

template <>
struct hash<broker::vector> {
  size_t operator()(const broker::vector& x) const{
    return broker::detail::fnv_hash(x);
  }
};

template <>
struct hash<broker::table::value_type> {
  size_t operator()(const broker::table::value_type& x) const{
    return broker::detail::fnv_hash(x);
  }
};

template <>
struct hash<broker::table> {
  size_t operator()(const broker::table& x) const{
    return broker::detail::fnv_hash(x);
  }
};

} // namespace std
