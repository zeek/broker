#ifndef BROKER_DATA_HH
#define BROKER_DATA_HH

#include <cstdint>
#include <map>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "broker/address.hh"
#include "broker/enum_value.hh"
#include "broker/optional.hh"
#include "broker/port.hh"
#include "broker/subnet.hh"
#include "broker/time_duration.hh"
#include "broker/time_point.hh"
#include "broker/variant.hh"

#include "broker/detail/hash.hh"

namespace broker {

class data;

/// A container of sequential data.
using vector = std::vector<data>;

/// An associative, ordered container of unique keys.
using set = std::set<data>;

/// An associative, ordered container that maps unique keys other values.
using table = std::map<data, data>;

/// A container of sequential, optional data.  That is, the value at any given
/// index either exists or does not.
class record : detail::totally_ordered<record> {
public:
  using field = optional<data>;

  /// Default construct an empty record.
  record() = default;

  /// Construct a record from a list of fields.
  record(std::vector<field> arg_fields);

  /// @return the number of fields in the record.
  size_t size() const;

  /// @return a const reference to a field at a given offset, if it exists.
  optional<const data&> get(size_t index) const;

  /// @return a reference to a field at a given offset, if it exists.
  optional<data&> get(size_t index);

  std::vector<field> fields;
};

template <class Processor>
void serialize(Processor& proc, record& r, const unsigned) {
  proc & r.fields;
}

/// A variant-like class that may store the data associated with one of several
/// different primitive or compound types.
class data : detail::totally_ordered<data> {
public:
  enum class tag : uint8_t {
    // Primitive types
    boolean,    // bool
    count,      // uint64_t
    integer,    // int64_t
    real,       // double
    string,     // std::string
    address,    // broker::address
    subnet,     // broker::subnet
    port,       // broker::port
    time,       // broker::time_point
    duration,   // broker::time_duration
    enum_value, // broker::enum_value
    // Compound types
    set,
    table,
    vector,
    record
  };

  using types = variant<
    tag, 
    bool,
    uint64_t,
    int64_t,
    double,
    std::string, 
    address, 
    subnet, 
    port,
    time_point, 
    time_duration, 
    enum_value, 
    set, 
    table, 
    vector, 
    record
   >;

  template <class T>
  using from = detail::conditional_t<
    std::is_floating_point<T>::value,
    double,
    detail::conditional_t<
      std::is_same<T, bool>::value,
      bool,
      detail::conditional_t<
        std::is_unsigned<T>::value,
        uint64_t,
        detail::conditional_t<
          std::is_signed<T>::value,
          int64_t,
          detail::conditional_t<
            std::is_convertible<T, std::string>::value,
            std::string,
            detail::conditional_t<
              std::is_same<T, address>::value 
                || std::is_same<T, subnet>::value 
                || std::is_same<T, port>::value
                || std::is_same<T, time_point>::value 
                || std::is_same<T, time_duration>::value 
                || std::is_same<T, enum_value>::value 
                || std::is_same<T, set>::value 
                || std::is_same<T, table>::value 
                || std::is_same<T, vector>::value 
                || std::is_same<T, record>::value,
              T,
              std::false_type
            >
          >
        >
      >
    >
  >;

  template <class T>
  using type = from<typename std::decay<T>::type>;

  /// Default construct data.
  data() {
  }

  /// Constructs data.
  /// @param x The instance to construct data from.
  template <
    class T,
    typename = detail::disable_if_t<
      detail::is_same_or_derived<data, T>::value 
      || std::is_same<type<T>, std::false_type>::value>
    >
  data(T&& x)
    : value(type<T>(std::forward<T>(x))) {
  }

  types value;
};

inline record::record(std::vector<field> arg_fields)
  : fields(std::move(arg_fields)) {
}

inline size_t record::size() const {
  return fields.size();
}

inline optional<const data&> record::get(size_t index) const {
  if (index >= fields.size())
    return {};
  if (!fields[index])
    return {};
  return *fields[index];
}

inline optional<data&> record::get(size_t index) {
  if (index >= fields.size())
    return {};
  if (!fields[index])
    return {};
  return *fields[index];
}

inline bool operator==(const record& lhs, const record& rhs) {
  return lhs.fields == rhs.fields;
}

inline bool operator<(const record& lhs, const record& rhs) {
  return lhs.fields < rhs.fields;
}

inline data::types& expose(data& d) {
  return d.value;
}

inline const data::types& expose(const data& d) {
  return d.value;
}

inline bool operator==(const data& lhs, const data& rhs) {
  return lhs.value == rhs.value;
}

inline bool operator<(const data& lhs, const data& rhs) {
  return lhs.value < rhs.value;
}

template <class Processor>
void serialize(Processor& proc, data& d, const unsigned) {
  proc& d.value;
}

std::string to_string(const data&);
std::string to_string(const vector&);
std::string to_string(const set&);
std::string to_string(const table&);
std::string to_string(const record&);

std::ostream& operator<<(std::ostream&, const broker::data&);
std::ostream& operator<<(std::ostream&, const broker::vector&);
std::ostream& operator<<(std::ostream&, const broker::set&);
std::ostream& operator<<(std::ostream&, const broker::table&);
std::ostream& operator<<(std::ostream&, const broker::record&);

} // namespace broker

namespace std {

template <>
struct hash<broker::data> {
  using value_type = broker::data::types;
  using result_type = typename std::hash<value_type>::result_type;
  using argument_type = broker::data;

  inline result_type operator()(const argument_type& d) const {
    return std::hash<value_type>{}(d.value);
  }
};

template <>
struct hash<broker::set> : broker::detail::container_hasher<broker::set> {};

template <>
struct hash<broker::vector> 
  : broker::detail::container_hasher<broker::vector> {};

template <>
struct hash<broker::table::value_type> {
  using result_type = typename std::hash<broker::data>::result_type;
  using argument_type = broker::table::value_type;

  inline result_type operator()(const argument_type& d) const {
    result_type rval{};
    broker::detail::hash_combine<broker::data>(rval, d.first);
    broker::detail::hash_combine<broker::data>(rval, d.second);
    return rval;
  }
};

template <>
struct hash<broker::table> : broker::detail::container_hasher<broker::table> {};

template <>
struct hash<broker::record> {
  using result_type = typename broker::detail::container_hasher<decltype(
    broker::record::fields)>::result_type;
  using argument_type = broker::record;

  inline result_type operator()(const argument_type& d) const {
    return broker::detail::container_hasher<decltype(broker::record::fields)>{}(
      d.fields);
  }
};
}

#endif // BROKER_DATA_HH
