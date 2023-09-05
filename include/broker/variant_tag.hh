#pragma once

#include <cstdint>
#include <string_view>

namespace broker {

/// A tag that discriminates the type of a @ref data or @ref variant object.
enum class variant_tag : uint8_t {
  // Warning: the values *must* have the same order as `data_variant`, because
  // the integer value for this tag must be equal to `get_data().index()`.
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
  list,
  vector = list, // alias for backward compatibility
};

constexpr std::string_view json_name(variant_tag tag) {
  switch (tag) {
    default:
      return "none";
    case variant_tag::boolean:
      return "boolean";
    case variant_tag::count:
      return "count";
    case variant_tag::integer:
      return "integer";
    case variant_tag::real:
      return "real";
    case variant_tag::string:
      return "string";
    case variant_tag::address:
      return "address";
    case variant_tag::subnet:
      return "subnet";
    case variant_tag::port:
      return "port";
    case variant_tag::timestamp:
      return "timestamp";
    case variant_tag::timespan:
      return "timespan";
    case variant_tag::enum_value:
      return "enum-value";
    case variant_tag::set:
      return "set";
    case variant_tag::table:
      return "table";
    case variant_tag::list:
      return "vector";
  }
}

} // namespace broker
