#pragma once

#include <cstdint>

namespace broker{

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

} // namespace broker
