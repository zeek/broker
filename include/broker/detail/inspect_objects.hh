#pragma once

#include "broker/error.hh"

#include <caf/config.hpp>

namespace broker::detail {

// Wraps object inspection and always returns an error for backwards
// compatibility with CAF 0.17 versions.
template <class Inspector, class... Ts>
error inspect_objects(Inspector& f, Ts&... xs) {
#if CAF_VERSION >= 1800
  if (f.apply_objects(xs...))
    return {};
  return f.get_error();
#else
  return f(xs...);
#endif
}

} // namespace broker::detail
