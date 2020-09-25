#pragma once

// TODO: compatibility header for CAF < 0.18. Drop when setting the minimum
//       required CAF version to ≥ 0.18.

#include <type_traits>

namespace broker::detail {

/// Evaulates to `true` if the `Inspector` uses the CAF inspection API prior to
/// CAF 0.18, `false` otherwise.
template <class Inspector>
constexpr bool is_legacy_inspector
  = !std::is_same<typename Inspector::result_type, bool>::value;

} // namespace broker::detail
