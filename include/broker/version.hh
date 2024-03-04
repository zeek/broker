#pragma once

#include <string>

namespace broker::version {

/// The type used for version numbers.
using type = unsigned;

constexpr type major = 6;
constexpr type minor = 2;
constexpr type patch = 0;
constexpr auto suffix = "";

constexpr type protocol = 2;

/// Determines whether two Broker protocol versions are compatible.
/// @param v The version of the other broker.
/// @returns `true` iff *v* is compatible to this version.
inline bool compatible(type v) {
  return v == protocol;
}

/// Generates a version string of the form `major.minor.patch`.
/// @returns A string representing the Broker version.
std::string string();

} // namespace broker::version
