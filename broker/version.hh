#ifndef BROKER_VERSION_HH
#define BROKER_VERSION_HH

namespace broker {
namespace version {

/// The type used for version numbers.
using type = unsigned;

// TODO: make this a CMake definition.
constexpr type major = 0;
constexpr type minor = 1;
constexpr type patch = 0;

constexpr type protocol = 1;

/// Determines whether two Broker protocol versions are compatible.
/// @param v The version of the other broker.
/// @returns `true` iff *v* is compatible to this version.
inline bool compatible(type v) {
  return v == protocol;
}

} // namespace version
} // namespace broker

#endif // BROKER_VERSION_HH
