#ifndef BROKER_SPAWN_FLAGS_HH
#define BROKER_SPAWN_FLAGS_HH

namespace broker {

/// Flags that control how to spawn endpoints.
enum class spawn_flags : int {
  no_flags = 0x00,
  blocking_flag = 0x01,
  nonblocking_flag = 0x02,
};

/// Denotes default settings.
/// @see spawn_flags
constexpr spawn_flags no_spawn_flags = spawn_flags::no_flags;

/// Causes `spawn` to create a blocking endpoint.
/// @see spawn_flags
constexpr spawn_flags blocking = spawn_flags::blocking_flag;

/// Causes `spawn` to create a nonblocking endpoint.
/// @see spawn_flags
constexpr spawn_flags nonblocking = spawn_flags::nonblocking_flag;

/// Checks wheter `haystack` contains `needle`.
/// @see spawn_flags
constexpr bool has_spawn_flags(spawn_flags haystack, spawn_flags needle) {
  return (static_cast<int>(haystack) & static_cast<int>(needle)) != 0;
}

} // namespace broker

#endif // BROKER_SPAWN_FLAGS_HH
