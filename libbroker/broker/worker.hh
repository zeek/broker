#pragma once

#include "broker/config.hh"
#include "broker/detail/comparable.hh"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>

namespace broker {

/// A handle to a background worker.
class worker : detail::comparable<worker> {
public:
  // -- member types -----------------------------------------------------------

  struct impl;

  // -- constants --------------------------------------------------------------

#ifdef BROKER_WINDOWS
  static constexpr size_t obj_size = sizeof(void*) * 2;
#else
  static constexpr size_t obj_size = sizeof(void*);
#endif

  // --- construction and destruction ------------------------------------------

  worker() noexcept;

  worker(worker&&) noexcept;

  worker(const worker&) noexcept;

  explicit worker(const impl*) noexcept;

  worker& operator=(worker&&) noexcept;

  worker& operator=(const worker&) noexcept;

  worker& operator=(std::nullptr_t) noexcept;

  ~worker();

  // -- properties -------------------------------------------------------------

  /// Queries whether this node is *not* default-constructed.
  bool valid() const noexcept;

  /// Queries whether this node is *not* default-constructed.
  explicit operator bool() const noexcept {
    return valid();
  }

  /// Queries whether this node is default-constructed.
  bool operator!() const noexcept {
    return !valid();
  }

  /// Exchanges the value of this object with `other`.
  void swap(worker& other) noexcept;

  /// Compares this instance to `other`.
  /// @returns -1 if `*this < other`, 0 if `*this == other`, and 1 otherwise.
  intptr_t compare(const worker& other) const noexcept;

  /// Returns a has value for the ID.
  size_t hash() const noexcept;

  /// Returns a pointer to the native representation.
  [[nodiscard]] impl* native_ptr() noexcept;

  /// Returns a pointer to the native representation.
  [[nodiscard]] const impl* native_ptr() const noexcept;

private:
  std::byte obj_[obj_size];
};

inline bool operator==(const worker& hdl, std::nullptr_t) {
  return !hdl.valid();
}

inline bool operator==(std::nullptr_t, const worker& hdl) {
  return !hdl.valid();
}

inline bool operator!=(const worker& hdl, std::nullptr_t) {
  return hdl.valid();
}

inline bool operator!=(std::nullptr_t, const worker& hdl) {
  return hdl.valid();
}

std::string to_string(const worker& x);

} // namespace broker

namespace std {

template <>
struct hash<broker::worker> {
  size_t operator()(const broker::worker& x) const noexcept {
    return x.hash();
  }
};

} // namespace std
