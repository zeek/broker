#pragma once

#include <optional>
#include <utility>

namespace broker::detail {

/// A lightweight scope guard implementation.
template <class Fn>
class scope_guard {
public:
  scope_guard(Fn f) : fn_(std::move(f)) {
    // nop
  }

  scope_guard(scope_guard&& other) {
    fn_.swap(other.fn_);
  }

  scope_guard() = delete;

  scope_guard(const scope_guard&) = delete;

  scope_guard& operator=(const scope_guard&) = delete;

  ~scope_guard() {
    if (fn_)
      (*fn_)();
  }

  /// Disables execution of the cleanup code at scope exit.
  void disable() noexcept {
    fn_ = std::nullopt;
  }

private:
  std::optional<Fn> fn_;
};

/// Creates a guard that executes @p fn at scope exit.
/// @relates scope_guard
template <class Fn>
scope_guard<Fn> make_scope_guard(Fn fn) {
  return {std::move(fn)};
}

} // namespace broker::detail
