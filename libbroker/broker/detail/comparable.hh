#pragma once

namespace broker::detail {

/// Barton–Nackman trick implementation.
template <class Derived, class T = Derived>
class comparable {
  friend bool operator==(const Derived& lhs, const T& rhs) noexcept {
    return lhs.compare(rhs) == 0;
  }

  friend bool operator==(const T& lhs, const Derived& rhs) noexcept {
    return rhs.compare(lhs) == 0;
  }

  friend bool operator!=(const Derived& lhs, const T& rhs) noexcept {
    return lhs.compare(rhs) != 0;
  }

  friend bool operator!=(const T& lhs, const Derived& rhs) noexcept {
    return rhs.compare(lhs) != 0;
  }

  friend bool operator<(const Derived& lhs, const T& rhs) noexcept {
    return lhs.compare(rhs) < 0;
  }

  friend bool operator>(const Derived& lhs, const T& rhs) noexcept {
    return lhs.compare(rhs) > 0;
  }

  friend bool operator<(const T& lhs, const Derived& rhs) noexcept {
    return rhs > lhs;
  }

  friend bool operator>(const T& lhs, const Derived& rhs) noexcept {
    return rhs < lhs;
  }

  friend bool operator<=(const Derived& lhs, const T& rhs) noexcept {
    return lhs.compare(rhs) <= 0;
  }

  friend bool operator>=(const Derived& lhs, const T& rhs) noexcept {
    return lhs.compare(rhs) >= 0;
  }

  friend bool operator<=(const T& lhs, const Derived& rhs) noexcept {
    return rhs >= lhs;
  }

  friend bool operator>=(const T& lhs, const Derived& rhs) noexcept {
    return rhs <= lhs;
  }
};

template <class Derived>
class comparable<Derived, Derived> {
  friend bool operator==(const Derived& lhs, const Derived& rhs) noexcept {
    return lhs.compare(rhs) == 0;
  }

  friend bool operator!=(const Derived& lhs, const Derived& rhs) noexcept {
    return lhs.compare(rhs) != 0;
  }

  friend bool operator<(const Derived& lhs, const Derived& rhs) noexcept {
    return lhs.compare(rhs) < 0;
  }

  friend bool operator<=(const Derived& lhs, const Derived& rhs) noexcept {
    return lhs.compare(rhs) <= 0;
  }

  friend bool operator>(const Derived& lhs, const Derived& rhs) noexcept {
    return lhs.compare(rhs) > 0;
  }

  friend bool operator>=(const Derived& lhs, const Derived& rhs) noexcept {
    return lhs.compare(rhs) >= 0;
  }
};

} // namespace broker::detail
