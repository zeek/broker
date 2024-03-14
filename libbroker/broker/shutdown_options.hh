#pragma once

#include <cstdint>
#include <string>

namespace broker {

class shutdown_options {
public:
  enum flag {
    await_stores_on_shutdown = 0x01,
  };

  constexpr bool contains(flag f) const noexcept {
    return (flags_ & static_cast<uint8_t>(f)) != 0;
  }

  constexpr void set(flag f) noexcept {
    flags_ |= static_cast<uint8_t>(f);
  }

  constexpr void unset(flag f) noexcept {
    flags_ &= ~static_cast<uint8_t>(f);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, shutdown_options& x) {
    return f.object(x).fields(f.field("flags", x.flags_));
  }

private:
  uint8_t flags_ = 0;
};

std::string to_string(shutdown_options options);

} // namespace broker
