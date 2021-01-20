#pragma once

#include <string>
#include <type_traits>

namespace broker::detail {

template <class Inspector, class Enumeration>
bool enum_inspect(Inspector& f, Enumeration& x) {
  using integer_type = std::underlying_type_t<Enumeration>;
  if (f.has_human_readable_format()) {
    auto get = [&x] {
      std::string result;
      convert(x, result);
      return result;
    };
    auto set = [&x](const std::string& str) { return convert(str, x); };
    return f.apply(get, set);
  } else {
    auto get = [&x] { return static_cast<integer_type>(x); };
    auto set = [&x](integer_type val) { return convert(val, x); };
    return f.apply(get, set);
  }
}

} // namespace broker::detail
