#pragma once

#include "broker/config.hh"
#include "broker/data.hh"
#include "broker/variant.hh"
#include "broker/variant_data.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace broker::format::txt::v1 {

/// Render the `nil` value to `out`.
template <class OutIter>
OutIter encode(none, OutIter out) {
  using namespace std::literals;
  auto str = "nil"sv;
  return std::copy(str.begin(), str.end(), out);
}

/// Renders `value` to `out` as `T` for `true` and `F` for `false`.
template <class T, class OutIter>
std::enable_if_t<std::is_integral_v<T>, OutIter> encode(T value, OutIter out) {
  if constexpr (std::is_same_v<T, bool>) {
    *out++ = value ? 'T' : 'F';
    return out;
  } else if constexpr (std::is_same_v<T, count>) {
    // An integer can at most have 20 digits (UINT64_MAX).
    char buf[24];
    auto size = std::snprintf(buf, 24, "%llu",
                              static_cast<long long unsigned>(value));
    return std::copy(buf, buf + size, out);
  } else {
    static_assert(std::is_same_v<T, integer>);
    // An integer can at most have 20 digits (UINT64_MAX).
    char buf[24];
    auto size = std::snprintf(buf, 24, "%lld", static_cast<long long>(value));
    return std::copy(buf, buf + size, out);
  }
}

/// Writes `value` to `out` using `snprintf`.
template <class OutIter>
OutIter encode(real value, OutIter out) {
  auto size = std::snprintf(nullptr, 0, "%f", value);
  if (size < 24) {
    char buf[24];
    auto size = std::snprintf(buf, 24, "%f", value);
    return std::copy(buf, buf + size, out);
  } else {
    std::vector<char> buf;
    buf.resize(size + 1); // +1 for the null terminator
    size = std::snprintf(buf.data(), size + 1, "%f", value);
    return std::copy(buf.data(), buf.data() + size, out);
  }
}

/// Copies `value` to `out`.
template <class OutIter>
OutIter encode(std::string_view value, OutIter out) {
  return std::copy(value.begin(), value.end(), out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
template <class OutIter>
OutIter encode(const address& value, OutIter out) {
  std::string str;
  convert(value, str);
  return std::copy(str.begin(), str.end(), out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
template <class OutIter>
OutIter encode(const subnet& value, OutIter out) {
  std::string str;
  convert(value, str);
  return std::copy(str.begin(), str.end(), out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
template <class OutIter>
OutIter encode(port value, OutIter out) {
  std::string str;
  convert(value, str);
  return std::copy(str.begin(), str.end(), out);
}

/// Renders `value` to `out` in nanoseconds resolution.
template <class OutIter>
OutIter encode(timestamp value, OutIter out) {
  using namespace std::literals;
  auto suffix = "ns"sv;
  out = encode(static_cast<integer>(value.time_since_epoch().count()), out);
  return std::copy(suffix.begin(), suffix.end(), out);
}

/// Renders `value` to `out` in nanoseconds resolution.
template <class OutIter>
OutIter encode(timespan value, OutIter out) {
  using namespace std::literals;
  auto suffix = "ns"sv;
  out = encode(static_cast<integer>(value.count()), out);
  return std::copy(suffix.begin(), suffix.end(), out);
}

/// Copies the name of `value` to `out`.
template <class OutIter>
OutIter encode(const enum_value& value, OutIter out) {
  return encode(value.name, out);
}

/// Copies the name of `value` to `out`.
template <class OutIter>
OutIter encode(const enum_value_view& value, OutIter out) {
  return encode(value.name, out);
}

/// Renders `value` to `out` as a sequence, enclosing it in curly braces.
template <class OutIter>
OutIter encode(const broker::set& values, OutIter out);

/// Renders `value` to `out` as a sequence, enclosing it in curly braces and
/// displaying key/value pairs as `key -> value`.
template <class OutIter>
OutIter encode(const broker::table& values, OutIter out);

template <class OutIter>
OutIter encode(const broker::vector& values, OutIter out);

template <class Data, class OutIter>
std::enable_if_t<std::is_same_v<data, Data>, OutIter> encode(const Data& value,
                                                             OutIter out);

/// Helper function to render a sequence of values to `out`.
template <class Iterator, class Sentinel, class OutIter>
OutIter encode_range(Iterator first, Sentinel last, char left, char right,
                     OutIter out);

template <class OutIter>
OutIter encode(const variant_data& value, OutIter out);

template <class OutIter>
OutIter encode(const variant& value, OutIter out) {
  return encode(*value.raw(), out);
}

template <class OutIter>
OutIter encode(const variant_data::set* values, OutIter out) {
  return encode_range(values->begin(), values->end(), '{', '}', out);
}

template <class OutIter>
OutIter encode(const variant_data::table* values, OutIter out) {
  return encode_range(values->begin(), values->end(), '{', '}', out);
}

template <class OutIter>
OutIter encode(const variant_data::list* values, OutIter out) {
  return encode_range(values->begin(), values->end(), '(', ')', out);
}

template <class OutIter>
OutIter encode(const variant_data& value, OutIter out) {
  return std::visit([&](auto&& x) { return encode(x, out); }, value.value);
}

// Unfortunately, broker::data is a nasty type due to its implicit conversions.
// This template hackery is necessary to make sure this function accepts only
// broker::data directly, without allowing to compiler to implicitly convert
// other types to broker::data.
template <class Data, class OutIter>
std::enable_if_t<std::is_same_v<data, Data>, OutIter> encode(const Data& value,
                                                             OutIter out) {
  return std::visit([&](auto&& x) { return encode(x, out); }, value.get_data());
}

template <class OutIter>
OutIter encode(const broker::set& values, OutIter out) {
  return encode_range(values.begin(), values.end(), '{', '}', out);
}

template <class OutIter>
OutIter encode(const broker::table& values, OutIter out) {
  return encode_range(values.begin(), values.end(), '{', '}', out);
}

template <class OutIter>
OutIter encode(const broker::vector& values, OutIter out) {
  return encode_range(values.begin(), values.end(), '(', ')', out);
}

/// Renders `kvp` as `key -> value` to `out`.
template <class Key, class Val, class OutIter>
OutIter encode(const std::pair<Key, Val>& kvp, OutIter out) {
  using namespace std::literals;
  out = encode(kvp.first, out);
  auto sep = " -> "sv;
  out = std::copy(sep.begin(), sep.end(), out);
  return encode(kvp.second, out);
}

template <class Iterator, class Sentinel, class OutIter>
OutIter encode_range(Iterator first, Sentinel last, char left, char right,
                     OutIter out) {
  using namespace std::literals;
  *out++ = left;
  if (first != last) {
    out = encode(*first++, out);
    auto sep = ", "sv;
    while (first != last) {
      out = std::copy(sep.begin(), sep.end(), out);
      out = encode(*first++, out);
    }
  }
  *out++ = right;
  return out;
}

} // namespace broker::format::txt::v1
