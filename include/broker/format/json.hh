#pragma once

#include "broker/config.hh"
#include "broker/data.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace broker::format::json::v1 {

/// Appends a string to the output iterator.
template <class OutIter>
OutIter append(std::string_view str, OutIter out) {
  return std::copy(str.begin(), str.end(), out);
}

/// Appends an encoded value to the output iterator.
template <class OutIter>
OutIter append_encoded(std::string_view value_type, std::string_view value, OutIter out) {
  using namespace std::literals;
  *out++ = '{';
  out = append(R"_("@data-type":")_", out);
  out = append(value_type, out);
  out = append(R"_(","data":)_", out);
  out = append(value, out);
  *out++ = '}';
  return out;
}

/// Tag type for selecting the `quoted` overload of `append_encoded`.
struct quoted {
  std::string_view str;
};

/// Appends an encoded value as a quoted string to the output iterator.
template <class OutIter>
OutIter append_encoded(std::string_view value_type, quoted value, OutIter out) {
  using namespace std::literals;
  *out++ = '{';
  out = append(R"_("@data-type":")_", out);
  out = append(value_type, out);
  out = append(R"_(","data":")_", out);
  for (auto c : value.str) {
    switch (c) {
      default:
        *out++ = c;
        break;
      case '\\':
        *out++ = '\\';
        *out++ = '\\';
        break;
      case '\b':
        *out++ = '\\';
        *out++ = 'b';
        break;
      case '\f':
        *out++ = '\\';
        *out++ = 'f';
        break;
      case '\n':
        *out++ = '\\';
        *out++ = 'n';
        break;
      case '\r':
        *out++ = '\\';
        *out++ = 'r';
        break;
      case '\t':
        *out++ = '\\';
        *out++ = 't';
        break;
      case '\v':
        *out++ = '\\';
        *out++ = 'v';
        break;
      case '"':
        *out++ = '\\';
        *out++ = '"';
        break;
    }
  }
  *out++ = '"';
  *out++ = '}';
  return out;
}

/// Render the `nil` value to `out`.
template <class OutIter>
OutIter encode(none, OutIter out) {
  return append_encoded("none", "{}", out);
}

/// Renders `value` to `out` as `T` for `true` and `F` for `false`.
template <class T, class OutIter>
std::enable_if_t<std::is_integral_v<T>, OutIter> encode(T value, OutIter out) {
  using namespace std::literals;
  if constexpr (std::is_same_v<T, bool>) {
    return append_encoded("boolean", value ? "true"sv : "false"sv, out);
  } else if constexpr (std::is_same_v<T, count>) {
    // An integer can at most have 20 digits (UINT64_MAX).
    char buf[24];
    auto size = std::snprintf(buf, 24, "%llu",
                              static_cast<long long unsigned>(value));
    return append_encoded("count", {buf, static_cast<size_t>(size)}, out);
  } else {
    static_assert(std::is_same_v<T, integer>);
    // An integer can at most have 20 digits (UINT64_MAX).
    char buf[24];
    auto size = std::snprintf(buf, 24, "%lld", static_cast<long long>(value));
    return append_encoded("integer", {buf, static_cast<size_t>(size)}, out);
  }
}

/// Writes `value` to `out` using `snprintf`.
template <class OutIter>
OutIter encode(real value, OutIter out) {
  auto size = std::snprintf(nullptr, 0, "%f", value);
  if (size < 24) {
    char buf[24];
    size = std::snprintf(buf, 24, "%f", value);
    return append_encoded("real", {buf, static_cast<size_t>(size)}, out);
  } else {
    std::vector<char> buf;
    buf.resize(static_cast<size_t>(size) + 1); // +1 for the null terminator
    size = std::snprintf(buf.data(), size + 1, "%f", value);
    return append_encoded("real", {buf.data(), static_cast<size_t>(size)}, out);
  }
}

/// Renders `value` to `out` as a quoted string.
template <class OutIter>
OutIter encode(std::string_view value, OutIter out) {
  return append_encoded("string", quoted{value}, out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
template <class OutIter>
OutIter encode(const address& value, OutIter out) {
  std::string str;
  convert(value, str);
  return append_encoded("address", quoted{str}, out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
template <class OutIter>
OutIter encode(const subnet& value, OutIter out) {
  std::string str;
  convert(value, str);
  return append_encoded("subnet", quoted{str}, out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
template <class OutIter>
OutIter encode(port value, OutIter out) {
  std::string str;
  convert(value, str);
  return append_encoded("port", quoted{str}, out);
}

// Helper function for encoding timestamps in ISO format, produces a quoted
// string.
size_t encode_to_buf(timestamp value, std::array<char, 32>& buf);

/// Renders `value` to `out` in millisecond resolution.
template <class OutIter>
OutIter encode(timestamp value, OutIter out) {
  std::array<char, 32> buf;
  auto size = encode_to_buf(value, buf);
  return append_encoded("timestamp", std::string_view{buf.data(), size}, out);
}

/// Renders `value` to `out` in millisecond resolution.
template <class OutIter>
OutIter encode(timespan value, OutIter out) {
  using namespace std::literals;
  // Helper function to print the value with a suffix via snprintf.
  auto do_encode = [&out](long long val, const char* suffix) {
    // An integer can at most have 20 digits (UINT64_MAX) and our suffix may
    // only be 2 characters long.
    char buf[32];
    auto size = std::snprintf(buf, 32, R"_("%lld%s")_", val, suffix);
    return append_encoded("timespan", {buf, static_cast<size_t>(size)}, out);
  };
  // Short-circuit for zero. Always prints "0s".
  auto val = value.count();
  if (val == 0) {
    return append_encoded("timespan", quoted{"0s"}, out);
  }
  // Render as nanoseconds if the fractional part is non-zero.
  if (val % 1000 != 0) {
    return do_encode(val, "ns");
  }
  // Render as microseconds if the fractional part is non-zero.
  val /= 1000;
  if (val % 1000 != 0) {
    return do_encode(val, "us");
  }
  // Render as milliseconds if the fractional part is non-zero.
  val /= 1000;
  if (val % 1000 != 0) {
    return do_encode(val, "ms");
  }
  // Render as seconds otherwise.
  val /= 1000;
  return do_encode(val, "s");
}

/// Copies the name of `value` to `out`.
template <class OutIter>
OutIter encode(const enum_value& value, OutIter out) {
  return append_encoded("enum-value", quoted{value.name}, out);
}

/// Encodes a JSON list.
template <class ForwardIterator, class Sentinel, class OutIter>
OutIter encode_list(std::string_view type, ForwardIterator first, Sentinel last,
                    OutIter out);

/// Renders `value` to `out` as a JSON list.
template <class OutIter>
OutIter encode(const broker::set& values, OutIter out) {
  return encode_list("set", values.begin(), values.end(), out);
}

/// Renders `value` to `out` as a JSON list.
template <class OutIter>
OutIter encode(const broker::vector& values, OutIter out) {
  return encode_list("vector", values.begin(), values.end(), out);
}

/// Renders the key-value pair to `out` as a JSON object.
template <class OutIter>
OutIter encode(const broker::table::value_type& values, OutIter out);

/// Renders `value` to `out` as a sequence, enclosing it in curly braces and
/// displaying key/value pairs as `key -> value`.
template <class OutIter>
OutIter encode(const broker::table& values, OutIter out) {
  return encode_list("table", values.begin(), values.end(), out);
}

/// Renders a `data` object to `out`.
template <class Data, class OutIter>
std::enable_if_t<std::is_same_v<data, Data>, OutIter> encode(const Data& value,
                                                             OutIter out) {
  return std::visit([&](auto&& x) { return encode(x, out); }, value.get_data());
}

template <class OutIter>
OutIter encode(const broker::table::value_type& values, OutIter out){
  out = append(R"_({"key":)_", out);
  out = encode(values.first, out);
  out = append(R"_(,"value":)_", out);
  out = encode(values.second, out);
  *out++ = '}';
  return out;
}

template <class ForwardIterator, class Sentinel, class OutIter>
OutIter encode_list(std::string_view type, ForwardIterator first, Sentinel last,
                    OutIter out) {
  if (first == last)
    return append_encoded(type, "[]", out);
  using namespace std::literals;
  *out++ = '{';
  out = append(R"_("@data-type":")_", out);
  out = append(type, out);
  out = append(R"_(","data":[)_", out);
  out = encode(*first, out);
  while (++first != last) {
    *out++ = ',';
    out = encode(*first, out);
  }
  *out++ = ']';
  *out++ = '}';
  return out;
}


} // namespace broker::format::json::v1
