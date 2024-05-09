#pragma once

#include "broker/config.hh"
#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace broker::format::json::v1 {

/// Configures `encode` to render the value as a JSON object, i.e., surround the
/// fields with curly braces.
struct render_object {};

/// Configures `encode` to render the value as an embedded JSON object, i.e.,
/// omit the curly braces.
struct render_embedded {};

/// Appends a string to the output iterator.
template <class OutIter>
OutIter append(std::string_view str, OutIter out) {
  return std::copy(str.begin(), str.end(), out);
}

/// Tag type for selecting the `quoted` overload of `append_encoded`.
struct quoted {
  std::string_view str;
};

/// Appends a quoted string to the output iterator. Special characters are
/// escaped.
template <class OutIter>
OutIter append(quoted value, OutIter out) {
  using namespace std::literals;
  *out++ = '"';
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
  return out;
}

/// Appends a field (key-value pair) to the output iterator.
template <class OutIter>
OutIter append_field(std::string_view key, std::string_view val, OutIter out) {
  out = append(quoted{key}, out);
  *out++ = ':';
  return append(quoted{val}, out);
}

/// Appends an encoded value to the output iterator.
/// @param value_type The type name of the value.
/// @param value The value. Must provide an overload for `append()`.
/// @param out The output iterator.
template <class Policy = render_object, class Appendable, class OutIter>
OutIter append_encoded(std::string_view value_type, Appendable value,
                       OutIter out) {
  using namespace std::literals;
  if constexpr (std::is_same_v<Policy, render_object>)
    *out++ = '{';
  out = append(R"_("@data-type":")_", out);
  out = append(value_type, out);
  out = append(R"_(","data":)_", out);
  out = append(value, out);
  if constexpr (std::is_same_v<Policy, render_object>)
    *out++ = '}';
  return out;
}

/// Renders the `nil` value to `out` as `{}`.
template <class Policy = render_object, class OutIter>
OutIter encode(none, OutIter out) {
  return append_encoded<Policy>("none", "{}", out);
}

/// Renders a boolean, `count` or `integer` value to `out`. The latter two are
/// rendered using `snprintf`, whereas boolean values are rendered as `true` or
/// `false`.
template <class Policy = render_object, class T, class OutIter>
std::enable_if_t<std::is_integral_v<T>, OutIter> encode(T value, OutIter out) {
  using namespace std::literals;
  if constexpr (std::is_same_v<T, bool>) {
    return append_encoded<Policy>("boolean", value ? "true"sv : "false"sv, out);
  } else if constexpr (std::is_same_v<T, count>) {
    // An integer can at most have 20 digits (UINT64_MAX).
    char buf[24];
    auto size = std::snprintf(buf, 24, "%llu",
                              static_cast<long long unsigned>(value));
    auto str = std::string_view{buf, static_cast<size_t>(size)};
    return append_encoded<Policy>("count", str, out);
  } else {
    static_assert(std::is_same_v<T, integer>);
    // An integer can at most have 20 digits (UINT64_MAX).
    char buf[24];
    auto size = std::snprintf(buf, 24, "%lld", static_cast<long long>(value));
    auto str = std::string_view{buf, static_cast<size_t>(size)};
    return append_encoded<Policy>("integer", str, out);
  }
}

/// Writes `value` to `out` using `snprintf` with the default precision (6
/// digits), e.g., `0.123456`.
template <class Policy = render_object, class OutIter>
OutIter encode(real value, OutIter out) {
  auto size = std::snprintf(nullptr, 0, "%f", value);
  if (size < 24) {
    char buf[24];
    size = std::snprintf(buf, 24, "%f", value);
    auto str = std::string_view{buf, static_cast<size_t>(size)};
    return append_encoded<Policy>("real", str, out);
  } else {
    std::vector<char> buf;
    buf.resize(static_cast<size_t>(size) + 1); // +1 for the null terminator
    size = std::snprintf(buf.data(), size + 1, "%f", value);
    auto str = std::string_view{buf.data(), static_cast<size_t>(size)};
    return append_encoded<Policy>("real", str, out);
  }
}

/// Renders `value` to `out` as a quoted string.
template <class Policy = render_object, class OutIter>
OutIter encode(std::string_view value, OutIter out) {
  return append_encoded<Policy>("string", quoted{value}, out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
/// For an IPV4 address, the result is a string in dotted decimal notation,
/// e.g., `192.168.99.9`. For an IPV6 address, the result is a string in
/// hexadecimal notation, e.g., `2001:0db8:85a3::8a2e:0370:7334`.
template <class Policy = render_object, class OutIter>
OutIter encode(const address& value, OutIter out) {
  std::string str;
  convert(value, str);
  return append_encoded<Policy>("address", quoted{str}, out);
}

/// Renders `value` using the `convert` API and copies the result to `out`. The
/// output string uses the format `prefix/length`, e.g., `192.168.99.0/24`.
template <class Policy = render_object, class OutIter>
OutIter encode(const subnet& value, OutIter out) {
  std::string str;
  convert(value, str);
  return append_encoded<Policy>("subnet", quoted{str}, out);
}

/// Renders `value` using the `convert` API and copies the result to `out`.
template <class Policy = render_object, class OutIter>
OutIter encode(port value, OutIter out) {
  std::string str;
  convert(value, str);
  return append_encoded<Policy>("port", quoted{str}, out);
}

// Helper function for encoding timestamps in ISO format, produces a quoted
// string.
size_t encode_to_buf(timestamp value, std::array<char, 32>& buf);

/// Renders `value` to `out` in millisecond resolution.
template <class Policy = render_object, class OutIter>
OutIter encode(timestamp value, OutIter out) {
  std::array<char, 32> buf;
  auto size = encode_to_buf(value, buf);
  return append_encoded<Policy>("timestamp", std::string_view{buf.data(), size},
                                out);
}

/// Renders `value` to `out` as an integer followed by a suffix, e.g., `42s`.
/// The function picks the highest possible resolution for the given value
/// without losing precision. The suffix is one of `ns`, `us`, `ms`, or `s`.
template <class Policy = render_object, class OutIter>
OutIter encode(timespan value, OutIter out) {
  using namespace std::literals;
  // Helper function to print the value with a suffix via snprintf.
  auto do_encode = [&out](long long val, const char* suffix) {
    // An integer can at most have 20 digits (UINT64_MAX) and our suffix may
    // only be 2 characters long.
    char buf[32];
    auto size = std::snprintf(buf, 32, R"_("%lld%s")_", val, suffix);
    auto str = std::string_view{buf, static_cast<size_t>(size)};
    return append_encoded<Policy>("timespan", str, out);
  };
  // Short-circuit for zero. Always prints "0s".
  auto val = value.count();
  if (val == 0) {
    return append_encoded<Policy>("timespan", quoted{"0s"}, out);
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
template <class Policy = render_object, class OutIter>
OutIter encode(const enum_value& value, OutIter out) {
  return append_encoded<Policy>("enum-value", quoted{value.name}, out);
}

/// Copies the name of `value` to `out`.
template <class Policy = render_object, class OutIter>
OutIter encode(const enum_value_view& value, OutIter out) {
  return append_encoded<Policy>("enum-value", quoted{value.name}, out);
}

/// Encodes a JSON list.
template <class Policy, class ForwardIterator, class Sentinel, class OutIter>
OutIter encode_list(std::string_view type, ForwardIterator first, Sentinel last,
                    OutIter out);

/// Renders `value` to `out` as a JSON list.
template <class Policy = render_object, class OutIter>
OutIter encode(const broker::set& values, OutIter out) {
  return encode_list<Policy>("set", values.begin(), values.end(), out);
}

/// Renders `value` to `out` as a JSON list.
template <class Policy = render_object, class OutIter>
OutIter encode(const variant_data::set* values, OutIter out) {
  return encode_list<Policy>("set", values->begin(), values->end(), out);
}

/// Renders `value` to `out` as a JSON list.
template <class Policy = render_object, class OutIter>
OutIter encode(const broker::vector& values, OutIter out) {
  return encode_list<Policy>("vector", values.begin(), values.end(), out);
}

/// Renders `value` to `out` as a JSON list.
template <class Policy = render_object, class OutIter>
OutIter encode(const variant_data::list* values, OutIter out) {
  return encode_list<Policy>("vector", values->begin(), values->end(), out);
}

/// Renders `value` to `out` as a sequence, enclosing it in curly braces and
/// displaying key/value pairs as `key -> value`.
template <class Policy = render_object, class OutIter>
OutIter encode(const broker::table& values, OutIter out) {
  return encode_list<Policy>("table", values.begin(), values.end(), out);
}

/// Renders `value` to `out` as a sequence, enclosing it in curly braces and
/// displaying key/value pairs as `key -> value`.
template <class Policy = render_object, class OutIter>
OutIter encode(const variant_data::table* values, OutIter out) {
  return encode_list<Policy>("table", values->begin(), values->end(), out);
}

/// Renders a `data` object to `out` by dispatching to the appropriate overload
/// for the underlying type.
template <class Policy = render_object, class Data, class OutIter>
std::enable_if_t<std::is_same_v<data, Data>, OutIter> encode(const Data& value,
                                                             OutIter out) {
  return std::visit([&](auto&& x) { return encode<Policy>(x, out); },
                    value.get_data());
}

/// Renders a `data` object to `out` by dispatching to the appropriate overload
/// for the underlying type.
template <class Policy = render_object, class OutIter>
OutIter encode(const variant_data& value, OutIter out) {
  return std::visit([&](auto&& x) { return encode<Policy>(x, out); },
                    value.value);
}

/// Renders a `data` object to `out` by dispatching to the appropriate overload
/// for the underlying type.
template <class Policy = render_object, class OutIter>
OutIter encode(const variant& value, OutIter out) {
  return encode<Policy>(*value.raw(), out);
}

/// Renders the key-value pair to `out` as a JSON object.
template <class OutIter>
OutIter encode(const broker::table::value_type& values, OutIter out) {
  // Note: this overload needs no policy because it's only used by the
  //      `encode(const broker::table&, OutIter)` overload and must always
  //      render sub-objects with enclosing curly braces.
  out = append(R"_({"key":)_", out);
  out = encode(values.first, out);
  out = append(R"_(,"value":)_", out);
  out = encode(values.second, out);
  *out++ = '}';
  return out;
}

/// Renders the key-value pair to `out` as a JSON object.
template <class OutIter>
OutIter encode(const variant_data::table::value_type& values, OutIter out) {
  // Note: this overload needs no policy because it's only used by the
  //      `encode(const broker::table&, OutIter)` overload and must always
  //      render sub-objects with enclosing curly braces.
  out = append(R"_({"key":)_", out);
  out = encode(values.first, out);
  out = append(R"_(,"value":)_", out);
  out = encode(values.second, out);
  *out++ = '}';
  return out;
}

template <class Policy, class ForwardIterator, class Sentinel, class OutIter>
OutIter encode_list(std::string_view type, ForwardIterator first, Sentinel last,
                    OutIter out) {
  if (first == last)
    return append_encoded<Policy>(type, "[]", out);
  using namespace std::literals;
  if constexpr (std::is_same_v<Policy, render_object>)
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
  if constexpr (std::is_same_v<Policy, render_object>)
    *out++ = '}';
  return out;
}

/// Renders a `data` object to `out`.
template <class OutIter>
OutIter encode(const data_message& msg, OutIter out) {
  *out++ = '{';
  out = append(R"_("type":"data-message","topic":)_", out);
  out = append(quoted{get_topic(msg)}, out);
  *out++ = ',';
  out = encode<render_embedded>(get_data(msg), out);
  *out++ = '}';
  return out;
}

/// Tries to decode a JSON object from `str`. On success, the result is stored
/// in `result` and the functions a default-constructed `error`. Otherwise, the
/// function returns a non-empty error and leaves `result` in an unspecified
/// state.
error decode(std::string_view str, variant& result);

} // namespace broker::format::json::v1
