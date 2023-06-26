#pragma once

#include "broker/config.hh"
#include "broker/data.hh"
#include "broker/data_view.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

/// BTF stands for "broker text format". This namespace contains all
/// functionality related to serializing Broker's data types to a human-readable
/// text format.
namespace broker::detail::btf {

template <class OutIter>
OutIter encode(none, OutIter out) {
  using namespace std::literals;
  auto str = "nil"s;
  return std::copy(str.begin(), str.end(), out);
}

template <class OutIter>
OutIter encode(boolean value, OutIter out) {
  *out++ = value ? 'T' : 'F';
  return out;
}

template <class OutIter>
OutIter encode(count value, OutIter out) {
  // An integer can at most have 20 digits (UINT64_MAX).
  char buf[24];
  auto size = std::snprintf(buf, 24, "%llu", value);
  return std::copy(buf, buf + size, out);
}

template <class OutIter>
OutIter encode(integer value, OutIter out) {
  // An integer can at most have 20 digits (UINT64_MAX).
  char buf[24];
  auto size = std::snprintf(buf, 24, "%lld", value);
  return std::copy(buf, buf + size, out);
}

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
    std::snprintf(buf.data(), buf.size(), "%f", value);
    return std::copy(buf.begin(), buf.end(), out);
  }
}

template <class OutIter>
OutIter encode(std::string_view value, OutIter out) {
  return std::copy(value.begin(), value.end(), out);
}

template <class OutIter>
OutIter encode(address value, OutIter out) {
  std::string str;
  convert(value, str);
  return std::copy(str.begin(), str.end(), out);
}

template <class OutIter>
OutIter encode(subnet value, OutIter out) {
  std::string str;
  convert(value, str);
  return std::copy(str.begin(), str.end(), out);
}

template <class OutIter>
OutIter encode(port value, OutIter out) {
  std::string str;
  convert(value, str);
  return std::copy(str.begin(), str.end(), out);
}

template <class OutIter>
OutIter encode(timestamp value, OutIter out) {
  using namespace std::literals;
  auto suffix = "ns"sv;
  out = encode(static_cast<integer>(value.time_since_epoch().count()), out);
  return std::copy(suffix.begin(), suffix.end(), out);
}

template <class OutIter>
OutIter encode(timespan value, OutIter out) {
  using namespace std::literals;
  auto suffix = "ns"sv;
  out = encode(static_cast<integer>(value.count()), out);
  return std::copy(suffix.begin(), suffix.end(), out);
}

template <class OutIter>
OutIter encode(enum_value_view value, OutIter out) {
  return encode(value.name, out);
}

template <class OutIter>
OutIter encode(const enum_value& value, OutIter out) {
  return encode(value.name, out);
}

template <class OutIter>
OutIter encode(const data_view_value& value, OutIter out);

template <class OutIter>
OutIter encode(const data_view_value::set_view* values, OutIter out);

template <class OutIter>
OutIter encode(const data_view_value::table_view* values, OutIter out);

template <class OutIter>
OutIter encode(const data_view_value::vector_view* values, OutIter out);

template <class OutIter>
OutIter encode(const broker::set& values, OutIter out);

template <class OutIter>
OutIter encode(const broker::table& values, OutIter out);

template <class OutIter>
OutIter encode(const broker::vector& values, OutIter out);

template <class OutIter>
OutIter encode(const data_view& value, OutIter out) {
  return std::visit([&](auto&& x) { return encode(x, out); },
                    value.as_variant());
}

template <class OutIter>
OutIter encode(const data_view_value& value, OutIter out) {
  return std::visit([&](auto&& x) { return encode(x, out); }, value.data);
}

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

template <class OutIter>
OutIter encode(const data_view_value::set_view* values, OutIter out) {
  return encode_range(values->begin(), values->end(), '{', '}', out);
}

template <class OutIter>
OutIter encode(const data_view_value::table_view* values, OutIter out) {
  return encode_range(values->begin(), values->end(), '(', ')', out);
}

template <class OutIter>
OutIter encode(const data_view_value::vector_view* values, OutIter out) {
  return encode_range(values->begin(), values->end(), '{', '}', out);
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
  return encode_range(values.begin(), values.end(), '(', ')', out);
}

template <class OutIter>
OutIter encode(const broker::vector& values, OutIter out) {
  return encode_range(values.begin(), values.end(), '{', '}', out);
}

} // namespace broker::detail::btf
