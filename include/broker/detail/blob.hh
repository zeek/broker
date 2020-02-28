#pragma once

#include <string>
#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace broker {
namespace detail {

template <class T, class... Ts>
auto to_blob(T&& x, Ts&&... xs) {
  typename caf::binary_serializer::container_type buf;
  caf::binary_serializer sink{nullptr, buf};
  sink(std::forward<T>(x), std::forward<Ts>(xs)...);
  return buf;
}

template <class T>
T from_blob(const void* buf, size_t size) {
  caf::binary_deserializer source{nullptr, reinterpret_cast<const char*>(buf),
                                  size};
  T result;
  source(result);
  return result;
}

template <class T, class Container>
T from_blob(const Container& buf) {
  return from_blob<T>(buf.data(), buf.size());
}

} // namespace detail
} // namespace broker
