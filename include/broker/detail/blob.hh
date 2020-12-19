#pragma once

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/span.hpp>

#include "broker/detail/assert.hh"

namespace broker::detail {

using blob_buffer = caf::binary_serializer::container_type;

using blob_span = caf::span<blob_buffer::value_type>;

using const_blob_span = caf::span<const blob_buffer::value_type>;

template <class T>
void to_blob(const T& x, blob_buffer& buf) {
  caf::binary_serializer sink{nullptr, buf};
  auto res = sink.apply(x);
  BROKER_ASSERT(res);
  static_cast<void>(res);
}

template <class T>
blob_buffer to_blob(const T& x) {
  blob_buffer buf;
  to_blob(x, buf);
  return buf;
}

template <class T>
T from_blob(const void* buf, size_t size) {
  caf::binary_deserializer source{nullptr, buf, size};
  auto result = T{};
  auto res = source.apply(result);
  BROKER_ASSERT(res);
  static_cast<void>(res);
  return result;
}

template <class T, class Container>
T from_blob(const Container& buf) {
  return from_blob<T>(buf.data(),
                      buf.size() * sizeof(typename Container::value_type));
}

} // namespace broker::detail
