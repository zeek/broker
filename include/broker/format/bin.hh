#pragma once

#include "broker/config.hh"
#include "broker/data.hh"
#include "broker/variant.hh"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace broker::format::bin::v1 {

/// Converts `value` from native to network byte order.
uint16_t to_network_order(uint16_t value);

/// Converts `value` from native to network byte order.
uint64_t to_network_order(uint64_t value);

/// Converts `value` from network to native byte order.
template <class T>
T from_network_order(T value) {
  // swapping the bytes again gives the native order
  return to_network_order(value);
}

/// Packs a double into a 64-bit integer, preserving the bit pattern.
uint64_t real_to_network_representation(real value);

/// Unpacks a double from a 64-bit integer, preserving the bit pattern.
real real_from_network_representation(uint64_t value);

/// The maximum size of a varbyte-encoded value.
constexpr size_t max_varbyte_size = 10;

/// A pointer that a sequence of bytes.
using const_byte_pointer = const std::byte*;

/// Reads a size_t from a byte sequence using varbyte encoding.
bool read_varbyte(const_byte_pointer& first, const_byte_pointer last,
                  size_t& result);

template <class OutIter>
struct value_type_oracle {
  using type = typename std::iterator_traits<OutIter>::value_type;
};

template <class Container>
struct value_type_oracle<std::back_insert_iterator<Container>> {
  using type = typename Container::value_type;
};

/// Deduces the value type of an output iterator.
template <class OutIter>
using iter_value_t = typename value_type_oracle<OutIter>::type;

template <class WriteFn>
auto write_varbyte_impl(size_t value, WriteFn&& write) {
  // Use varbyte encoding to compress sequence size on the wire.
  // For 64-bit values, the encoded representation cannot get larger than 10
  // bytes. A scratch space of 16 bytes suffices as upper bound.
  uint8_t buf[16];
  auto i = buf;
  auto x = static_cast<uint32_t>(value);
  while (x > 0x7f) {
    *i++ = (static_cast<uint8_t>(x) & 0x7f) | 0x80;
    x >>= 7;
  }
  *i++ = static_cast<uint8_t>(x) & 0x7f;
  return write(reinterpret_cast<std::byte*>(buf),
               reinterpret_cast<std::byte*>(i));
}

/// Returns the number of bytes required to encode `value` as a varbyte.
inline size_t varbyte_size(size_t value) {
  return write_varbyte_impl(value, [](auto* begin, auto* end) {
    return static_cast<size_t>(end - begin);
  });
}

/// Encodes `value` to a variable-length byte sequence and appends it to `out`.
template <class OutIter>
OutIter write_varbyte(size_t value, OutIter out) {
  return write_varbyte_impl(value, [out](auto* begin, auto* end) {
    using value_type = iter_value_t<OutIter>;
    return std::copy(reinterpret_cast<value_type*>(begin),
                     reinterpret_cast<value_type*>(end), out);
  });
}

/// Encodes `value` to its binary representation and appends it to `out`.
template <class T, class OutIter>
OutIter write_unsigned(T value, OutIter out) {
  if constexpr (std::is_enum_v<T>) {
    return write_unsigned(static_cast<std::underlying_type_t<T>>(value), out);
  } else if constexpr (sizeof(T) > 1) {
    static_assert(std::is_unsigned_v<T> && !std::is_same_v<T, bool>);
    auto tmp = to_network_order(value);
    iter_value_t<OutIter> buf[sizeof(T)];
    memcpy(buf, &tmp, sizeof(T));
    return std::copy(buf, buf + sizeof(T), out);
  } else {
    *out++ = static_cast<iter_value_t<OutIter>>(value);
    return out;
  }
}

template <class T, class OutIter>
OutIter write_bytes(const T* first, const T* last, OutIter out) {
  static_assert(sizeof(T) == 1);
  using val_t = iter_value_t<OutIter>;
  return std::copy(reinterpret_cast<const val_t*>(first),
                   reinterpret_cast<const val_t*>(last), out);
}

template <class Container, class OutIter>
OutIter write_bytes(const Container& in, OutIter out) {
  return write_bytes(in.data(), in.data() + in.size(), out);
}

/// Initializes a buffer for encoding a sequence of values.
template <class Buffer>
void encode_sequence_begin(Buffer& out) {
  // When starting to encode a new sequence, we don't have the size yet. Hence,
  // we reserve the maximum size of a varbyte-encoded value.
  out.clear();
  out.reserve(32);
  out.resize(max_varbyte_size + 1); // +1 for the tag
}

/// Finalizes a buffer for encoding a sequence of values.
/// @param tag The tag of the sequence.
/// @param len The number of elements in the sequence.
/// @param out The buffer to write to.
/// @returns the offset for the buffer where the sequence starts.
template <class Buffer>
size_t encode_sequence_end(data::type tag, size_t len, Buffer& out) {
  // Now that we know the size, we can encode it at the beginning of the buffer.
  // We also encode the tag of the sequence.
  auto len_size = varbyte_size(len);
  auto offset = max_varbyte_size - len_size;
  out[offset] = static_cast<std::byte>(tag);
  write_varbyte(len, out.data() + offset + 1);
  return offset;
}

/// Returns the byte range holding the encoded values of a sequence that was
/// initialized with `encode_sequence_begin`.
template <class Buffer>
auto encoded_values(const Buffer& buf) {
  constexpr size_t offset = max_varbyte_size + 1;
  auto begin = buf.data() + offset;
  auto end = begin + (buf.size() - offset);
  return std::make_pair(begin, end);
}

template <class OutIter>
OutIter encode(none, OutIter out) {
  return write_unsigned(data::type::none, out);
}

template <class OutIter>
OutIter encode(boolean value, OutIter out) {
  out = write_unsigned(data::type::boolean, out);
  return write_unsigned(static_cast<uint8_t>(value), out);
}

/// Encodes `value` to its binary representation and appends it to `out`.
template <class OutIter>
OutIter encode(count value, OutIter out) {
  static_assert(sizeof(count) == sizeof(uint64_t));
  out = write_unsigned(data::type::count, out);
  return write_unsigned(value, out);
}

template <class OutIter>
OutIter encode(integer value, OutIter out) {
  static_assert(sizeof(integer) == sizeof(uint64_t));
  out = write_unsigned(data::type::integer, out);
  return write_unsigned(static_cast<count>(value), out);
}

template <class OutIter>
OutIter encode(real value, OutIter out) {
  out = write_unsigned(data::type::real, out);
  return write_unsigned(real_to_network_representation(value), out);
}

template <class OutIter>
OutIter encode(std::string_view value, OutIter out) {
  out = write_unsigned(data::type::string, out);
  out = write_varbyte(value.size(), out);
  return write_bytes(value, out);
}

template <class OutIter>
OutIter encode(const address& value, OutIter out) {
  out = write_unsigned(data::type::address, out);
  return write_bytes(value.bytes(), out);
}

template <class OutIter>
OutIter encode(const subnet& value, OutIter out) {
  out = write_unsigned(data::type::subnet, out);
  out = write_bytes(value.network().bytes(), out);
  return write_unsigned(value.length(), out);
}

template <class OutIter>
OutIter encode(port value, OutIter out) {
  out = write_unsigned(data::type::port, out);
  out = write_unsigned(value.number(), out);
  return write_unsigned(value.type(), out);
}

template <class OutIter>
OutIter encode(timestamp value, OutIter out) {
  out = write_unsigned(data::type::timestamp, out);
  return write_unsigned(static_cast<count>(value.time_since_epoch().count()),
                        out);
}

template <class OutIter>
OutIter encode(timespan value, OutIter out) {
  out = write_unsigned(data::type::timespan, out);
  return write_unsigned(static_cast<count>(value.count()), out);
}

template <class OutIter>
OutIter encode(enum_value_view value, OutIter out) {
  out = write_unsigned(data::type::enum_value, out);
  out = write_varbyte(value.name.size(), out);
  return write_bytes(value.name, out);
}

template <class OutIter>
OutIter encode(const enum_value& value, OutIter out) {
  out = write_unsigned(data::type::enum_value, out);
  out = write_varbyte(value.name.size(), out);
  return write_bytes(value.name, out);
}

template <class OutIter>
OutIter encode(const variant_data& value, OutIter out);

template <class OutIter>
OutIter encode(const variant_data::set* values, OutIter out);

template <class OutIter>
OutIter encode(const variant_data::table* values, OutIter out);

template <class OutIter>
OutIter encode(const variant_data::list* values, OutIter out);

template <class OutIter>
OutIter encode(const variant& value, OutIter out) {
  return std::visit([&](auto&& x) { return encode(x, out); },
                    value.stl_value());
}

template <class OutIter>
OutIter encode(const variant_data& value, OutIter out) {
  return std::visit([&](auto&& x) { return encode(x, out); },
                    value.stl_value());
}

template <class OutIter>
OutIter encode(const variant_data::set* values, OutIter out) {
  out = write_unsigned(data::type::set, out);
  out = write_varbyte(values->size(), out);
  for (const auto& x : *values)
    out = encode(x, out);
  return out;
}

template <class OutIter>
OutIter encode(const variant_data::table* values, OutIter out) {
  out = write_unsigned(data::type::set, out);
  out = write_varbyte(values->size(), out);
  for (const auto& [key, val] : *values) {
    out = encode(key, out);
    out = encode(val, out);
  }
  return out;
}

template <class OutIter>
OutIter encode(const variant_data::list* values, OutIter out) {
  out = write_unsigned(data::type::list, out);
  out = write_varbyte(values->size(), out);
  for (const auto& x : *values)
    out = encode(x, out);
  return out;
}

/// Embeds an already encoded sequence into `out`.
/// @param tag The tag of the sequence.
/// @param num_elements The number of elements in the sequence.
/// @param first The first byte of the encoded sequence.
/// @param last The last byte of the encoded sequence.
/// @param out The buffer to write to.
/// @pre `first` and `last` must be a valid range.
template <class InputIter, class Sentinel, class OutIter>
OutIter write_sequence(data::type tag, size_t num_elements, InputIter first,
                       Sentinel last, OutIter out) {
  out = write_unsigned(tag, out);
  out = write_varbyte(num_elements, out);
  return write_bytes(first, last, out);
}
} // namespace broker::format::bin::v1
