#pragma once

#include "broker/config.hh"
#include "broker/data.hh"
#include "broker/detail/type_traits.hh"
#include "broker/variant.hh"
#include "broker/variant_data.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace broker::format::bin::v1 {

uint16_t to_network_order_impl(uint16_t value);

uint32_t to_network_order_impl(uint32_t value);

uint64_t to_network_order_impl(uint64_t value);

/// Converts `value` from native to network byte order.
template <class T>
T to_network_order(T value) {
  static_assert(std::is_unsigned_v<T>);
  if constexpr (sizeof(T) == 2) {
    auto res = to_network_order_impl(static_cast<uint16_t>(value));
    return static_cast<T>(res);
  } else if constexpr (sizeof(T) == 4) {
    auto res = to_network_order_impl(static_cast<uint32_t>(value));
    return static_cast<T>(res);
  } else {
    static_assert(sizeof(T) == 8);
    auto res = to_network_order_impl(static_cast<uint64_t>(value));
    return static_cast<T>(res);
  }
}

/// Converts `value` from network to native byte order.
template <class T>
T from_network_order(T value) {
  // swapping the bytes again gives the native order
  return to_network_order(value);
}

/// Packs a float into a 32-bit integer, preserving the bit pattern.
uint32_t to_network_representation(float value);

/// Packs a double into a 64-bit integer, preserving the bit pattern.
uint64_t to_network_representation(double value);

/// Unpacks a 32-bit integer into a float, preserving the bit pattern.
float float32_from_network_representation(uint32_t value);

/// Unpacks a 64-bit integer into a double, preserving the bit pattern.
double float64_from_network_representation(uint64_t value);

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
  return out;
}

template <class OutIter>
OutIter encode(std::byte value, OutIter out) {
  return write_unsigned(value, out);
}

/// Encodes `value` to its binary representation and appends it to `out`.
template <class T, class OutIter>
std::enable_if_t<std::is_arithmetic_v<T>, OutIter> encode(T value,
                                                          OutIter out) {
  if constexpr (std::is_same_v<T, bool>) {
    return write_unsigned(static_cast<uint8_t>(value), out);
  } else if constexpr (std::is_floating_point_v<T>) {
    return write_unsigned(to_network_representation(value), out);
  } else if constexpr (std::is_unsigned_v<T>) {
    return write_unsigned(value, out);
  } else {
    static_assert(std::is_signed<T>::value);
    using unsigned_t = std::make_unsigned_t<T>;
    return write_unsigned(static_cast<unsigned_t>(value), out);
  }
}

template <class OutIter>
OutIter encode(std::string_view value, OutIter out) {
  out = write_varbyte(value.size(), out);
  return write_bytes(value, out);
}

template <class OutIter>
OutIter encode(const address& value, OutIter out) {
  return write_bytes(value.bytes(), out);
}

template <class OutIter>
OutIter encode(const subnet& value, OutIter out) {
  out = write_bytes(value.network().bytes(), out);
  return write_unsigned(value.raw_len(), out);
}

template <class OutIter>
OutIter encode(port value, OutIter out) {
  out = write_unsigned(value.number(), out);
  return write_unsigned(value.type(), out);
}

template <class Rep, class Period, class OutIter>
OutIter encode(std::chrono::duration<Rep, Period> value, OutIter out) {
  return encode(value.count(), out);
}

template <class Clock, class Duration, class OutIter>
OutIter encode(std::chrono::time_point<Clock, Duration> value, OutIter out) {
  return encode(value.time_since_epoch(), out);
}

template <class OutIter>
OutIter encode(const enum_value& value, OutIter out) {
  out = write_varbyte(value.name.size(), out);
  return write_bytes(value.name, out);
}

template <class OutIter>
OutIter encode(enum_value_view value, OutIter out) {
  out = write_varbyte(value.name.size(), out);
  return write_bytes(value.name, out);
}

template <class OutIter>
OutIter encode(const variant_data& value, OutIter out);

template <class OutIter>
OutIter encode(const variant& value, OutIter out) {
  return encode(*value.raw(), out);
}

template <class OutIter>
OutIter encode(const variant_data::set* values, OutIter out) {
  out = write_varbyte(values->size(), out);
  for (const auto& x : *values)
    out = encode(x, out);
  return out;
}

template <class OutIter>
OutIter encode(const variant_data::table* values, OutIter out) {
  out = write_varbyte(values->size(), out);
  for (const auto& [key, val] : *values) {
    out = encode(key, out);
    out = encode(val, out);
  }
  return out;
}

template <class OutIter>
OutIter encode(const variant_data::list* values, OutIter out) {
  out = write_varbyte(values->size(), out);
  for (const auto& x : *values)
    out = encode(x, out);
  return out;
}

template <class OutIter>
OutIter encode(const variant_list& values, OutIter out) {
  return encode(values.raw(), out);
}

template <class OutIter>
OutIter encode(const variant_set& values, OutIter out) {
  return encode(values.raw(), out);
}

template <class OutIter>
OutIter encode(const variant_table& values, OutIter out) {
  return encode(values.raw(), out);
}

template <class OutIter>
OutIter encode(const variant_data& value, OutIter out) {
  out = write_unsigned(value.get_tag(), out);
  return std::visit([&](const auto& x) { return encode(x, out); }, value.value);
}

// Note: enable_if trickery to suppress implicit conversions.
template <class Data, class OutIter>
std::enable_if_t<std::is_same_v<Data, data>, OutIter> encode(const Data& value,
                                                             OutIter out);

template <class OutIter>
OutIter encode(const broker::set& values, OutIter out) {
  out = write_varbyte(values.size(), out);
  for (const auto& x : values)
    out = encode(x, out);
  return out;
}

template <class OutIter>
OutIter encode(const broker::table& values, OutIter out) {
  out = write_varbyte(values.size(), out);
  for (const auto& [key, val] : values) {
    out = encode(key, out);
    out = encode(val, out);
  }
  return out;
}

template <class OutIter>
OutIter encode(const broker::vector& values, OutIter out) {
  out = write_varbyte(values.size(), out);
  for (const auto& x : values)
    out = encode(x, out);
  return out;
}

template <class Data, class OutIter>
std::enable_if_t<std::is_same_v<Data, data>, OutIter> encode(const Data& value,
                                                             OutIter out) {
  return std::visit(
    [&](const auto& x) {
      using value_type = std::decay_t<std::remove_const_t<decltype(x)>>;
      out = write_unsigned(data_tag_v<value_type>, out);
      return encode(x, out);
    },
    value.get_data());
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

BROKER_DEF_HAS_ENCODE_IN_NS(broker::format::bin::v1);

/// Adapter for the `inspect` API.
template <class OutIter>
class encoder {
public:
  static constexpr bool is_loading = false;

  explicit encoder(OutIter out) : out_(out) {}

  bool constexpr has_human_readable_format() const noexcept {
    return false;
  }

  template <class T>
  encoder& object(const T&) {
    return *this;
  }

  encoder& pretty_name(std::string_view) {
    return *this;
  }

  template <class T>
  bool apply(const T& value) {
    if constexpr (has_encode_overload_v<T, OutIter>) {
      out_ = encode(value, out_);
      return true;
    } else if constexpr (detail::is_variant<T>) {
      out_ = write_unsigned(static_cast<uint8_t>(value.index()), out_);
      return std::visit([this](const auto& x) { return apply(x); }, value);
    } else if constexpr (detail::is_optional<T>) {
      if (value) {
        out_ = encode(true, out_);
        return apply(*value);
      }
      out_ = encode(false, out_);
      return true;
    } else if constexpr (detail::is_array<T>) {
      for (auto&& item : value)
        if (!apply(item))
          return false;
      return true;
    } else if constexpr (detail::is_map<T>) {
      out_ = write_varbyte(value.size(), out_);
      for (const auto& [key, val] : value)
        if (!apply(key) || !apply(val))
          return false;
      return true;
    } else if constexpr (detail::is_list<T>) {
      out_ = write_varbyte(value.size(), out_);
      for (const auto& val : value)
        if (!apply(val))
          return false;
      return true;
    } else {
      // Inspect overloads are always non-const but serializing is guaranteed to
      // not modify the value.
      return inspect(*this, const_cast<T&>(value));
    }
  }

  template <class Getter, class Setter>
  bool apply(Getter&& get, Setter&&) {
    return apply(get());
  }

  template <class T>
  const T& field(std::string_view, const T& value) {
    return value;
  }

  bool fields() {
    return true;
  }

  template <class T, class... Ts>
  bool fields(const T& value, const Ts&... values) {
    if (!apply(value))
      return false;
    return fields(values...);
  }

  template <class Iterator, class Sentinel>
  void append(Iterator first, Sentinel last) {
    out_ = std::copy(first, last, out_);
  }

private:
  OutIter out_;
};

template <class OutIter, class T>
auto encode_with_tag(const T& x, OutIter out) -> decltype(encode(x, out)) {
  return encode(x, write_unsigned(data_tag_v<T>, out));
}

} // namespace broker::format::bin::v1
