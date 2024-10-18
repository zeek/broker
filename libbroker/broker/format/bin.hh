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

/// Reads a single 8-bit unsigned integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, uint8_t& result);

/// Reads a single 16-bit unsigned integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, uint16_t& result);

/// Reads a single 16-bit unsigned integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, uint32_t& result);

/// Reads a single 64-bit unsigned integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, uint64_t& result);

/// Reads a single 8-bit signed integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, int8_t& result);

/// Reads a single 16-bit signed integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, int16_t& result);

/// Reads a single 32-bit signed integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, int32_t& result);

/// Reads a single 64-bit signed integer from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, int64_t& result);

/// Reads a single 64-bit floating point number from a byte sequence.
bool read(const_byte_pointer& first, const_byte_pointer last, double& result);

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
    static_assert(std::is_signed_v<T>);
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

  template <class T>
  bool begin_object_t() {
    return true;
  }

  bool end_object() {
    return true;
  }

  bool begin_field(std::string_view) {
    return true;
  }

  bool end_field() {
    return true;
  }

  bool begin_sequence(size_t num_elements) {
    out_ = write_varbyte(num_elements, out_);
    return true;
  }

  bool end_sequence() {
    return true;
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
  decltype(auto) field(std::string_view, T&& value) {
    return std::forward<T>(value);
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

/// Decodes a broker::data or broker::variant from a binary representation. The
/// decoding functions will generate a series of events for the handler, similar
/// to a SAX parser.
template <class Handler>
std::pair<bool, const std::byte*>
decode(const std::byte* pos, const std::byte* end, Handler& handler) {
  if (pos == end)
    return {false, end};
  switch (static_cast<variant_tag>(*pos++)) {
    case variant_tag::none:
      return {handler.value(nil), pos};
    case variant_tag::boolean:
      if (pos == end)
        return {false, end};
      return {handler.value(*pos++ != std::byte{0}), pos};
    case variant_tag::count: {
      auto tmp = uint64_t{0};
      if (!read(pos, end, tmp))
        return {false, pos};
      return {handler.value(broker::count{tmp}), pos};
    }
    case variant_tag::integer: {
      auto tmp = uint64_t{0};
      if (!read(pos, end, tmp))
        return {false, pos};
      return {handler.value(static_cast<broker::integer>(tmp)), pos};
    }
    case variant_tag::real: {
      auto tmp = 0.0;
      if (!read(pos, end, tmp))
        return {false, pos};
      return {handler.value(tmp), pos};
    }
    case variant_tag::string: {
      auto size = size_t{0};
      if (!read_varbyte(pos, end, size))
        return {false, pos};
      if (end - pos < static_cast<ptrdiff_t>(size))
        return {false, pos};
      auto str = reinterpret_cast<const char*>(pos);
      pos += size;
      return {handler.value(std::string_view{str, size}), pos};
    }
    case variant_tag::address: {
      if (end - pos < static_cast<ptrdiff_t>(address::num_bytes))
        return {false, end};
      address tmp;
      memcpy(tmp.bytes().data(), pos, address::num_bytes);
      pos += address::num_bytes;
      return {handler.value(tmp), pos};
    }
    case variant_tag::subnet: {
      static constexpr size_t subnet_len = address::num_bytes + 1;
      if (end - pos < static_cast<ptrdiff_t>(subnet_len))
        return {false, end};
      address tmp;
      memcpy(tmp.bytes().data(), pos, address::num_bytes);
      pos += address::num_bytes;
      auto length = uint8_t{0};
      if (!read(pos, end, length))
        return {false, pos};
      return {handler.value(subnet::unchecked(tmp, length)), pos};
    }
    case variant_tag::port: {
      if (end - pos < 3)
        return {false, end};
      auto num = uint16_t{0};
      if (!read(pos, end, num))
        return {false, pos};
      auto proto = uint8_t{0};
      if (!read(pos, end, proto))
        return {false, pos};
      // ICMP has the highest protocol number we support
      if (proto > static_cast<uint8_t>(port::protocol::icmp))
        return {false, end};
      return {handler.value(port{num, static_cast<port::protocol>(proto)}),
              pos};
    }
    case variant_tag::timestamp: {
      auto tmp = uint64_t{0};
      if (!read(pos, end, tmp))
        return {false, pos};
      return {handler.value(timestamp{timespan{tmp}}), pos};
    }
    case variant_tag::timespan: {
      auto tmp = uint64_t{0};
      if (!read(pos, end, tmp))
        return {false, pos};
      return {handler.value(timespan{tmp}), pos};
    }
    case variant_tag::enum_value: {
      size_t size = 0;
      if (!format::bin::v1::read_varbyte(pos, end, size))
        return {false, pos};
      if (end - pos < static_cast<ptrdiff_t>(size))
        return {false, pos};
      auto str = reinterpret_cast<const char*>(pos);
      pos += size;
      return {handler.value(enum_value_view{std::string_view{str, size}}), pos};
    }
    case variant_tag::set: {
      size_t size = 0;
      if (!format::bin::v1::read_varbyte(pos, end, size))
        return {false, pos};
      auto&& nested = handler.begin_set();
      for (size_t i = 0; i < size; ++i) {
        auto [ok, next] = decode(pos, end, nested);
        if (!ok)
          return {false, pos};
        pos = next;
      }
      return {handler.end_set(nested), pos};
    }
    case variant_tag::table: {
      size_t size = 0;
      if (!format::bin::v1::read_varbyte(pos, end, size))
        return {false, pos};
      auto&& nested = handler.begin_table();
      for (size_t i = 0; i < size; ++i) {
        nested.begin_key_value_pair();
        if (auto [ok, next] = decode(pos, end, nested); ok) {
          pos = next;
        } else {
          return {false, next};
        }
        if (auto [ok, next] = decode(pos, end, nested); ok) {
          pos = next;
        } else {
          return {false, next};
        }
        nested.end_key_value_pair();
      }
      return {handler.end_table(nested), pos};
    }
    case variant_tag::list: {
      size_t size = 0;
      if (!format::bin::v1::read_varbyte(pos, end, size))
        return {false, pos};
      auto&& nested = handler.begin_list();
      for (size_t i = 0; i < size; ++i) {
        auto [ok, next] = decode(pos, end, nested);
        if (!ok)
          return {false, next};
        pos = next;
      }
      return {handler.end_list(nested), pos};
    }
    default:
      return {false, pos};
  }
}

/// Adapter for the `inspect` API.
class decoder {
public:
  static constexpr bool is_loading = true;

  decoder(const_byte_pointer first, const_byte_pointer last)
    : pos_(first), end_(last) {}

  decoder(const void* data, size_t size)
    : pos_(static_cast<const std::byte*>(data)), end_(pos_ + size) {}

  void reset(const_byte_pointer first, const_byte_pointer last) {
    pos_ = first;
    end_ = last;
  }

  void reset(const void* data, size_t size) {
    pos_ = static_cast<const std::byte*>(data);
    end_ = pos_ + size;
  }

  bool constexpr has_human_readable_format() const noexcept {
    return false;
  }

  template <class T>
  decoder& object(const T&) {
    return *this;
  }

  template <class T>
  bool begin_object_t() {
    return true;
  }

  bool end_object() {
    return true;
  }

  bool begin_field(std::string_view) {
    return true;
  }

  bool end_field() {
    return true;
  }

  bool begin_sequence(size_t& num_elements) {
    return read_varbyte(pos_, end_, num_elements);
  }

  bool end_sequence() {
    return true;
  }

  decoder& pretty_name(std::string_view) {
    return *this;
  }

  template <class T>
  bool apply(T& value) {
    if constexpr (detail::is_variant<T>) {
      auto index = uint8_t{0};
      if (!apply(index)) {
        return false;
      }
      return decode_variant<0>(value, index);
    } else if constexpr (std::is_same_v<T, none>) {
      return true;
    } else if constexpr (detail::is_duration<T>) {
      auto tmp = typename T::rep{0};
      if (!apply(tmp)) {
        return false;
      }
      value = T{tmp};
      return true;
    } else if constexpr (std::is_same_v<T, timestamp>) {
      auto tmp = timespan{0};
      if (!apply(tmp)) {
        return false;
      }
      value = timestamp{tmp};
      return true;
    } else if constexpr (std::is_same_v<T, bool>) {
      auto tmp = uint8_t{0};
      if (!read(pos_, end_, tmp))
        return false;
      value = tmp != 0;
      return true;
    } else if constexpr (std::is_same_v<T, std::byte>) {
      if (pos_ == end_)
        return false;
      value = *pos_++;
      return true;
    } else if constexpr (std::is_integral_v<T> || std::is_same_v<T, double>) {
      return read(pos_, end_, value);
    } else if constexpr (std::is_same_v<T, std::string>) {
      auto size = size_t{0};
      if (!read_varbyte(pos_, end_, size))
        return false;
      if (end_ - pos_ < static_cast<ptrdiff_t>(size))
        return false;
      value.assign(reinterpret_cast<const char*>(pos_), size);
      pos_ += size;
      return true;
    } else if constexpr (detail::is_optional<T>) {
      uint8_t flag = 0;
      if (!read(pos_, end_, flag)) {
        return false;
      }
      if (flag == 0) {
        value.reset();
        return true;
      }
      auto& nested = value.emplace();
      return apply(nested);
    } else if constexpr (detail::is_tuple<T>) {
      return decode_tuple<0>(value);
    } else if constexpr (detail::is_array<T>) {
      for (auto&& item : value)
        if (!apply(item))
          return false;
      return true;
    } else if constexpr (detail::is_map<T>) {
      auto size = size_t{0};
      if (!read_varbyte(pos_, end_, size))
        return false;
      for (size_t i = 0; i < size; ++i) {
        auto key = typename T::key_type{};
        auto val = typename T::mapped_type{};
        if (!apply(key) || !apply(val)) {
          return false;
        }
        if (!value.emplace(std::move(key), std::move(val)).second) {
          return false;
        }
      }
      return true;
    } else if constexpr (detail::is_list<T>) {
      auto size = size_t{0};
      if (!read_varbyte(pos_, end_, size))
        return false;
      for (size_t i = 0; i < size; ++i)
        if (!apply(value.emplace_back()))
          return false;
      return true;
    } else if constexpr (detail::is_set<T>) {
      auto size = size_t{0};
      if (!read_varbyte(pos_, end_, size))
        return false;
      for (size_t i = 0; i < size; ++i) {
        auto tmp = typename T::value_type{};
        if (!apply(tmp) || !value.insert(std::move(tmp)).second) {
          return false;
        }
      }
      return true;
    } else {
      return inspect(*this, value);
    }
  }

  template <class Getter, class Setter>
  bool apply(Getter getter, Setter setter) {
    using value_t = decltype(getter());
    auto tmp = value_t{};
    if (!apply(tmp))
      return false;
    setter(std::move(tmp));
    return true;
  }

  template <class T>
  T& field(std::string_view, T& value) {
    return value;
  }

  bool fields() {
    return true;
  }

  template <class T, class... Ts>
  bool fields(T& value, Ts&... values) {
    if (!apply(value))
      return false;
    return fields(values...);
  }

  size_t remaining() const noexcept {
    return static_cast<size_t>(end_ - pos_);
  }

private:
  template <size_t Pos, class... Ts>
  bool decode_variant(std::variant<Ts...>& value, size_t pos) {
    if constexpr (Pos == sizeof...(Ts)) {
      return false;
    } else {
      if (pos == Pos) {
        using variant_t = std::variant<Ts...>;
        using value_t = std::variant_alternative_t<Pos, variant_t>;
        auto tmp = value_t{};
        if (!apply(tmp)) {
          return false;
        }
        value.template emplace<value_t>(std::move(tmp));
        return true;
      }
      return decode_variant<Pos + 1>(value, pos);
    }
  }

  template <size_t Pos, class... Ts>
  bool decode_tuple(std::tuple<Ts...>& value) {
    if constexpr (Pos == sizeof...(Ts)) {
      return true;
    } else {
      if (!apply(std::get<Pos>(value))) {
        return false;
      }
      return decode_tuple<Pos + 1>(value);
    }
  }

  const_byte_pointer pos_;
  const_byte_pointer end_;
};

template <class OutIter, class T>
auto encode_with_tag(const T& x, OutIter out) -> decltype(encode(x, out)) {
  return encode(x, write_unsigned(data_tag_v<T>, out));
}

} // namespace broker::format::bin::v1
