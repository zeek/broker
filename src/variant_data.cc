#include "broker/variant_data.hh"

#include "broker/data.hh"
#include "broker/detail/type_traits.hh"

#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

#include <cstring>

namespace broker {

namespace {

/// Global instance of `variant_data` that represents a `nil` value.
const variant_data nil_instance;

template <variant_tag Tag, class Pred, class T1, class T2>
auto stl_visit(Pred& pred, const T1& lhs, const T2& rhs) {
  static constexpr auto index = static_cast<size_t>(Tag);
  return pred(std::get<index>(lhs.stl_value()),
              std::get<index>(rhs.stl_value()));
}

/// Visits two `data` and/or `variant` objects by calling `pred(lhs, rhs)` if
/// the types of `lhs` and `rhs` are the same. Otherwise, returns
/// `pred(lhs.type(), rhs.type())`.
template <class Predicate, class T1, class T2>
auto visit_if_same_type(Predicate&& pred, const T1& lhs, const T2& rhs) {
  // Note: we could do std::visit here, but that would require the Predicate
  //       to support all possible combinations of types. Instead, we only
  //       require the Predicate to support all combinations of types that
  //       can actually occur.
  auto lhs_type = lhs.get_tag();
  auto rhs_type = rhs.get_tag();
  if (lhs_type != rhs_type)
    return pred(lhs_type, rhs_type);
  using type = variant_tag;
  switch (lhs_type) {
    default: // type::none:
      return stl_visit<type::none>(pred, lhs, rhs);
    case type::boolean:
      return stl_visit<type::boolean>(pred, lhs, rhs);
    case type::integer:
      return stl_visit<type::integer>(pred, lhs, rhs);
    case type::count:
      return stl_visit<type::count>(pred, lhs, rhs);
    case type::real:
      return stl_visit<type::real>(pred, lhs, rhs);
    case type::string:
      return stl_visit<type::string>(pred, lhs, rhs);
    case type::address:
      return stl_visit<type::address>(pred, lhs, rhs);
    case type::subnet:
      return stl_visit<type::subnet>(pred, lhs, rhs);
    case type::port:
      return stl_visit<type::port>(pred, lhs, rhs);
    case type::timestamp:
      return stl_visit<type::timestamp>(pred, lhs, rhs);
    case type::timespan:
      return stl_visit<type::timespan>(pred, lhs, rhs);
    case type::enum_value:
      return stl_visit<type::enum_value>(pred, lhs, rhs);
    case type::set:
      return stl_visit<type::set>(pred, lhs, rhs);
    case type::table:
      return stl_visit<type::table>(pred, lhs, rhs);
    case type::vector:
      return stl_visit<type::vector>(pred, lhs, rhs);
  }
}

/// Compares two `data` and/or `variant` objects for equality.
struct eq_predicate {
  template <class T1, class T2>
  bool operator()(const T1& lhs, const T2& rhs) const {
    if constexpr (std::is_pointer_v<T1>)
      return (*this)(*lhs, rhs);
    else if constexpr (std::is_pointer_v<T2>)
      return (*this)(lhs, *rhs);
    else if constexpr (detail::has_begin_v<T1>)
      return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), *this);
    else if constexpr (detail::is_pair<T1>)
      return (*this)(lhs.first, rhs.first) && (*this)(lhs.second, rhs.second);
    else
      return lhs == rhs;
  }
};

} // namespace

const variant_data* variant_data::nil() noexcept {
  return &nil_instance;
}

data variant_data::to_data() const {
  auto f = [](const auto& val) -> broker::data {
    using val_type = std::decay_t<decltype(val)>;
    if constexpr (std::is_same_v<std::string_view, val_type>) {
      return broker::data{std::string{val}};
    } else if constexpr (std::is_same_v<enum_value_view, val_type>) {
      return broker::data{enum_value{std::string{val.name}}};
    } else if constexpr (std::is_same_v<variant_data::set*, val_type>) {
      broker::set result;
      for (const auto& x : *val)
        result.emplace(x.to_data());
      return broker::data{std::move(result)};
    } else if constexpr (std::is_same_v<variant_data::table*, val_type>) {
      broker::table result;
      for (const auto& [key, val] : *val)
        result.emplace(key.to_data(), val.to_data());
      return broker::data{std::move(result)};
    } else if constexpr (std::is_same_v<variant_data::list*, val_type>) {
      broker::vector result;
      result.reserve(val->size());
      for (const auto& x : *val)
        result.emplace_back(x.to_data());
      return broker::data{std::move(result)};
    } else {
      return broker::data{val};
    }
  };
  return std::visit(f, value);
}

namespace {

template <class T>
using mbr_allocator = broker::detail::monotonic_buffer_resource::allocator<T>;

using const_byte_pointer = const std::byte*;

/// Reads a 64-bit unsigned integer from a byte sequence.
uint64_t rd_u64(const_byte_pointer& bytes) {
  broker::count tmp = 0;
  memcpy(&tmp, bytes, sizeof(broker::count));
  bytes += sizeof(broker::count);
  return caf::detail::from_network_order(tmp);
}

/// Reads an 8-bit unsigned integer from a byte sequence.
uint8_t rd_u8(const_byte_pointer& bytes) {
  auto result = *bytes++;
  return static_cast<uint8_t>(result);
}

/// Reads a 16-bit unsigned integer from a byte sequence.
uint16_t rd_u16(const_byte_pointer& bytes) {
  uint16_t tmp = 0;
  memcpy(&tmp, bytes, sizeof(uint16_t));
  bytes += sizeof(uint16_t);
  return caf::detail::from_network_order(tmp);
}

/// Reads a size_t from a byte sequence using varbyte encoding.
bool rd_varbyte(const_byte_pointer& first, const_byte_pointer last,
                size_t& result) {
  // Use varbyte encoding to compress sequence size on the wire.
  uint32_t x = 0;
  int n = 0;
  uint8_t low7 = 0;
  do {
    if (first == last)
      return false;
    low7 = rd_u8(first);
    x |= static_cast<uint32_t>((low7 & 0x7F)) << (7 * n);
    ++n;
  } while (low7 & 0x80);
  result = x;
  return true;
}

// Like sizeof(), but returns a ptrdiff_t instead of a size_t.
template <class T>
ptrdiff_t ssizeof() {
  return static_cast<ptrdiff_t>(sizeof(T));
}

} // namespace

std::pair<bool, const std::byte*>
variant_data::parse_shallow(detail::monotonic_buffer_resource& buf,
                            const std::byte* pos, const std::byte* end) {
  if (pos == end)
    return {false, end};
  switch (static_cast<variant_tag>(*pos++)) {
    case variant_tag::none:
      value = none{};
      return {true, pos};
    case variant_tag::boolean:
      if (pos == end)
        return {false, end};
      value = *pos++ != std::byte{0};
      return {true, pos};
    case variant_tag::count:
      if (end - pos < ssizeof<count>())
        return {false, end};
      value = rd_u64(pos);
      return {true, pos};
    case variant_tag::integer:
      if (end - pos < ssizeof<count>())
        return {false, end};
      value = static_cast<broker::integer>(rd_u64(pos));
      return {true, pos};
    case variant_tag::real:
      if (end - pos < ssizeof<real>())
        return {false, end};
      value = caf::detail::unpack754(rd_u64(pos));
      return {true, pos};
    case variant_tag::string: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      if (end - pos < static_cast<ptrdiff_t>(size))
        return {false, pos};
      auto str = reinterpret_cast<const char*>(pos);
      pos += size;
      value = std::string_view{str, size};
      return {true, pos};
    }
    case variant_tag::address: {
      if (end - pos < static_cast<ptrdiff_t>(address::num_bytes))
        return {false, end};
      address tmp;
      memcpy(tmp.bytes().data(), pos, address::num_bytes);
      pos += address::num_bytes;
      value = tmp;
      return {true, pos};
    }
    case variant_tag::subnet: {
      static constexpr size_t subnet_len = address::num_bytes + 1;
      if (end - pos < static_cast<ptrdiff_t>(subnet_len))
        return {false, end};
      address addr;
      memcpy(addr.bytes().data(), pos, address::num_bytes);
      pos += address::num_bytes;
      auto length = rd_u8(pos);
      value = subnet{addr, length};
      return {true, pos};
    }
    case variant_tag::port: {
      if (end - pos < 3)
        return {false, end};
      auto num = rd_u16(pos);
      auto proto = rd_u8(pos);
      if (proto > 3) // 3 is the highest protocol number we support (ICMP).
        return {false, end};
      value = port{num, static_cast<port::protocol>(proto)};
      return {true, pos};
    }
    case variant_tag::timestamp: {
      if (end - pos < ssizeof<timespan>())
        return {false, end};
      value = timestamp{timespan{rd_u64(pos)}};
      return {true, pos};
    }
    case variant_tag::timespan: {
      if (end - pos < ssizeof<timespan>())
        return {false, end};
      value = timespan{rd_u64(pos)};
      return {true, pos};
    }
    case variant_tag::enum_value: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      if (end - pos < static_cast<ptrdiff_t>(size))
        return {false, pos};
      auto str = reinterpret_cast<const char*>(pos);
      pos += size;
      value = enum_value_view{std::string_view{str, size}};
      return {true, pos};
    }
    case variant_tag::set: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      using set_allocator = mbr_allocator<variant_data>;
      using set_type = variant_data::set;
      mbr_allocator<set_type> allocator{&buf};
      auto res = new (allocator.allocate(1)) set_type(set_allocator{&buf});
      for (size_t i = 0; i < size; ++i) {
        auto tmp = variant_data{};
        auto [ok, next] = tmp.parse_shallow(buf, pos, end);
        if (!ok)
          return {false, pos};
        auto [_, added] = res->emplace(std::move(tmp));
        if (!added)
          return {false, pos};
        pos = next;
      }
      value = res;
      return {true, pos};
    }
    case variant_tag::table: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      using table_allocator = variant_data::table_allocator;
      using table_type = variant_data::table;
      mbr_allocator<table_type> allocator{&buf};
      auto res = new (allocator.allocate(1)) table_type(table_allocator{&buf});
      for (size_t i = 0; i < size; ++i) {
        auto key = variant_data{};
        if (auto [ok, next] = key.parse_shallow(buf, pos, end); ok)
          pos = next;
        else
          return {false, pos};
        auto val = variant_data{};
        if (auto [ok, next] = val.parse_shallow(buf, pos, end); ok)
          pos = next;
        else
          return {false, pos};
        auto [_, added] = res->emplace(std::move(key), std::move(val));
        if (!added)
          return {false, pos};
      }
      value = res;
      return {true, pos};
    }
    case variant_tag::list: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      using vec_allocator = mbr_allocator<variant_data>;
      using vec_type = variant_data::list;
      mbr_allocator<vec_type> allocator{&buf};
      auto vec = new (allocator.allocate(1)) vec_type(vec_allocator{&buf});
      for (size_t i = 0; i < size; ++i) {
        auto [ok, next] = vec->emplace_back().parse_shallow(buf, pos, end);
        if (!ok)
          return {false, pos};
        pos = next;
      }
      value = vec;
      return {true, pos};
    }
    default:
      return {false, pos};
  }
}

// -- free functions -----------------------------------------------------------

bool operator==(const data& lhs, const variant_data& rhs) {
  return visit_if_same_type(eq_predicate{}, lhs, rhs);
}

bool operator==(const variant_data& lhs, const data& rhs) {
  return visit_if_same_type(eq_predicate{}, lhs, rhs);
}

bool operator==(const variant_data& lhs, const variant_data& rhs) {
  return visit_if_same_type(eq_predicate{}, lhs, rhs);
}

bool operator<(const variant_data& lhs, const variant_data& rhs) {
  if (lhs.value.index() != rhs.value.index())
    return lhs.value.index() < rhs.value.index();
  return std::visit(
    [&rhs](const auto& x) -> bool {
      using T = std::decay_t<decltype(x)>;
      if constexpr (std::is_pointer_v<T>) {
        return *x < *std::get<T>(rhs.value);
      } else {
        return x < std::get<T>(rhs.value);
      }
    },
    lhs.value);
}

} // namespace broker
