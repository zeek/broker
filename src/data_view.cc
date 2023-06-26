#include "broker/data_view.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/btf.hh"
#include "broker/detail/type_traits.hh"
#include "broker/internal/stringifier.hh"
#include "broker/internal/type_id.hh"
#include "broker/topic.hh"

#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

#include <functional>
#include <iterator>
#include <memory>
#include <type_traits>

namespace broker::detail {

namespace {

template <class T>
using mbr_allocator = broker::detail::monotonic_buffer_resource::allocator<T>;

using const_byte_pointer = const std::byte*;

/// Visits two `data` and/or `data_view` objects by calling `pred(lhs, rhs)` if
/// the types of `lhs` and `rhs` are the same. Otherwise, returns
/// `pred(lhs.type(), rhs.type())`.
template <class Predicate, class T1, class T2>
auto visit_if_same_type(Predicate&& pred, const T1& lhs, const T2& rhs) {
  // Note: we could do std::visit here, but that would require the Predicate
  //       to support all possible combinations of types. Instead, we only
  //       require the Predicate to support all combinations of types that
  //       can actually occur.
  auto lhs_type = lhs.get_type();
  auto rhs_type = rhs.get_type();
  if (lhs_type != rhs_type)
    return pred(lhs_type, rhs_type);
  using type = data::type;
  switch (lhs_type) {
    default: // type::none:
      return pred(get<type::none>(lhs), get<type::none>(rhs));
    case type::boolean:
      return pred(get<type::boolean>(lhs), get<type::boolean>(rhs));
    case type::integer:
      return pred(get<type::integer>(lhs), get<type::integer>(rhs));
    case type::count:
      return pred(get<type::count>(lhs), get<type::count>(rhs));
    case type::real:
      return pred(get<type::real>(lhs), get<type::real>(rhs));
    case type::string:
      return pred(get<type::string>(lhs), get<type::string>(rhs));
    case type::address:
      return pred(get<type::address>(lhs), get<type::address>(rhs));
    case type::subnet:
      return pred(get<type::subnet>(lhs), get<type::subnet>(rhs));
    case type::port:
      return pred(get<type::port>(lhs), get<type::port>(rhs));
    case type::timestamp:
      return pred(get<type::timestamp>(lhs), get<type::timestamp>(rhs));
    case type::timespan:
      return pred(get<type::timespan>(lhs), get<type::timespan>(rhs));
    case type::enum_value:
      return pred(get<type::enum_value>(lhs), get<type::enum_value>(rhs));
    case type::set:
      return pred(get<type::set>(lhs), get<type::set>(rhs));
    case type::table:
      return pred(get<type::table>(lhs), get<type::table>(rhs));
    case type::vector:
      return pred(get<type::vector>(lhs), get<type::vector>(rhs));
  }
}

struct eq_predicate {
  template <class T1, class T2>
  bool operator()(const T1& lhs, const T2& rhs) const noexcept {
    if constexpr (std::is_pointer_v<T1>)
      return (*this)(*lhs, rhs);
    else if constexpr (std::is_pointer_v<T2>)
      return (*this)(lhs, *rhs);
    else if constexpr (has_begin_v<T1>)
      return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), *this);
    else if constexpr (is_pair_v<T1>)
      return (*this)(lhs.first, rhs.first) && (*this)(lhs.second, rhs.second);
    else
      return lhs == rhs;
  }
};

} // namespace

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

std::pair<bool, const_byte_pointer>
parse_shallow(detail::monotonic_buffer_resource& buf, data_view_value& value,
              const_byte_pointer pos, const_byte_pointer end) {
  if (pos == end)
    return {false, end};
  switch (static_cast<data::type>(*pos++)) {
    case data::type::none:
      value.data = none{};
      return {true, pos};
    case data::type::boolean:
      if (pos == end)
        return {false, end};
      value.data = *pos++ != std::byte{0};
      return {true, pos};
    case data::type::count:
      if (end - pos < sizeof(count))
        return {false, end};
      value.data = rd_u64(pos);
      return {true, pos};
    case data::type::integer:
      if (end - pos < sizeof(count))
        return {false, end};
      value.data = static_cast<broker::integer>(rd_u64(pos));
      return {true, pos};
    case data::type::real:
      if (end - pos < sizeof(real))
        return {false, end};
      value.data = caf::detail::unpack754(rd_u64(pos));
      return {true, pos};
    case data::type::string: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      if (end - pos < static_cast<ptrdiff_t>(size))
        return {false, pos};
      auto str = reinterpret_cast<const char*>(pos);
      pos += size;
      value.data = std::string_view{str, size};
      return {true, pos};
    }
    case data::type::address: {
      if (end - pos < address::num_bytes)
        return {false, end};
      address tmp;
      memcpy(tmp.bytes().data(), pos, address::num_bytes);
      pos += address::num_bytes;
      value.data = tmp;
      return {true, pos};
    }
    case data::type::subnet: {
      static constexpr size_t subnet_len = address::num_bytes + 1;
      if (end - pos < subnet_len)
        return {false, end};
      address addr;
      memcpy(addr.bytes().data(), pos, address::num_bytes);
      pos += address::num_bytes;
      auto length = rd_u8(pos);
      value.data = subnet{addr, length};
      return {true, pos};
    }
    case data::type::port: {
      if (end - pos < 3)
        return {false, end};
      auto num = rd_u16(pos);
      auto proto = rd_u8(pos);
      if (proto > 3) // 3 is the highest protocol number we support (ICMP).
        return {false, end};
      value.data = port{num, static_cast<port::protocol>(proto)};
      return {true, pos};
    }
    case data::type::timestamp: {
      if (end - pos < sizeof(timespan))
        return {false, end};
      value.data = timestamp{timespan{rd_u64(pos)}};
      return {true, pos};
    }
    case data::type::timespan: {
      if (end - pos < sizeof(timespan))
        return {false, end};
      value.data = timespan{rd_u64(pos)};
      return {true, pos};
    }
    case data::type::enum_value: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      if (end - pos < static_cast<ptrdiff_t>(size))
        return {false, pos};
      auto str = reinterpret_cast<const char*>(pos);
      pos += size;
      value.data = enum_value_view{std::string_view{str, size}};
      return {true, pos};
    }
    case data::type::set: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      using set_allocator = mbr_allocator<data_view_value>;
      using set_type = data_view_value::set_view;
      mbr_allocator<set_type> allocator{&buf};
      auto res = new (allocator.allocate(1)) set_type(set_allocator{&buf});
      for (size_t i = 0; i < size; ++i) {
        auto tmp = data_view_value{};
        auto [ok, next] = parse_shallow(buf, tmp, pos, end);
        if (!ok)
          return {false, pos};
        auto [_, added] = res->emplace(std::move(tmp));
        if (!added)
          return {false, pos};
        pos = next;
      }
      value.data = res;
      return {true, pos};
    }
    case data::type::table: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      using table_allocator = data_view_value::table_allocator;
      using table_type = data_view_value::table_view;
      mbr_allocator<table_type> allocator{&buf};
      auto res = new (allocator.allocate(1)) table_type(table_allocator{&buf});
      for (size_t i = 0; i < size; ++i) {
        auto key = data_view_value{};
        if (auto [ok, next] = parse_shallow(buf, key, pos, end); ok)
          pos = next;
        else
          return {false, pos};
        auto val = data_view_value{};
        if (auto [ok, next] = parse_shallow(buf, val, pos, end); ok)
          pos = next;
        else
          return {false, pos};
        auto [_, added] = res->emplace(std::move(key), std::move(val));
        if (!added)
          return {false, pos};
      }
      value.data = res;
      return {true, pos};
    }
    case data::type::vector: {
      size_t size = 0;
      if (!rd_varbyte(pos, end, size))
        return {false, pos};
      using vec_allocator = mbr_allocator<data_view_value>;
      using vec_type = data_view_value::vector_view;
      mbr_allocator<vec_type> allocator{&buf};
      auto vec = new (allocator.allocate(1)) vec_type(vec_allocator{&buf});
      for (size_t i = 0; i < size; ++i) {
        auto [ok, next] = parse_shallow(buf, vec->emplace_back(), pos, end);
        if (!ok)
          return {false, pos};
        pos = next;
      }
      value.data = vec;
      return {true, pos};
    }
    default:
      return {false, pos};
  }
}

namespace {

const data_view_value nil_instance;

const data_view_value::set_view empty_set_instance;

const data_view_value::table_view empty_table_instance;

const data_view_value::vector_view empty_vector_instance;

} // namespace

const data_view_value* data_view_value::nil() noexcept {
  return &nil_instance;
}

data data_view_value::deep_copy() const {
  auto f = [](const auto& value) -> broker::data {
    using value_type = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<std::string_view, value_type>) {
      return broker::data{std::string{value}};
    } else if constexpr (std::is_same_v<enum_value_view, value_type>) {
      return broker::data{enum_value{std::string{value.name}}};
    } else if constexpr (std::is_same_v<set_view*, value_type>) {
      broker::set result;
      for (const auto& x : *value)
        result.emplace(x.deep_copy());
      return broker::data{std::move(result)};
    } else if constexpr (std::is_same_v<table_view*, value_type>) {
      broker::table result;
      for (const auto& [key, val] : *value)
        result.emplace(key.deep_copy(), val.deep_copy());
      return broker::data{std::move(result)};
    } else if constexpr (std::is_same_v<vector_view*, value_type>) {
      broker::vector result;
      result.reserve(value->size());
      for (const auto& x : *value)
        result.emplace_back(x.deep_copy());
      return broker::data{std::move(result)};
    } else {
      return broker::data{value};
    }
  };
  return std::visit(f, data);
}

bool operator==(const data& lhs, const data_view_value& rhs) noexcept {
  return visit_if_same_type(eq_predicate{}, lhs, rhs);
}

bool operator==(const data_view_value& lhs, const data& rhs) noexcept {
  return visit_if_same_type(eq_predicate{}, lhs, rhs);
}

bool operator<(const data_view_value& lhs,
               const data_view_value& rhs) noexcept {
  if (lhs.data.index() != rhs.data.index())
    return lhs.data.index() < rhs.data.index();
  return std::visit(
    [&rhs](const auto& x) -> bool {
      using T = std::decay_t<decltype(x)>;
      if constexpr (std::is_pointer_v<T>) {
        return *x < *std::get<T>(rhs.data);
      } else {
        return x < std::get<T>(rhs.data);
      }
    },
    lhs.data);
}

} // namespace broker::detail

namespace broker {

data_envelope::~data_envelope() {
  // nop
}

detail::data_view_value*
data_envelope::do_parse(detail::monotonic_buffer_resource& buf, error& err) {
  auto [bytes, size] = raw_bytes();
  if (bytes == nullptr || size == 0) {
    err = make_error(ec::deserialization_failed, "cannot parse null data");
    return nullptr;
  }
  // Create the root object.
  detail::data_view_value* root;
  {
    detail::mbr_allocator<detail::data_view_value> allocator{&buf};
    root = new (allocator.allocate(1)) detail::data_view_value();
  }
  // Parse the data. This is a shallow parse, which is why we need to copy the
  // bytes into the buffer resource first.
  auto end = bytes + size;
  auto [ok, pos] = parse_shallow(buf, *root, bytes, end);
  if (ok && pos == end)
    return root;
  err = make_error(ec::deserialization_failed, "failed to parse data");
  return nullptr;
}

namespace {

/// The default implementation for @ref data_envelope that wraps a byte buffer
/// and a topic..
class default_data_envelope : public data_envelope {
public:
  default_data_envelope(topic t, caf::byte_buffer bytes)
    : topic_(std::move(t)), bytes_(std::move(bytes)) {
    // nop
  }

  data_view get_data() const noexcept override {
    return {root_, shared_from_this()};
  }

  const topic& get_topic() const noexcept override {
    return topic_;
  }

  bool is_root(const detail::data_view_value* val) const noexcept override {
    return val == root_;
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {reinterpret_cast<const std::byte*>(bytes_.data()), bytes_.size()};
  }

  error parse() {
    error result;
    root_ = do_parse(buf_, result);
    return result;
  }

private:
  detail::data_view_value* root_ = nullptr;
  topic topic_;
  caf::byte_buffer bytes_;
  detail::monotonic_buffer_resource buf_;
};

} // namespace

data_envelope_ptr data_envelope::make(topic t, const data& d) {
  caf::byte_buffer buf;
  caf::binary_serializer sink{nullptr, buf};
#ifndef NDEBUG
  if (auto ok = sink.apply(d); !ok) {
    auto errstr = caf::to_string(sink.get_error());
    fprintf(stderr,
            "broker::data_envelope::make failed to serialize data: %s\n",
            errstr.c_str());
    abort();
  }
#else
  std::ignore = sink.apply(d);
#endif
  auto res = std::make_shared<default_data_envelope>(std::move(t),
                                                     std::move(buf));
#ifndef NDEBUG
  if (auto err = res->parse()) {
    auto errstr = to_string(err);
    fprintf(stderr,
            "broker::data_envelope::make generated malformed data: %s\n",
            errstr.c_str());
    abort();
  }
#else
  std::ignore = res->parse();
#endif
  return res;
}

namespace {

/// Wraps a data view and a topic.
class data_envelope_wrapper : public data_envelope {
public:
  data_envelope_wrapper(topic t, data_view val)
    : topic_(std::move(t)), val_(std::move(val)) {
    // nop
  }

  data_view get_data() const noexcept override {
    return val_;
  }

  const topic& get_topic() const noexcept override {
    return topic_;
  }

  bool is_root(const detail::data_view_value* val) const noexcept override {
    return val == val_.raw_ptr() && val_.is_root();
  }


  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    if (val_.is_root())
      val_.envelope_ptr()->raw_bytes();
    return {nullptr, 0};
  }

private:
  topic topic_;
  data_view val_;
};

} // namespace

data_envelope_ptr data_envelope::make(topic t, data_view d) {
  return std::make_shared<data_envelope_wrapper>(std::move(t), std::move(d));
}

data data_view::deep_copy() const {
  return value_->deep_copy();
}

set_view data_view::to_set() const noexcept {
  using detail_t = detail::data_view_value::set_view*;
  if (auto ptr = std::get_if<detail_t>(&value_->data))
    return set_view{*ptr, envelope_};
  return set_view{&detail::empty_set_instance, nullptr};
}

table_view data_view::to_table() const noexcept {
  using detail_t = detail::data_view_value::table_view*;
  if (auto ptr = std::get_if<detail_t>(&value_->data))
    return table_view{*ptr, envelope_};
  return table_view{&detail::empty_table_instance, nullptr};
}

vector_view data_view::to_vector() const noexcept {
  using detail_t = detail::data_view_value::vector_view*;
  if (auto ptr = std::get_if<detail_t>(&value_->data))
    return vector_view{*ptr, envelope_};
  return vector_view{&detail::empty_vector_instance, nullptr};
}

void convert(const data_view& value, std::string& out) {
  detail::btf::encode(value.raw_ptr(), std::back_inserter(out));
}

void convert(const set_view& value, std::string& out) {
  detail::btf::encode(value.raw_ptr(), std::back_inserter(out));
}

void convert(const table_view& value, std::string& out) {
  detail::btf::encode(value.raw_ptr(), std::back_inserter(out));
}

void convert(const vector_view& value, std::string& out) {
  detail::btf::encode(value.raw_ptr(), std::back_inserter(out));
}

std::ostream& operator<<(std::ostream& out, const data_view& what) {
  detail::btf::encode(what, std::ostream_iterator<char>(out));
  return out;
}

std::ostream& operator<<(std::ostream& out, const set_view& what) {
  detail::btf::encode(what.raw_ptr(), std::ostream_iterator<char>(out));
  return out;
}

std::ostream& operator<<(std::ostream& out, const table_view& what) {
  detail::btf::encode(what.raw_ptr(), std::ostream_iterator<char>(out));
  return out;
}

std::ostream& operator<<(std::ostream& out, const vector_view& what) {
  detail::btf::encode(what.raw_ptr(), std::ostream_iterator<char>(out));
  return out;
}

} // namespace broker
