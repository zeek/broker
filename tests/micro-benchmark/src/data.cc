#include "broker/data.hh"
#include "broker/configuration.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/expected.hh"
#include "broker/internal/json_type_mapper.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/detail/network_order.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <caf/json_reader.hpp>

#include <benchmark/benchmark.h>

#include <cstring>
#include <list>
#include <map>
#include <set>

#include <iomanip> // delme
#include <iostream> //delme

using namespace broker;

namespace {

// A simple Zeek event called "event_1" with two arguments (42 and "test").
constexpr std::string_view event_1_json = R"_({
  "@data-type": "vector",
  "data": [
    {
      "@data-type": "count",
      "data": 1
    },
    {
      "@data-type": "count",
      "data": 1
    },
    {
      "@data-type": "vector",
      "data": [
        {
          "@data-type": "string",
          "data": "event_1"
        },
        {
          "@data-type": "vector",
          "data": [
            {
              "@data-type": "integer",
              "data": 42
            },
            {
              "@data-type": "string",
              "data": "test"
            }
          ]
        }
      ]
    }
  ]
})_";

constexpr std::string_view table_1_json = R"_({
  "@data-type": "table",
  "data": [
    {
      "key": {
        "@data-type": "string",
        "data": "first-name"
      },
      "value": {
        "@data-type": "string",
        "data": "John"
      }
    },
    {
      "key": {
        "@data-type": "string",
        "data": "last-name"
      },
      "value": {
        "@data-type": "string",
        "data": "Doe"
      }
    }
  ]
})_";

class broker_data : public benchmark::Fixture {
public:
  data event_1;
  caf::byte_buffer event_1_bytes;

  data table_1;
  caf::byte_buffer table_1_bytes;

  broker_data() {
    internal::json_type_mapper mapper;
    caf::json_reader reader;
    reader.mapper(&mapper);
    // Read the JSON for event-1.
    if (!reader.load(event_1_json))
      throw std::logic_error("failed to parse data JSON: "
                             + to_string(reader.get_error()));
    if (!reader.apply(event_1))
      throw std::logic_error("failed to parse data from the JSON: "
                             + to_string(reader.get_error()));
    printf("event_1: %s\n", to_string(event_1).c_str());
    // Serialize event-1 to the native format.
    {
      caf::binary_serializer snk{nullptr, event_1_bytes};
      if (!snk.apply(event_1))
        throw std::logic_error("failed to serialize event_1");
      printf("serialized event_1 has %d bytes\n", (int) event_1_bytes.size());
    }
    // Read the JSON for table-1.
    if (!reader.load(table_1_json))
      throw std::logic_error("failed to parse data JSON: "
                             + to_string(reader.get_error()));
    if (!reader.apply(table_1))
      throw std::logic_error("failed to parse data from the JSON: "
                             + to_string(reader.get_error()));
    printf("table_1: %s\n", to_string(table_1).c_str());
    // Serialize table-1 to the native format.
    {
      caf::binary_serializer snk{nullptr, table_1_bytes};
      if (!snk.apply(table_1))
        throw std::logic_error("failed to serialize table_1");
      printf("serialized table_1 has %d bytes\n", (int) table_1_bytes.size());
    }
  }
};

struct init_helper {
  init_helper() {
    configuration::init_global_state();
  }
};

init_helper init;

} // namespace

// -- broker::data -------------------------------------------------------------

BENCHMARK_DEFINE_F(broker_data, event_1)
(benchmark::State& state) {
  for (auto _ : state) {
    data uut;
    caf::binary_deserializer snk{nullptr, event_1_bytes};
    if (!snk.apply(uut))
      throw std::logic_error("failed to serialize event_1");
    benchmark::DoNotOptimize(uut);
  }
}

BENCHMARK_REGISTER_F(broker_data, event_1);

BENCHMARK_DEFINE_F(broker_data, table_1)
(benchmark::State& state) {
  for (auto _ : state) {
    data uut;
    caf::binary_deserializer snk{nullptr, table_1_bytes};
    if (!snk.apply(uut))
      throw std::logic_error("failed to serialize table_1");
    benchmark::DoNotOptimize(uut);
  }
}

BENCHMARK_REGISTER_F(broker_data, table_1);

// -- broker::shallow_data -----------------------------------------------------

template <class T>
using mbr_alloc = detail::monotonic_buffer_resource::allocator<T>;

class shallow_data;

using shallow_data_alloc = mbr_alloc<shallow_data>;

/// A container of sequential data.
struct shallow_vector {
  size_t size;
  shallow_data* items;
  shallow_data* begin();
  const shallow_data* begin() const;
  shallow_data* end();
  const shallow_data* end() const;
};

bool operator<(const shallow_vector& lhs, const shallow_vector& rhs) {
  return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(),
                                      rhs.end());
}

/// An associative, ordered container of unique keys.
using shallow_set =
  std::set<shallow_data, std::less<shallow_data>, shallow_data_alloc>;

/// An associative, ordered container that maps unique keys to values.
using shallow_table =
  std::map<shallow_data, shallow_data, std::less<shallow_data>,
           mbr_alloc<std::pair<const shallow_data, shallow_data>>>;

using shallow_data_variant =
  std::variant<none, boolean, count, integer, real, std::string_view, address,
               subnet, port, timestamp, timespan, enum_value, shallow_set,
               shallow_table, shallow_vector>;

class shallow_data {
public:
  using type = data::type;

  shallow_data() = default;

  /// Constructs an empty data value in `none` state.
  explicit shallow_data(none) {
    // nop
  }

  explicit shallow_data(std::string_view str) : data_(str) {
    // nop
  }

  shallow_data(const shallow_data&) = default;

  shallow_data& operator=(const shallow_data&) = default;

  template <class T, class = std::enable_if_t<std::is_arithmetic_v<T>>>
  explicit shallow_data(T x) {
    if constexpr (std::is_same_v<T, bool>)
      data_ = x;
    else if constexpr (std::is_floating_point_v<T>)
      data_ = static_cast<real>(x);
    else if constexpr (std::is_signed_v<T>)
      data_ = static_cast<integer>(x);
    else
      data_ = static_cast<count>(x);
  }

  shallow_data& operator=(none) noexcept {
    data_ = nil;
    return *this;
  }

  shallow_data& operator=(std::string_view str) noexcept {
    data_ = str;
    return *this;
  }

  shallow_data& operator=(shallow_vector vec) noexcept {
    data_ = vec;
    return *this;
  }

  shallow_data& operator=(shallow_table tbl) noexcept {
    data_ = std::move(tbl);
    return *this;
  }

  template <class T, class = std::enable_if_t<std::is_arithmetic_v<T>>>
  shallow_data& operator=(T x) {
    if constexpr (std::is_same_v<T, bool>)
      data_ = x;
    else if constexpr (std::is_floating_point_v<T>)
      data_ = static_cast<real>(x);
    else if constexpr (std::is_signed_v<T>)
      data_ = static_cast<integer>(x);
    else
      data_ = static_cast<count>(x);
    return *this;
  }

  type get_type() const noexcept {
    return static_cast<data::type>(data_.index());
  }

  [[nodiscard]] shallow_data_variant& get_data() noexcept {
    return data_;
  }

  [[nodiscard]] const shallow_data_variant& get_data() const noexcept {
    return data_;
  }

private:
  shallow_data_variant data_;
};

shallow_data* shallow_vector::begin() {
  return items;
}

const shallow_data* shallow_vector::begin() const{
  return items;
}

shallow_data* shallow_vector::end() {
  return items + size;
}
const shallow_data* shallow_vector::end() const{
  return items + size;
}

template <class Container>
void container_convert(Container& c, std::string& str, char left, char right) {
  constexpr auto* delim = ", ";
  auto first = c.begin();
  auto last = c.end();
printf("container size: %d\n",(int)std::distance(first,last));
  str += left;
  if (first != last) {
    str += to_string(*first);
    while (++first != last)
      str += delim + to_string(*first);
  }
  str += right;
}

std::string to_string(const shallow_table&) {
  return "implement-me";
}

std::string to_string(const shallow_set&) {
  return "implement-me";
}

std::string to_string(const shallow_vector& xs) {
  std::string str;
  container_convert(xs, str, '(', ')');
  return str;
}


struct shallow_data_converter {
  template <class T>
  void operator()(const T& x) {
    using std::to_string;
    str += to_string(x);
  }

  void operator()(timespan ts) {
    convert(ts.count(), str);
    str += "ns";
  }

  void operator()(timestamp ts) {
    (*this)(ts.time_since_epoch());
  }

  void operator()(bool b) {
    str = b ? 'T' : 'F';
  }

  void operator()(std::string_view x) {
    str = x;
  }

  std::string& str;
};

std::string to_string(const shallow_data& x) {
  std::string str;
  std::visit(shallow_data_converter{str}, x.get_data());
  return str;
}

bool operator<(const shallow_data& lhs, const shallow_data& rhs) {
  return lhs.get_data() < rhs.get_data();
}

class shallow_message {
public:
  shallow_message(
    shallow_data* data,
    std::shared_ptr<detail::monotonic_buffer_resource> mem) noexcept
    : data_(std::move(data)), mem_(std::move(mem)) {
    // nop
  }

  shallow_message(const shallow_message&) noexcept = default;

  shallow_message& operator=(const shallow_message&) noexcept = default;

  const shallow_data& data() const noexcept {
    return *data_;
  }

private:
  shallow_data* data_;
  std::shared_ptr<detail::monotonic_buffer_resource> mem_;
};

using maybe_shallow_data = expected<shallow_data>;

using const_byte_ptr = const std::byte*;


uint64_t rd_u64(const_byte_ptr& bytes) {
  broker::count tmp = 0;
  memcpy(&tmp, bytes, sizeof(broker::count));
  bytes += sizeof(broker::count);
  return caf::detail::from_network_order(tmp);
}

uint8_t rd_u8(const_byte_ptr& bytes) {
  auto result = *bytes++;
  return static_cast<uint8_t>(result);
}

bool rd_varbyte(const_byte_ptr& first, const_byte_ptr last, size_t& result) {
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

bool deserialize_shallow(const_byte_ptr& first, const_byte_ptr last,
                         detail::monotonic_buffer_resource* mem,
                         shallow_data* out) {
  auto type = static_cast<data::type>(*first++);
  switch (type) {
    case data::type::none:
      *out = nil;
      return true;
    case data::type::count: {
      if (first + sizeof(broker::count) > last)
        return false;
      *out = rd_u64(first);
      return true;
    }
    case data::type::integer: {
      if (first + sizeof(broker::integer) > last)
        return false;
      *out = static_cast<broker::integer>(rd_u64(first));
      return true;
    }
    case data::type::string: {
      size_t len = 0;
      if (!rd_varbyte(first, last, len))
        return false;
      if (first + len > last)
        return false;
      mbr_alloc<char> alloc{mem};
      auto* str = alloc.allocate(len);
      memcpy(str, first, len);
      *out = std::string_view{str, len};
      first += len;
      return true;
    }
    case data::type::vector: {
      size_t len = 0;
      if (!rd_varbyte(first, last, len))
        return false;
      shallow_data_alloc alloc{mem};
      auto ls = detail::new_instance<shallow_vector>(*mem);
      ls->size = len;
      ls->items = alloc.allocate(len);
      for (size_t n = 0; n < len; ++n) {
        auto* item = new (std::addressof(ls->items[n])) shallow_data();
        if (!deserialize_shallow(first, last, mem, item))
          return false;
      }
      *out = *ls;
      return true;
    }
    case data::type::table: {
      size_t len = 0;
      if (!rd_varbyte(first, last, len))
        return false;
      using tbl_alloc = shallow_table::allocator_type;
      auto tbl = detail::new_instance<shallow_table>(*mem, tbl_alloc{mem});
      for (size_t n = 0; n < len; ++n) {
        shallow_data key;
        shallow_data val;
        if (!deserialize_shallow(first, last, mem, &key)
            || !deserialize_shallow(first, last, mem, &val))
          return false;
        tbl->emplace(std::move(key), std::move(val));
      }
      *out = std::move(*tbl);
      return true;
    }
    default:
      return false;
  };
}

expected<shallow_message> deserialize_shallow(const std::byte* first,
                                              const std::byte* last) {
  if (first == last)
    auto err = make_error(ec::deserialization_failed, "no data to deserialize");
  auto mem = std::make_shared<detail::monotonic_buffer_resource>();
  shallow_data_alloc alloc{mem.get()};
  auto result = new (alloc.allocate(1)) shallow_data();
  if (deserialize_shallow(first, last, mem.get(), result) && first == last) {
    shallow_message msg{result, mem};
    return expected<shallow_message>{std::move(msg)};
  }
  auto err = make_error(ec::deserialization_failed, "invalid format");
  return expected<shallow_message>{std::move(err)};
}

BENCHMARK_DEFINE_F(broker_data, shallow_event_1)
(benchmark::State& state) {
  for (auto _ : state) {
    auto first = reinterpret_cast<std::byte*>(event_1_bytes.data());
    auto res = deserialize_shallow(first, first + event_1_bytes.size());
    if (!res)
      throw std::logic_error("failed to deserialize event_1");
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK_REGISTER_F(broker_data, shallow_event_1);

BENCHMARK_DEFINE_F(broker_data, shallow_table_1)
(benchmark::State& state) {
  for (auto _ : state) {
    auto first = reinterpret_cast<std::byte*>(table_1_bytes.data());
    auto res = deserialize_shallow(first, first + table_1_bytes.size());
    if (!res)
      throw std::logic_error("failed to deserialize table_1");
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK_REGISTER_F(broker_data, shallow_table_1);
