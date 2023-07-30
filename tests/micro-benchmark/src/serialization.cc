#include "broker/builder.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/variant.hh"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>

#include <benchmark/benchmark.h>

using namespace broker;
using namespace std::literals;

namespace {

// Usually, the envelope owns the buffer. Since the buffer is owned by the
// fixture in this benchmark, we use a shallow envelope to avoid copying that
// would not happen in real code either.
class shallow_envelope : public data_envelope {
public:
  shallow_envelope(const std::byte* data, size_t size)
    : data_(data), size_(size) {
    // nop
  }

  uint16_t ttl() const noexcept override {
    return defaults::ttl;
  }

  variant value() noexcept override {
    return {root_, {new_ref, this}};
  }

  std::string_view topic() const noexcept override {
    return {};
  }

  bool is_root(const variant_data* val) const noexcept override {
    return val == root_;
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {data_, size_};
  }

  error parse() {
    error result;
    root_ = do_parse(buf_, result);
    return result;
  }

private:
  variant_data* root_ = nullptr;
  const std::byte* data_;
  size_t size_;
  detail::monotonic_buffer_resource buf_;
};

void write(std::pair<const std::byte*, size_t> range, caf::byte_buffer& buf) {
  const auto* first = reinterpret_cast<const caf::byte*>(range.first);
  buf.insert(buf.end(), first, first + range.second);
}

class serialization : public benchmark::Fixture {
public:
  broker::data event_1;

  broker::data table_1;

  serialization() {
    event_1 = data{vector{1, 1, vector{"event_1", vector{42, "test"}}}};
    table_1 = data{table{{"first-name", "John"}, {"last-name", "Doe"}}};
  }
};

class deserialization : public benchmark::Fixture {
public:
  static constexpr size_t num_message_types = 3;

  /// Event 1: [1u, 1u, ["event_1", [42, "test"]]]
  caf::byte_buffer event_1_bytes;

  /// Table 1: {"first-name" -> "John", "last-name" -> "Doe"}
  caf::byte_buffer table_1_bytes;

  deserialization() {
    // Event 1.
    list_builder lst;
    lst.add(1u).add(1u).add_list("event_1"sv, std::tuple{42, "test"sv});
    write(lst.bytes(), event_1_bytes);
    // Table 1.
    table_builder tbl;
    tbl.add("first-name"sv, "John"sv).add("last-name"sv, "Doe"sv);
    write(tbl.bytes(), table_1_bytes);
  }

  auto event_1_data() {
    return std::pair{reinterpret_cast<const std::byte*>(event_1_bytes.data()),
                     event_1_bytes.size()};
  }

  auto table_1_data() {
    return std::pair{reinterpret_cast<const std::byte*>(table_1_bytes.data()),
                     table_1_bytes.size()};
  }
};

} // namespace

// -- serialization: broker::data ----------------------------------------------

BENCHMARK_DEFINE_F(serialization, event_1_data)(benchmark::State& state) {
  for (auto _ : state) {
    caf::byte_buffer buf;
    buf.reserve(512);
    caf::binary_serializer snk{nullptr, buf};
    if (!snk.apply(event_1))
      throw std::logic_error("failed to serialize event_1");
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(serialization, event_1_data);

BENCHMARK_DEFINE_F(serialization, table_1_data)
(benchmark::State& state) {
  for (auto _ : state) {
    caf::byte_buffer buf;
    buf.reserve(512);
    caf::binary_serializer snk{nullptr, buf};
    if (!snk.apply(table_1))
      throw std::logic_error("failed to serialize table_1");
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(serialization, table_1_data);

// -- serialization: broker::builder -------------------------------------------

BENCHMARK_DEFINE_F(serialization, event_1_builder)(benchmark::State& state) {
  for (auto _ : state) {
    list_builder builder;
    auto res = builder.add(1u)
                 .add(1u)
                 .add_list("event_1"sv, std::tuple{42, "test"sv})
                 .bytes();
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK_REGISTER_F(serialization, event_1_builder);

BENCHMARK_DEFINE_F(serialization, table_1_builder)
(benchmark::State& state) {
  for (auto _ : state) {
    table_builder builder;
    auto res = builder.add("first-name"sv, "John"sv)
                 .add("last-name"sv, "Doe"sv) //
                 .bytes();
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK_REGISTER_F(serialization, table_1_builder);

// -- deserialization: broker::data --------------------------------------------

BENCHMARK_DEFINE_F(deserialization, event_1_data)(benchmark::State& state) {
  for (auto _ : state) {
    data uut;
    caf::binary_deserializer src{nullptr, event_1_bytes};
    if (!src.apply(uut))
      throw std::logic_error("failed to deserialize event_1");
    benchmark::DoNotOptimize(uut);
  }
}

BENCHMARK_REGISTER_F(deserialization, event_1_data);

BENCHMARK_DEFINE_F(deserialization, table_1_data)
(benchmark::State& state) {
  for (auto _ : state) {
    data uut;
    caf::binary_deserializer src{nullptr, table_1_bytes};
    if (!src.apply(uut))
      throw std::logic_error("failed to deserialize table_1");
    benchmark::DoNotOptimize(uut);
  }
}

BENCHMARK_REGISTER_F(deserialization, table_1_data);

// -- deserialization: broker::variant -----------------------------------------

BENCHMARK_DEFINE_F(deserialization, event_1_variant)
(benchmark::State& state) {
  for (auto _ : state) {
    detail::monotonic_buffer_resource buf;
    variant_data uut;
    auto [bytes, size] = event_1_data();
    auto [ok, pos] = uut.parse_shallow(buf, bytes, size);
    if (!ok)
      throw std::logic_error("failed to deserialize event_1");
    benchmark::DoNotOptimize(uut);
  }
}

BENCHMARK_REGISTER_F(deserialization, event_1_variant);

BENCHMARK_DEFINE_F(deserialization, table_1_variant)
(benchmark::State& state) {
  for (auto _ : state) {
    detail::monotonic_buffer_resource buf;
    variant_data uut;
    auto [bytes, size] = table_1_data();
    auto [ok, pos] = uut.parse_shallow(buf, bytes, size);
    if (!ok)
      throw std::logic_error("failed to deserialize table_1");
    benchmark::DoNotOptimize(uut);
  }
}

BENCHMARK_REGISTER_F(deserialization, table_1_variant);

// -- deserialization: broker::envelope ----------------------------------------

BENCHMARK_DEFINE_F(deserialization, event_1_envelope)
(benchmark::State& state) {
  for (auto _ : state) {
    auto [bytes, size] = event_1_data();
    auto env = std::make_shared<shallow_envelope>(bytes, size);
    if (auto err = env->parse())
      throw std::logic_error("failed to deserialize event_1");
    auto val = env->value();
    benchmark::DoNotOptimize(val);
  }
}

BENCHMARK_REGISTER_F(deserialization, event_1_envelope);

BENCHMARK_DEFINE_F(deserialization, table_1_envelope)
(benchmark::State& state) {
  for (auto _ : state) {
    auto [bytes, size] = table_1_data();
    auto env = std::make_shared<shallow_envelope>(bytes, size);
    if (auto err = env->parse())
      throw std::logic_error("failed to deserialize table_1");
    auto val = env->value();
    benchmark::DoNotOptimize(val);
  }
}

BENCHMARK_REGISTER_F(deserialization, table_1_envelope);
