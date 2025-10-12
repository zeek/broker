#include "generator.hh"

#include "broker/alm/multipath.hh"
#include "broker/endpoint.hh"
#include "broker/format/json.hh"
#include "broker/fwd.hh"
#include "broker/internal/wire_format.hh"
#include "broker/message.hh"

#include <benchmark/benchmark.h>

#include <atomic>
#include <limits>
#include <random>

using namespace broker;
using namespace std::literals;

namespace {

using std::byte_buffer;
using char_buffer = std::vector<char>;

class serialization : public benchmark::Fixture {
public:
  static constexpr size_t num_message_types = 3;

  template <class T>
  using array_t = std::array<T, num_message_types>;

  serialization() {
    generator g;
    dst = g.next_endpoint_id();
    for (size_t index = 0; index < num_message_types; ++index) {
      dmsg[index] = make_data_message("/micro/benchmark",
                                      g.next_data(index + 1));
      bin_buf.clear();
      to_bytes(dmsg[index], bin_buf);
      dmsg_bin[index] = bin_buf;
      json_buf.clear();
      to_json(dmsg[index], json_buf);
      dmsg_json[index] = json_buf;
    }
  }

  void to_bytes(const data_envelope_ptr& msg, byte_buffer& buf) {
    broker::internal::wire_format::v1::trait trait;
    if (!trait.convert(msg, buf))
      throw std::logic_error("serialization failed");
  }

  void from_bytes(const byte_buffer& buf, envelope_ptr& msg) {
    broker::internal::wire_format::v1::trait trait;
    if (!trait.convert(buf, msg))
      throw std::logic_error("deserialization failed");
  }

  void to_json(const data_envelope_ptr& msg, char_buffer& buf) {
    broker::format::json::v1::encode(msg, std::back_inserter(buf));
  }

  void from_json(const char_buffer& buf, envelope_ptr& msg) {
    auto res = envelope::deserialize_json(buf.data(), buf.size());
    if (!res)
      throw std::logic_error("deserialization failed");
    msg = std::move(*res);
  }

  // Dummy node ID for a receiver.
  endpoint_id dst;

  // One data message per type.
  array_t<data_message> dmsg;

  // Serialized versions of dmsg;
  array_t<byte_buffer> dmsg_bin;

  // Serialized versions of dmsg;
  array_t<char_buffer> dmsg_json;

  // A pre-allocated buffer for the benchmarks to serialize into.
  byte_buffer bin_buf;

  // A pre-allocated buffer for the benchmarks to serialize into.
  char_buffer json_buf;
};

} // namespace

// -- saving and loading data messages -----------------------------------------

BENCHMARK_DEFINE_F(serialization, save_binary)(benchmark::State& state) {
  const auto& msg = dmsg[static_cast<size_t>(state.range(0))];
  for (auto _ : state) {
    bin_buf.clear();
    to_bytes(msg, bin_buf);
    benchmark::DoNotOptimize(bin_buf);
  }
}

BENCHMARK_REGISTER_F(serialization, save_binary)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_binary)(benchmark::State& state) {
  const auto& buf = dmsg_bin[static_cast<size_t>(state.range(0))];
  for (auto _ : state) {
    broker::envelope_ptr msg;
    from_bytes(buf, msg);
    benchmark::DoNotOptimize(msg);
  }
}

BENCHMARK_REGISTER_F(serialization, load_binary)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, save_json)(benchmark::State& state) {
  const auto& msg = dmsg[static_cast<size_t>(state.range(0))];
  for (auto _ : state) {
    json_buf.clear();
    to_json(msg, json_buf);
    benchmark::DoNotOptimize(json_buf);
  }
}

BENCHMARK_REGISTER_F(serialization, save_json)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_json)(benchmark::State& state) {
  const auto& buf = dmsg_json[static_cast<size_t>(state.range(0))];
  for (auto _ : state) {
    broker::envelope_ptr msg;
    from_json(buf, msg);
    benchmark::DoNotOptimize(msg);
  }
}

BENCHMARK_REGISTER_F(serialization, load_json)->DenseRange(0, 2, 1);
