#include "main.hh"

#include "broker/alm/multipath.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"

#include <benchmark/benchmark.h>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include <atomic>
#include <limits>
#include <random>

using namespace broker;

namespace {

using buffer_type = caf::binary_serializer::container_type;

size_t max_size(size_t init) {
  return init;
}

template <class T, class... Ts>
size_t max_size(size_t init, const T& x, const Ts&... xs) {
  auto combinator = [](size_t init, const buffer_type& buf) {
    return std::max(init, buf.size());
  };
  return max_size(std::accumulate(x.begin(), x.end(), init, combinator), xs...);
}

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
      to_bytes(dmsg[index], dmsg_buf[index]);
      nmsg[index] = make_node_message(dmsg[index], alm::multipath{dst});
      to_bytes(nmsg[index], nmsg_buf[index]);
      legacy_nmsg[index] = legacy_node_message{dmsg[index], 20};
      to_bytes(legacy_nmsg[index], legacy_nmsg_buf[index]);
    }
    sink_buf.reserve(max_size(0, dmsg_buf, nmsg_buf, legacy_nmsg_buf));
  }

  template <class T>
  void to_bytes(T&& what, buffer_type& storage) {
    caf::binary_serializer sink{nullptr, storage};
    std::ignore = sink.apply(what);
  }

  template <class T>
  void from_bytes(const buffer_type& storage, T& what) {
    caf::binary_deserializer source{nullptr, storage};
    std::ignore = source.apply(what);
  }

  // Dummy node ID for a receiver.
  endpoint_id dst;

  // One data message per type.
  array_t<data_message> dmsg;

  // Serialized versions of dmsg;
  array_t<buffer_type> dmsg_buf;

  // One node message per type.
  array_t<node_message> nmsg;

  // Serialized versions of dmsg;
  array_t<buffer_type> nmsg_buf;

  // One legacy node message per type.
  array_t<legacy_node_message> legacy_nmsg;

  // Serialized versions of legacy_dmsg;
  array_t<buffer_type> legacy_nmsg_buf;

  // A pre-allocated buffer for the benchmarks to serialize into.
  buffer_type sink_buf;

  template <class T>
  auto& get_msg(int signed_index) {
    auto index = static_cast<size_t>(signed_index);
    if constexpr (std::is_same_v<T, data_message>) {
      return dmsg[index];
    } else if constexpr (std::is_same_v<T, node_message>) {
      return nmsg[index];
    } else {
      static_assert(std::is_same_v<T, legacy_node_message>);
      return legacy_nmsg[index];
    }
  }

  template <class T>
  const buffer_type& get_buf(int signed_index) const {
    auto index = static_cast<size_t>(signed_index);
    if constexpr (std::is_same_v<T, data_message>) {
      return dmsg_buf[index];
    } else if constexpr (std::is_same_v<T, node_message>) {
      return nmsg_buf[index];
    } else {
      static_assert(std::is_same_v<T, legacy_node_message>);
      return legacy_nmsg_buf[index];
    }
  }

  template <class T>
  void run_serialization_bench(benchmark::State& state) {
    const auto& msg = get_msg<T>(state.range(0));
    caf::binary_serializer sink{nullptr, sink_buf};
    for (auto _ : state) {
      sink.seek(0);
      std::ignore = sink.apply(msg);
      benchmark::DoNotOptimize(sink_buf);
    }
  }

  template <class T>
  void run_deserialization_bench(benchmark::State& state) {
    const auto& buf = get_buf<T>(state.range(0));
    for (auto _ : state) {
      T msg;
      caf::binary_deserializer source{nullptr, buf};
      std::ignore = source.apply(msg);
      benchmark::DoNotOptimize(msg);
    }
  }
};

} // namespace

// -- saving and loading data messages -----------------------------------------

BENCHMARK_DEFINE_F(serialization, save_data_message)(benchmark::State& state) {
  run_serialization_bench<data_message>(state);
}

BENCHMARK_REGISTER_F(serialization, save_data_message)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_data_message)(benchmark::State& state) {
  run_deserialization_bench<data_message>(state);
}

BENCHMARK_REGISTER_F(serialization, load_data_message)->DenseRange(0, 2, 1);

// -- saving and loading node messages -----------------------------------------

BENCHMARK_DEFINE_F(serialization, save_node_message)(benchmark::State& state) {
  run_serialization_bench<node_message>(state);
}

BENCHMARK_REGISTER_F(serialization, save_node_message)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_node_message)(benchmark::State& state) {
  run_deserialization_bench<node_message>(state);
}

BENCHMARK_REGISTER_F(serialization, load_node_message)->DenseRange(0, 2, 1);

// -- saving and loading legacy node messages ----------------------------------

BENCHMARK_DEFINE_F(serialization, save_legacy_node_message)
(benchmark::State& state) {
  run_serialization_bench<legacy_node_message>(state);
}

BENCHMARK_REGISTER_F(serialization, save_legacy_node_message)
  ->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_legacy_node_message)
(benchmark::State& state) {
  run_deserialization_bench<legacy_node_message>(state);
}

BENCHMARK_REGISTER_F(serialization, load_legacy_node_message)
  ->DenseRange(0, 2, 1);
