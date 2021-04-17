#include "main.hh"

#include "broker/alm/multipath.hh"
#include "broker/endpoint.hh"
#include "broker/message.hh"

#include <benchmark/benchmark.h>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include <atomic>
#include <limits>
#include <random>

using namespace broker;

using namespace std::literals;

namespace {

struct vector_builder {
  vector* vec;
};

template <class T>
vector_builder&& operator<<(vector_builder&& builder, T&& value) {
  builder.vec->emplace_back(std::forward<T>(value));
  return std::move(builder);
}

template <class T>
vector_builder& operator<<(vector_builder& builder, T&& value) {
  builder.vec->emplace_back(std::forward<T>(value));
  return builder;
}

auto add_to(vector& vec) {
  return vector_builder{&vec};
}

timestamp brokergenesis() {
  // Broker started its life on Jul 9, 2014, 5:16 PM GMT+2 with the first commit
  // by Jon Siwek. This function returns a UNIX timestamp for that time.
  return clock::from_time_t(1404918960);
}

class generator {
public:
  generator() : rng_(0xB7E57), ts_(brokergenesis()) {
    // nop
  }

  endpoint_id next_endpoint_id() {
    using array_type = caf::hashed_node_id::host_id_type;
    using value_type = array_type::value_type;
    std::uniform_int_distribution<> d{0,
                                      std::numeric_limits<value_type>::max()};
    array_type result;
    for (auto& x : result)
      x = static_cast<value_type>(d(rng_));
    return caf::make_node_id(d(rng_), result);
  }

  count next_count() {
    std::uniform_int_distribution<count> d;
    return d(rng_);
  }

  std::string next_string(size_t length) {
    std::string_view charset
      = "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<size_t> d{0, charset.size() - 1};
    std::string result;
    result.resize(length);
    for (auto& c : result)
      c = charset[d(rng_)];
    return result;
  }

  timestamp next_timestamp() {
    std::uniform_int_distribution<size_t> d{1, 100};
    ts_ += std::chrono::seconds(d(rng_));
    return ts_;
  }

  // Generates events for one of three possible types:
  // 1. Trivial data consisting of a number and a string.
  // 2. More complex data that resembles a line in a conn.log.
  // 3. Large tables of size 100 by 10, filled with random strings.
  data next_data(size_t event_type) {
    vector result;
    switch (event_type) {
      case 1: {
        add_to(result) << 42 << "test"s;
        break;
      }
      case 2: {
         address a1;
         address a2;
         convert("1.2.3.4", a1);
         convert("3.4.5.6", a2);
         add_to(result) << next_timestamp() << next_string(10)
                        << vector{a1, port(4567, port::protocol::tcp), a2,
                                  port(80, port::protocol::tcp)}
                        << enum_value("tcp") << next_string(10)
                        << std::chrono::duration_cast<timespan>(3140ms)
                        << next_count() << next_count() << next_string(5)
                        << true << false << next_count() << next_string(10)
                        << next_count() << next_count() << next_count()
                        << next_count()
                        << set({next_string(10), next_string(10)});
        break;
      }
      case 3: {
        table m;
        for (int i = 0; i < 100; ++i) {
          set s;
          for (int j = 0; j < 10; ++j)
            s.insert(next_string(5));
          m[next_string(15)] = std::move(s);
        }
        add_to(result) << next_timestamp() << std::move(m);
        break;
      }
      default: {
        std::cerr << "event type must be 1, 2, or 3; got " << event_type
                  << '\n';
        throw std::logic_error("invalid event type");
      }
    }
    return data{std::move(result)};
  }

private:
  std::minstd_rand rng_;
  timestamp ts_;
};

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
      nmsg[index] = make_node_message(dmsg[index], alm::multipath{dst}, {dst});
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
};

} // namespace

// -- saving and loading data messages -----------------------------------------

BENCHMARK_DEFINE_F(serialization, save_data_message)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& msg = dmsg[index];
  for (auto _ : state) {
    sink_buf.clear();
    to_bytes(msg, sink_buf);
    benchmark::DoNotOptimize(sink_buf);
  }
}

BENCHMARK_REGISTER_F(serialization, save_data_message)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_data_message)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& buf = dmsg_buf[index];
  for (auto _ : state) {
    data_message msg;
    from_bytes(buf, msg);
    benchmark::DoNotOptimize(msg);
  }
}

BENCHMARK_REGISTER_F(serialization, load_data_message)->DenseRange(0, 2, 1);

// -- saving and loading node messages -----------------------------------------

BENCHMARK_DEFINE_F(serialization, save_node_message)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& msg = nmsg[index];
  for (auto _ : state) {
    sink_buf.clear();
    to_bytes(msg, sink_buf);
    benchmark::DoNotOptimize(sink_buf);
  }
}

BENCHMARK_REGISTER_F(serialization, save_node_message)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_node_message)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& buf = nmsg_buf[index];
  for (auto _ : state) {
    node_message msg;
    from_bytes(buf, msg);
    benchmark::DoNotOptimize(msg);
  }
}

BENCHMARK_REGISTER_F(serialization, load_node_message)->DenseRange(0, 2, 1);

// -- saving and loading legacy node messages ----------------------------------

BENCHMARK_DEFINE_F(serialization, save_legacy_node_message)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& msg = legacy_nmsg[index];
  for (auto _ : state) {
    sink_buf.clear();
    to_bytes(msg, sink_buf);
    benchmark::DoNotOptimize(sink_buf);
  }
}

BENCHMARK_REGISTER_F(serialization, save_legacy_node_message)->DenseRange(0, 2, 1);

BENCHMARK_DEFINE_F(serialization, load_legacy_node_message)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& buf = legacy_nmsg_buf[index];
  for (auto _ : state) {
    legacy_node_message msg;
    from_bytes(buf, msg);
    benchmark::DoNotOptimize(msg);
  }
}

BENCHMARK_REGISTER_F(serialization, load_legacy_node_message)->DenseRange(0, 2, 1);
