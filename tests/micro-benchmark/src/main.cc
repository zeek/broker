#include "main.hh"

#include "broker/configuration.hh"
#include "broker/data.hh"

#include <benchmark/benchmark.h>

#include <caf/init_global_meta_objects.hpp>

#include <cstdlib>

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

} // namespace

generator::generator() : rng_(0xB7E57), ts_(brokergenesis()) {
  // nop
}

endpoint_id generator::next_endpoint_id() {
  using array_type = caf::hashed_node_id::host_id_type;
  using value_type = array_type::value_type;
  std::uniform_int_distribution<> d{0, std::numeric_limits<value_type>::max()};
  array_type result;
  for (auto& x : result)
    x = static_cast<value_type>(d(rng_));
  return caf::make_node_id(d(rng_), result);
}

count generator::next_count() {
  std::uniform_int_distribution<count> d;
  return d(rng_);
}

std::string generator::next_string(size_t length) {
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

timestamp generator::next_timestamp() {
  std::uniform_int_distribution<size_t> d{1, 100};
  ts_ += std::chrono::seconds(d(rng_));
  return ts_;
}

data generator::next_data(size_t event_type) {
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
                     << next_count() << next_count() << next_string(5) << true
                     << false << next_count() << next_string(10) << next_count()
                     << next_count() << next_count() << next_count()
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
      std::cerr << "event type must be 1, 2, or 3; got " << event_type << '\n';
      throw std::logic_error("invalid event type");
    }
  }
  return data{std::move(result)};
}

int main(int argc, char** argv) {
  caf::init_global_meta_objects<caf::id_block::micro_benchmarks>();
  configuration::init_global_state();
  run_streaming_benchmark();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return EXIT_FAILURE;
  } else {
    benchmark::RunSpecifiedBenchmarks();
    return EXIT_SUCCESS;
  }
}
