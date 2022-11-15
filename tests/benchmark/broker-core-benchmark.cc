#include "broker/endpoint.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/type_id.hh"

#include <caf/async/blocking_consumer.hpp>
#include <caf/async/blocking_producer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/send.hpp>

#include <cstdint>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include <poll.h>

using namespace broker;
using namespace std::literals;

namespace {

int64_t producer_sleep_probability = 0;

int64_t producer_min_sleep_us = 100;

int64_t producer_max_sleep_us = 5'000;

int64_t producer_count = 16;

int64_t consumer_sleep_probability = 0;

int64_t consumer_min_sleep_us = 1'000'000;

int64_t consumer_max_sleep_us = 1'200'000;

int64_t consumer_count = 1;

std::atomic<size_t> consumed;

} // namespace

void producer_impl(caf::actor core, unsigned seed) {
  std::minstd_rand engine{seed};
  std::uniform_int_distribution<int64_t> dice{1, 10'000};
  std::uniform_int_distribution<int64_t> us_dis{producer_min_sleep_us,
                                                producer_max_sleep_us};
  using caf::async::make_spsc_buffer_resource;
  auto [con1, prod1] = make_spsc_buffer_resource<node_message>();
  auto [con2, prod2] = make_spsc_buffer_resource<node_message>();
  auto id = endpoint_id::random();
  auto filter = filter_type{"/producer/foo/bar"};
  caf::anon_send(core, internal::atom::peer_v, id,
                 network_info{to_string(id), 42}, filter, con1, prod2);
  auto out = caf::async::make_blocking_producer(prod1);
  if (!out) {
    std::cerr << "*** failed to create producer\n";
    ::abort();
  }
  auto in = caf::async::make_blocking_consumer(con2);
  int64_t n = 1;
  auto out_topic = topic{"/my/data"};
  auto buf = caf::byte_buffer{};
  auto pack = [&](const auto& msg) {
    using msg_t = std::decay_t<decltype(msg)>;
    buf.clear();
    caf::binary_serializer snk{nullptr, buf};
    if constexpr (std::is_same_v<msg_t, data_message>) {
      std::ignore = snk.apply(get_data(msg));
    } else {
      static_assert(std::is_same_v<msg_t, command_message>);
      std::ignore = snk.apply(get_command(msg));
    }
    return make_packed_message(packed_message_type_v<msg_t>, 32, get_topic(msg),
                               buf);
  };

  for (;;) {
    auto msg = make_node_message(id, pack(make_data_message(out_topic, n)));
    out->push(msg);
    ++n;
    auto roll = dice(engine);
    if (roll == 1) {
      // Insta-death!
      return;
    }
    if (roll <= (producer_sleep_probability * 100)) {
      auto us = us_dis(engine);
      std::this_thread::sleep_for(std::chrono::microseconds{us});
    }
  }
}

void producer(caf::actor core, unsigned seed) {
  for (;;) {
    auto hdl = std::thread{producer_impl, core, seed};
    hdl.join();
    std::this_thread::sleep_for(23ms);
    ++seed;
  }
}

void consumer(endpoint* ep) {
  std::random_device device;
  std::minstd_rand engine{device()};
  std::uniform_int_distribution<int64_t> dice{1, 10'000};
  std::uniform_int_distribution<int64_t> us_dis{consumer_min_sleep_us,
                                                consumer_max_sleep_us};
  auto sub = ep->make_subscriber(filter_type{"/my/data"});
  pollfd pfd;
  pfd.fd = sub.fd();
  pfd.events = POLLIN;
  for (;;) {
    auto ready = poll(&pfd, 1, -1);
    if (ready == -1) {
      std::cerr << "POLL FAILED!\n";
      ::abort();
    }
    auto xs = sub.poll();
    consumed += xs.size();
    auto roll = dice(engine);
    if (roll <= (consumer_sleep_probability * 100)) {
      auto us = us_dis(engine);
      std::this_thread::sleep_for(std::chrono::microseconds{us});
    }
  }
}

void add_options(configuration& cfg) {
  cfg.add_option(&producer_sleep_probability, "producer-sleep-probability", "");
  cfg.add_option(&producer_min_sleep_us, "producer-min-sleep-us", "");
  cfg.add_option(&producer_max_sleep_us, "producer-max-sleep-us", "");
  cfg.add_option(&producer_count, "producer-count", "");
  cfg.add_option(&consumer_sleep_probability, "consumer-sleep-probability", "");
  cfg.add_option(&consumer_min_sleep_us, "consumer-min-sleep-us", "");
  cfg.add_option(&consumer_max_sleep_us, "consumer-max-sleep-us", "");
  cfg.add_option(&consumer_count, "consumer-count", "");
}

// void slow_start_producers(caf::actor core) {
//     std::this_thread::sleep_for(500ms);
//   std::vector<std::thread> threads;
//   for (int64_t i = 0; i < producer_count; ++i) {
//     std::cout << "start producer #" << (i + 1) << "\n";
//     threads.push_back(std::thread{producer, core});
//   }
//   // Wait for it!
//   for (auto& hdl : threads)
//     hdl.join();
// }

int main(int argc, char** argv) {
  // Parse config.
  endpoint::system_guard sys_guard;
  configuration cfg{skip_init};
  add_options(cfg);
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << ex.what() << "\n\n";
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed())
    return EXIT_SUCCESS;
  // Spin up our endpoint.
  std::vector<std::thread> threads;
  endpoint ep(std::move(cfg));
  auto ss = ep.make_status_subscriber(true);
  auto* sys = std::addressof(internal::endpoint_access{&ep}.sys());
  auto core = internal::native(ep.core());
  // Spin up our producers.
  std::random_device rng_dev;
  for (int64_t i = 0; i < producer_count; ++i) {
    std::cout << "start producer #" << (i + 1) << "\n";
    threads.push_back(std::thread{producer, core, rng_dev()});
  }
  //threads.push_back(std::thread{slow_start_producers, core});

  // Spin up our consumers.
  for (int64_t i = 0; i < consumer_count; ++i) {
    std::cout << "start consumer #" << (i + 1) << "\n";
    threads.push_back(std::thread{consumer, &ep});
  }
  auto tout = std::chrono::steady_clock::now() + 1s;
  auto last_count = size_t{0};
  for (;;) {
    std::this_thread::sleep_until(tout);
    tout += 1s;
    for (auto&& ev : ss.poll())
      std::visit([](auto& x) { std::cout << to_string(x) << std::endl; }, ev);
    auto cnt = consumed.load();
    std::cout << "consumed: " << (cnt - last_count) << std::endl;
    last_count = cnt;
  }
  // Wait for it!
  for (auto& hdl : threads)
    hdl.join();
}
