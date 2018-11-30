#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <caf/atom.hpp>
#include <caf/downstream.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/config_option_adder.hpp>

#include "broker/configuration.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/topic.hh"

using std::cout;
using std::cerr;
using std::endl;

using namespace broker;

namespace {

using sink_atom = caf::atom_constant<caf::atom("sink")>;
using source_atom = caf::atom_constant<caf::atom("source")>;

class config : public configuration {
public:
  uint16_t port = 0;
  std::string host = "localhost";
  caf::atom_value mode;

  config() {
    opt_group{custom_options_, "global"}
    .add(mode, "mode,m", "one of 'sink' or 'source'")
    .add(port, "port,p", "sets the port for listening or peering")
    .add(host, "host,o", "sets the peering with the sink");
  }
};

std::atomic<bool> stop_rate_calculator;
std::atomic<size_t> global_count;

void subscribe_mode(broker::endpoint& ep, const std::string& topic_str) {
  using namespace caf;
  auto worker = ep.subscribe(
    {topic_str},
    [](caf::unit_t&) {
      // nop
    },
    [=](caf::unit_t&, std::vector<data_message>& xs) {
      global_count += xs.size();
    },
    [=](caf::unit_t&, const caf::error&) {
      // nop
    }
  );
  scoped_actor self{ep.system()};
  self->wait_for(worker);
}

void publish_mode(broker::endpoint& ep, const std::string& topic_str) {
  using namespace caf;
  auto worker = ep.publish_all(
    [](caf::unit_t&) {
      // nop
    },
    [=](caf::unit_t&, downstream<data_message>& out, size_t num) {
      for (size_t i = 0; i < num; ++i)
        out.push(make_data_message(topic_str, "Lorem ipsum dolor sit amet."));
      global_count += num;
    },
    [=](const caf::unit_t&) {
      return false;
    }
  );
  scoped_actor self{ep.system()};
  self->wait_for(worker);
}

void rate_calculator() {
  using namespace std::chrono;
  auto t = system_clock::now();
  t += seconds(1);
  while (!stop_rate_calculator.load()) {
    std::this_thread::sleep_until(t);
    auto cnt = global_count.load();
    while (!global_count.compare_exchange_strong(cnt, 0)) {
      // repeat
    }
    std::cout << cnt << " messages/s" << std::endl;
    t += seconds(1);
  }
}

} // namespace <anonymous>


int main(int argc, char** argv) {
  config cfg;
  cfg.parse(argc, argv);
  auto mode = cfg.mode;
  auto port = cfg.port;
  auto host = cfg.host;
  broker::endpoint ep{std::move(cfg)};
  std::string topic{"foobar"};
  std::thread t{rate_calculator};
  auto g = caf::detail::make_scope_guard([&] {
    std::cout << "*** stop rate calculator";
    stop_rate_calculator = true;
    t.join();
  });
  switch (static_cast<uint64_t>(mode)) {
    default:
      std::cerr << "invalid mode: " << to_string(mode) << endl;
      return EXIT_FAILURE;
    case source_atom::uint_value():
      if (!ep.peer(host, port)) {
        std::cerr << "cannot peer to node: " << to_string(host)
                  << " on port " << port << endl;
        return EXIT_FAILURE;
      }
      publish_mode(ep, topic);
      break;
    case sink_atom::uint_value(): {
      ep.listen({}, port);
      subscribe_mode(ep, topic);
      break;
    }
  }
  return EXIT_SUCCESS;
}
