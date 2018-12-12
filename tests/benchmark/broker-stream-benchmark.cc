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

class config : public configuration {
public:
  uint16_t port = 0;
  std::string host = "localhost";
  caf::atom_value mode;

  config() {
    opt_group{custom_options_, "global"}
    .add(mode, "mode,m", "one of 'sink', 'source', or 'both'")
    .add(port, "port,p", "sets the port for listening or peering")
    .add(host, "host,o", "sets the peering with the sink");
  }
};

std::atomic<size_t> global_count;

void sink_mode(broker::endpoint& ep, const std::string& topic_str) {
  using namespace caf;
  auto worker = ep.subscribe(
    {topic_str},
    [](caf::unit_t&) {
      // nop
    },
    [=](caf::unit_t&, std::vector<std::pair<topic, data>>& xs) {
      global_count += xs.size();
    },
    [=](caf::unit_t&, const caf::error&) {
      // nop
    }
  );
  scoped_actor self{ep.system()};
  self->wait_for(worker);
}

void source_mode(broker::endpoint& ep, const std::string& topic_str) {
  using namespace caf;
  auto msg = std::make_pair(topic_str, data{"Lorem ipsum dolor sit amet."});
  auto worker = ep.publish_all(
    [](caf::unit_t&) {
      // nop
    },
    [=](caf::unit_t&, downstream<std::pair<topic, data>>& out, size_t num) {
      for (size_t i = 0; i < num; ++i)
        out.push(msg);
    },
    [=](const caf::unit_t&) {
      return false;
    }
  );
  scoped_actor self{ep.system()};
  self->wait_for(worker);
}

void rate_calculator() {
  // Counts consecutive rates that are 0.
  size_t zero_rates = 0;
  // Keeps track of the message count in our last iteration.
  size_t last_count = 0;
  // Used to compute absolute timeouts.
  auto t = std::chrono::steady_clock::now();
  // Stop after 2s of no activity.
  while (zero_rates < 2) {
    t += std::chrono::seconds(1);
    std::this_thread::sleep_until(t);
    auto count = global_count.load();
    auto rate = count - last_count;
    std::cout << rate << " msgs/s\n";
    last_count = count;
    if (rate == 0)
      ++zero_rates;
  }
}

} // namespace <anonymous>


int main(int argc, char** argv) {
  config cfg;
  cfg.parse(argc, argv);
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  auto mode = cfg.mode;
  auto port = cfg.port;
  auto host = cfg.host;
  std::string topic{"foobar"};
  std::thread t{rate_calculator};
  switch (caf::atom_uint(caf::to_lowercase(mode))) {
    default:
      std::cerr << "invalid mode: " << to_string(mode) << endl;
      return EXIT_FAILURE;
    case caf::atom_uint("source"): {
      broker::endpoint ep{std::move(cfg)};
      if (!ep.peer(host, port)) {
        std::cerr << "cannot peer to node: " << to_string(host) << " on port "
                  << port << endl;
        return EXIT_FAILURE;
      }
      source_mode(ep, topic);
      break;
    }
    case caf::atom_uint("sink"): {
      broker::endpoint ep{std::move(cfg)};
      ep.listen({}, port);
      sink_mode(ep, topic);
      break;
    }
    case caf::atom_uint("both"): {
      broker::endpoint ep1{std::move(cfg)};
      auto snk_port = ep1.listen({}, 0);
      std::thread source_thread{[argc, argv, host, snk_port, topic] {
        config cfg2;
        cfg2.parse(argc, argv);
        broker::endpoint ep2{std::move(cfg2)};
        if (!ep2.peer(host, snk_port)) {
          std::cerr << "cannot peer to node: " << to_string(host) << " on port "
                    << snk_port << endl;
          return;
        }
        source_mode(ep2, topic);
      }};
      sink_mode(ep1, topic);
      source_thread.join();
      break;
    }
  }
  t.join();
  return EXIT_SUCCESS;
}
