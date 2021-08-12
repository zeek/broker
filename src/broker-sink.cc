#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <caf/deep_to_string.hpp>
#include <caf/downstream.hpp>

#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/status_subscriber.hh"
#include "broker/topic.hh"
#include "broker/zeek.hh"

using namespace broker;
using namespace std::literals;

namespace {

using string_list = std::vector<std::string>;
using topic_list = std::vector<topic>;
using uri_list = std::vector<caf::uri>;

struct config : configuration {
  using super = broker::configuration;
  config() : super(skip_init) {
    opt_group{custom_options_, "global"}
      .add<uri_list>("peers,p",
                     "list of peers we connect to on startup in "
                     "<tcp://$host:$port> notation")
      .add<string_list>("topics,t", "topics for sending/receiving messages");
  }
  using super::init;
};

std::optional<network_info> to_network_info(const caf::uri& from) {
  using std::nullopt;
  if (from.empty())
    return nullopt;
  if (from.scheme() != "tcp")
    return nullopt;
  const auto& auth = from.authority();
  if (auth.empty())
    return nullopt;
  std::string host;
  auto get_host = [&](const auto& host) {
    if constexpr (std::is_same<decltype(host), const std::string&>::value)
      return host;
    else
      return to_string(host);
  };
  return network_info{caf::visit(get_host, auth.host), auth.port, 1s};
}

} // namespace

int main(int argc, char** argv) {
  broker::configuration::init_global_state();
  // Parse CLI parameters using our config.
  config cfg;
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << "cfg.init failed: " << ex.what() << '\n';
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  // Get topics (mandatory).
  topic_list topics;
  { // Lifetime scope of temporary variables.
    auto topic_names = caf::get_or(cfg, "topics", string_list{});
    if (topic_names.empty()) {
      std::cerr << "no topics specified" << '\n';
      return EXIT_FAILURE;
    }
    for (auto& topic_name : topic_names)
      topics.emplace_back(std::move(topic_name));
  }
  // Get peer URIs.
  auto peers = caf::get_or(cfg, "peers", uri_list{});
  if (peers.empty()) {
    std::cerr << "no peers specified" << '\n';
    return EXIT_FAILURE;
  }
  // Spin up the endpoint, subscribe and peer.
  broker::endpoint ep{std::move(cfg)};
  auto in = ep.make_subscriber(std::move(topics));
  for (auto& peer : peers) {
    if (auto info = to_network_info(peer)) {
      if (!ep.peer(*info)) {
        std::cerr << "unable to connect to <" << peer.str() << ">\n";
        return EXIT_FAILURE;
      }
    } else {
      std::cerr
        << "unrecognized scheme (expected tcp) or found no authority in: <"
        << peer.str() << ">\n";
      return EXIT_FAILURE;
    }
  }
  // Receive loop.
  auto timeout = broker::now();
  timeout += 1s;
  size_t received = 0;
  for (;;) {
    auto x = in.get(timeout);
    if (x) {
      ++received;
    } else {
      std::cout << received << '\n';
      timeout += 1s;
      received = 0;
    }
  }
}
