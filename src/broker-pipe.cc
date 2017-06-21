#include <map>
#include <mutex>
#include <thread>
#include <iostream>

#include <caf/all.hpp>

#include "broker/broker.hh"

using namespace caf;

namespace {

using publish_atom = atom_constant<atom("publish")>;
using subscribe_atom = atom_constant<atom("subscribe")>;

std::mutex cout_mtx;

using guard_type = std::unique_lock<std::mutex>;

class config : public broker::configuration {
public:
  atom_value mode;
  std::string topic;
  std::vector<std::string> peers;
  uint16_t local_port;
  config() {
    opt_group{custom_options_, "global"}
    .add(peers, "peers,p",
         "list of peers we connect to on startup (host:port notation)")
    .add(local_port, "local-port,l",
         "local port for publishing this endpoint at (ignored if 0)")
    .add(topic, "topic,t",
         "topic for sending/receiving messages")
    .add(mode, "mode,m",
         "set mode ('publish' or 'subscribe')");
  }
};

void publish_mode(broker::endpoint& ep, const std::string& topic_str) {
  auto out = ep.make_publisher(topic_str);
  std::string line;
  while (std::getline(std::cin, line)) {
    out.publish(std::move(line));
  }
}

void subscribe_mode(broker::endpoint& ep, const std::string& topic_str) {
  auto in = ep.make_subscriber({topic_str});
  std::string line;
  for (;;) {
    line = deep_to_string(in.get());
    guard_type guard{cout_mtx};
    std::cout << line << std::endl;
  }
}

} // namespace <anonymous>

int main(int argc, char** argv) {
  // Parse CLI parameters using our config.
  config cfg;
  cfg.parse(argc, argv);
  broker::endpoint ep{std::move(cfg)};
  auto es = ep.make_event_subscriber(true);
  std::thread es_thread{[&] {
    std::string line;
    for (;;) {
      line = "*** ";
      auto event = es.get();
      if (broker::is<broker::error>(event))
        line += to_string(get<broker::error>(event));
      else if (broker::is<broker::status>(event))
        line += to_string(get<broker::status>(event));
      else
        continue;
      guard_type guard{cout_mtx};
      std::cerr << line << std::endl;
    }
  }};
  // Publish endpoint at demanded port.
  if (cfg.local_port != 0)
    ep.listen({}, cfg.local_port);
  // Connect to the requested peers.
  for (auto& p : cfg.peers) {
    std::vector<std::string> fields;
    split(fields, p, ':');
    if (fields.size() != 2) {
      guard_type guard{cout_mtx};
      std::cerr << "*** invalid peer: " << p << std::endl;
      continue;
    }
    uint16_t port;
    try {
      port = static_cast<uint16_t>(std::stoi(fields.back()));
    } catch(std::exception&) {
      guard_type guard{cout_mtx};
      std::cerr << "*** invalid port: " << fields.back() << std::endl;
      continue;
    }
    ep.peer(fields.front(), port);
  }
  // Run requested mode.
  void (*f)(broker::endpoint&, const std::string&) = nullptr;
  switch (static_cast<uint64_t>(cfg.mode)) {
    default: {
      guard_type guard{cout_mtx};
      std::cerr
        << "*** invalid mode, expected either 'publish' or 'subscribe'\n";
      return -1;
    }
    case publish_atom::uint_value():
      f = publish_mode;
      break;
    case subscribe_atom::uint_value():
      f = subscribe_mode;
      break;
  }
  f(ep, cfg.topic);
  es_thread.join();
}

