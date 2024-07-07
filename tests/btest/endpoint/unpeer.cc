#include <broker/endpoint.hh>

#include <cstdlib>
#include <iostream>
#include <string_view>

using namespace std::literals;

// Run the responder role, i.e., wait for an incoming peering and then
// wait for the peer_lost status.
int run_responder(broker::endpoint& ep, uint16_t port) {
  // Start listening on the given port.
  auto ssub = ep.make_status_subscriber(true);
  if (ep.listen({}, port) != port) {
    std::cout << "Failed to listen on port " << port << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "Listening on configured port\n";
  // Wait for the peer_lost status.
  auto status_code = broker::sc::unspecified;
  do {
    auto notification = ssub.get(5s);
    if (std::holds_alternative<broker::none>(notification)) {
      std::cout << "Timeout waiting for status\n";
      return EXIT_FAILURE;
    }
    if (std::holds_alternative<broker::error>(notification)) {
      std::cout << "Error waiting for incoming peering: "
                << broker::to_string(std::get<broker::error>(notification))
                << '\n';
      return EXIT_FAILURE;
    }
    status_code = std::get<broker::status>(notification).code();
    std::cout << ">> " << broker::to_string(status_code) << '\n';
  } while (status_code != broker::sc::peer_lost);
  return EXIT_SUCCESS;
}

// Run the originator role, i.e., peer with localhost, wait for the
// peering to be established, and then unpeer.
int run_originator(broker::endpoint& ep, uint16_t port) {
  auto ssub = ep.make_status_subscriber(true);
  if (!ep.peer("localhost", port, 1s)) {
    std::cout << "Failed to peer with localhost:" << port << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "Peering established\n";
  ep.unpeer("localhost", port);
  // Wait for the peer_lost status.
  auto status_code = broker::sc::unspecified;
  do {
    auto notification = ssub.get(5s);
    if (std::holds_alternative<broker::none>(notification)) {
      std::cout << "Timeout waiting for status\n";
      return EXIT_FAILURE;
    }
    if (std::holds_alternative<broker::error>(notification)) {
      std::cout << "Error waiting for incoming peering: "
                << broker::to_string(std::get<broker::error>(notification))
                << '\n';
      return EXIT_FAILURE;
    }
    status_code = std::get<broker::status>(notification).code();
    std::cout << ">> " << broker::to_string(status_code) << '\n';
  } while (status_code != broker::sc::peer_removed);
  return EXIT_SUCCESS;
}

// Run the invalid mode, i.e,. try to unpeer from a non-existing peer.
int run_invalid(broker::endpoint& ep, uint16_t port) {
  auto ssub = ep.make_status_subscriber();
  ep.unpeer("localhost", port);
  auto notification = ssub.get(5s);
  if (!std::holds_alternative<broker::error>(notification)) {
    std::cout << "Timeout waiting for status\n";
    return EXIT_FAILURE;
  }
  auto code = std::get<broker::error>(notification).code();
  std::cout << ">> " << broker::to_string(static_cast<broker::ec>(code))
            << '\n';
  return EXIT_SUCCESS;
}

void usage() {
  std::cout << "Usage: unpeer (originator|responder|invalid)\n";
}

int main(int argc, char** argv) {
  setvbuf(stdout, nullptr, _IOLBF, 0); // Always line-buffer stdout.
  if (argc < 2) {
    usage();
    return EXIT_FAILURE;
  }
  auto role = std::string{argv[1]};
  // Get the port from the BROKER_PORT environment variable.
  std::string port_str;
  if (auto env = getenv("BROKER_PORT")) {
    // btest uses <value>/<protocol> notation. We only care about the value.
    port_str = env;
    if (auto pos = port_str.find('/'); pos != std::string::npos)
      port_str.erase(pos);
  }
  auto port = std::stoi(port_str);
  if (port <= 0 || port > 65535) {
    std::cout << "Invalid port: " << argv[2] << '\n';
    return 1;
  }
  // Dispatch to the appropriate role.
  broker::endpoint::system_guard sys_guard; // Initialize global state.
  broker::endpoint ep;
  if (role == "originator") {
    return run_originator(ep, static_cast<uint16_t>(port));
  } else if (role == "responder") {
    return run_responder(ep, static_cast<uint16_t>(port));
  } else if (role == "invalid") {
    return run_invalid(ep, static_cast<uint16_t>(port));
  }
  std::cout << "Invalid role: " << role << '\n';
  usage();
  return EXIT_FAILURE;
}
