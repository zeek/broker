// Driver for the forwarding test. One process forwards messages, one publishes
// them and another receives them. The role is determined via command-line
// argument.
//
// The forwarder waits for incoming peerings and simply runs in an endless loop.
// The other two processes use a simple rendezvous mechanism to ensure that the
// receiver is subscribed to the correct topic before the publisher starts
// sending messages.
//
// Rendesvouz:
// - receiver waits for a message on /rendezvous/ping, when received, it sends a
//   message on /rendezvous/pong
// - publisher waits for a message on /rendezvous/pong, when received, it starts
//   sending messages on /test
//
// After the last message, the publisher sends a message to /rendezvous/stop in
// order to the tell the forwarder to terminate.

#include <broker/endpoint.hh>
#include <broker/message.hh>
#include <broker/publisher.hh>
#include <broker/status_subscriber.hh>
#include <broker/subscriber.hh>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <variant>

using namespace std::literals;

struct invalid_usage : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

int run_forwarder(broker::endpoint& ep, uint16_t port) {
  // Start listening on the given port.
  auto sub = ep.make_subscriber({"/rendezvous/stop"});
  ep.forward({"/rendezvous/ping"s, "/rendezvous/pong"s, "/test"s});
  if (ep.listen({}, port) != port) {
    std::cout << "Failed to listen on port " << port << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "Listening on configured port\n";
  // Wait up to 5s for the stop message.
  std::ignore = sub.get(5s);
  return EXIT_SUCCESS;
}

int run_publisher(broker::endpoint& ep, uint16_t port, int num) {
  // Establish peering with the forwarder.
  auto pong_sub = ep.make_subscriber({"/rendezvous/pong"});
  if (!ep.peer("localhost", port, 1s)) {
    std::cout << "Failed to peer with localhost:" << port << '\n';
    return EXIT_FAILURE;
  }
  // Send ping every 50ms (up to 5s) and wait for pong.
  auto wait_for_pong = [&] {
    auto ping_pub = ep.make_publisher("/rendezvous/ping");
    auto timeout = std::chrono::steady_clock::now() + 5s;
    for (;;) {
      ping_pub.publish(broker::data{"ping"s});
      auto res = pong_sub.get(50ms);
      if (res)
        return;
      if (std::chrono::steady_clock::now() > timeout) {
        std::cout << "Timeout waiting for pong\n";
        throw std::runtime_error("timeout waiting for pong");
      }
    }
  };
  wait_for_pong();
  // Send messages.
  std::cout << "Start sending messages\n";
  auto pub = ep.make_publisher("/test"); // Subscribed by the receiver.
  for (broker::integer value = 0; value < num; ++value) {
    pub.publish(broker::data{value});
  }
  std::cout << "Sent " << num << " messages\n";
  ep.publish("/rendezvous/stop", broker::data{"stop"s});
  return EXIT_SUCCESS;
}

int run_receiver(broker::endpoint& ep, uint16_t port, int num) {
  // Establish peering with the forwarder.
  auto ping_sub = ep.make_subscriber({"/rendezvous/ping"});
  auto vals_sub = ep.make_subscriber({"/test"});
  if (!ep.peer("localhost", port, 1s)) {
    std::cout << "Failed to peer with localhost:" << port << '\n';
    return EXIT_FAILURE;
  }
  // Wait for the ping and acknowledge with pong.
  if (auto msg = ping_sub.get(5s); !msg) {
    std::cout << "Timeout waiting for ping\n";
    return EXIT_FAILURE;
  }
  ep.publish("/rendezvous/pong", broker::data{"pong"s});
  // Receive messages.
  for (auto i = 0; i < num; ++i) {
    auto msg = vals_sub.get(3s);
    if (!msg) {
      std::cout << "Timeout waiting for message\n";
      return EXIT_FAILURE;
    }
    std::cout << "- " << broker::to_string(broker::get_data(*msg)) << '\n';
  }
  return EXIT_SUCCESS;
}

int main(int argc, char** argv) {
  setvbuf(stdout, nullptr, _IOLBF, 0); // Always line-buffer stdout.
  try {
    if (argc < 2) {
      throw invalid_usage{"missing role"};
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
    if (role == "publisher") {
      if (argc != 3) {
        throw invalid_usage{"missing num-items"};
      }
      return run_publisher(ep, static_cast<uint16_t>(port), std::stoi(argv[2]));
    } else if (role == "receiver") {
      if (argc != 3) {
        throw invalid_usage{"missing num-items"};
      }
      return run_receiver(ep, static_cast<uint16_t>(port), std::stoi(argv[2]));
    }
    if (role != "forwarder") {
      throw invalid_usage{"invalid role"};
    }
    return run_forwarder(ep, static_cast<uint16_t>(port));
  } catch (const invalid_usage& e) {
    std::cerr << "Usage:\n"
              << "- publish-and-receive forwarder\n"
              << "- publish-and-receive publisher <num-items>\n"
              << "- publish-and-receive receiver <num-items>\n";
    return EXIT_FAILURE;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return EXIT_FAILURE;
  } catch (...) {
    std::cerr << "Unknown error\n";
    return EXIT_FAILURE;
  }
}
