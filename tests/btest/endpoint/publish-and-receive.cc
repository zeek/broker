// Driver for the publish-and-receive test. One process publishes messages and
// another receives them. The role is determined via command-line argument.
//
// The publisher waits for an incoming peering from the receiver and then sends
// the messages.

#include <broker/endpoint.hh>
#include <broker/message.hh>
#include <broker/publisher.hh>
#include <broker/status_subscriber.hh>
#include <broker/subscriber.hh>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <variant>

using namespace std::literals;

int run_publisher(broker::endpoint& ep, uint16_t port, int num,
                  std::string_view mode) {
  // Start listening on the given port.
  auto ssub = ep.make_status_subscriber(true);
  if (ep.listen({}, port) != port) {
    std::cout << "Failed to listen on port " << port << '\n';
    return EXIT_FAILURE;
  }
  std::cout << "Listening on configured port\n";
  // Wait for the incoming peering.
  auto status_code = broker::sc::unspecified;
  do {
    auto notification = ssub.get(5s);
    if (std::holds_alternative<broker::none>(notification)) {
      std::cout << "Timeout waiting for incoming peering\n";
      return EXIT_FAILURE;
    }
    if (std::holds_alternative<broker::error>(notification)) {
      std::cout << "Error waiting for incoming peering: "
                << broker::to_string(std::get<broker::error>(notification))
                << '\n';
      return EXIT_FAILURE;
    }
    status_code = std::get<broker::status>(notification).code();
  } while (status_code != broker::sc::peer_added);
  // Send messages.
  std::cout << "Start sending messages\n";
  auto pub1 = ep.make_publisher("/test");         // Subscribed by the receiver.
  auto pub2 = ep.make_publisher("/unsubscribed"); // Not subscribed.
  if (mode == "single"sv) {
    for (broker::integer value = 0; value < num; ++value) {
      pub1.publish(broker::data{value});
      pub2.publish(broker::data{value});
      std::ignore = ssub.poll(); // Drop any events.
    }
  } else {
    std::vector<broker::data> batch;
    for (broker::integer value = 0; value < num; ++value) {
      batch.emplace_back(value);
    }
    pub1.publish(batch);
    pub2.publish(batch);
  }
  std::cout << "Sent " << num << " messages\n";
  return EXIT_SUCCESS;
}

int run_receiver(broker::endpoint& ep, uint16_t port, int num) {
  auto sub = ep.make_subscriber({"/test"});
  if (!ep.peer("localhost", port, 1s)) {
    std::cout << "Failed to peer with localhost:" << port << '\n';
    return EXIT_FAILURE;
  }
  std::vector<broker::variant> received;
  for (auto i = 0; i < num; ++i) {
    auto msg = sub.get(3s);
    if (!msg) {
      std::cout << "Timeout waiting for message\n";
      return EXIT_FAILURE;
    }
    received.push_back(broker::get_data(*msg));
  }
  std::cout << "Received:\n";
  for (auto& value : received) {
    std::cout << "- " << broker::to_string(value) << '\n';
  }
  return EXIT_SUCCESS;
}

[[noreturn]] void usage() {
  std::cerr << "Usage:\n"
            << "- publish-and-receive publisher <num-items> <single|batch>\n"
            << "- publish-and-receive receiver <num-items>\n";
  exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {
  setvbuf(stdout, nullptr, _IOLBF, 0); // Always line-buffer stdout.
  if (argc < 2) {
    usage();
  }
  // Check the role.
  auto is_publisher = false;
  if (std::string_view{argv[1]} == "publisher") {
    is_publisher = true;
    std::cout << "running as publisher\n";
  } else if (argv[1] == "receiver"sv) {
    std::cout << "running as receiver\n";
  } else {
    std::cout << "Invalid role: " << argv[1] << '\n';
    return EXIT_FAILURE;
  }
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
  if (is_publisher) {
    if (argc != 4) {
      usage();
    }
    return run_publisher(ep, static_cast<uint16_t>(port), std::stoi(argv[2]),
                         argv[3]);
  } else {
    if (argc != 3) {
      usage();
    }
    return run_receiver(ep, static_cast<uint16_t>(port), std::stoi(argv[2]));
  }
}
