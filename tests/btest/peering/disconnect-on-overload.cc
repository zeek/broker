// Driver for the disconnect-on-overload test. One process subscribes to topic
// /test/data and another one publishes messages to it. The subscriber will
// consume messages at a very slow rate, causing the publisher to buffer them
// and eventually disconnect due to overload.
//
// Rendesvouz:
// - receiver waits for a message on /rendezvous/ping, when received, it sends a
//   message on /rendezvous/pong
// - publisher waits for a message on /rendezvous/pong, when received, it starts
//   sending messages on /test
//
// Both processes terminate in response to the disconnect event.

#include <broker/detail/overload.hh>
#include <broker/endpoint.hh>
#include <broker/message.hh>
#include <broker/publisher.hh>
#include <broker/status_subscriber.hh>
#include <broker/subscriber.hh>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

using namespace std::literals;

struct invalid_usage : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct done_predicate {
  bool operator()(broker::none) const {
    return false;
  }

  bool operator()(const broker::error& err) const {
    std::cerr << "Error: " << broker::to_string(err) << '\n';
    return true;
  }

  bool operator()(const broker::status& st) const {
    std::cerr << "Status: " << broker::to_string(st) << '\n';
    return st == broker::sc::peer_removed || st == broker::sc::peer_lost;
  }
};

int run_sender(broker::endpoint& ep, uint16_t port) {
  auto ssub = ep.make_status_subscriber(true);
  auto ok = ep.peer("localhost", port, 0s);
  if (!ok) {
    throw std::runtime_error{"failed to peer with receiver"};
  }
  auto value = broker::count{0};
  for (;;) {
    // Check every 50 messages whether the receiver disconnected.
    for (auto i = 0; i < 50; ++i) {
      ep.publish("/test/data", broker::data{value});
      ++value;
    }
    if (std::visit(done_predicate{}, ssub.get(10ms))) {
      return EXIT_SUCCESS;
    }
  }
}

int run_receiver(broker::endpoint& ep, uint16_t port) {
  auto ssub = ep.make_status_subscriber(true);
  auto vsub = ep.make_subscriber({"/test/data"});
  auto used_port = ep.listen({}, port);
  if (used_port != port) {
    throw std::runtime_error{"failed to listen on port "
                             + std::to_string(port)};
  }
  puts("");
  // Read a message every 50ms until the peer disconnects.
  for (;;) {
    std::this_thread::sleep_for(50ms);
    auto val = vsub.get(0s);
    if (val) {
      std::cout << "\rreceived: " << broker::to_string(*val) << std::flush;
    }
    if (std::visit(done_predicate{}, ssub.get(10ms))) {
      return EXIT_SUCCESS;
    }
  }
}

int main(int argc, char** argv) {
  setvbuf(stdout, nullptr, _IOLBF, 0); // Always line-buffer stdout.
  try {
    if (argc != 3) {
      throw invalid_usage{"missing role / port"};
    }
    auto role = std::string{argv[1]};
    auto port = static_cast<uint16_t>(std::stoi(argv[2]));
    broker::broker_options opts;
    opts.peer_buffer_size = 128;
    broker::endpoint ep{broker::configuration{opts}};
    if (role == "sender") {
      return run_sender(ep, port);
    } else if (role == "receiver") {
      return run_receiver(ep, port);
    } else {
      throw invalid_usage{"invalid role"};
    }
  } catch (const invalid_usage&) {
    std::cerr << "Usage:\n"
              << "- disconnect-on-overload sender <port>\n"
              << "- disconnect-on-overload receiver <port>\n";
    return EXIT_FAILURE;
  } catch (const std::exception& ex) {
    std::cerr << "Error: " << ex.what() << '\n';
    return EXIT_FAILURE;
  } catch (...) {
    std::cerr << "Unknown error\n";
    return EXIT_FAILURE;
  }
}
