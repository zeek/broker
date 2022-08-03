// ping.cc

#include <cassert>
#include <iostream>

#include "broker/broker.hh"
#include "broker/zeek.hh"

using namespace broker;

int main() {
  // Setup endpoint and connect to Zeek.
  endpoint ep;
  auto sub = ep.make_subscriber({"/topic/test"});
  auto ss = ep.make_status_subscriber(true);
  ep.peer("127.0.0.1", 9999);

  // Wait until connection is established.
  for (bool has_peer = false; !has_peer;) {
    auto val = ss.get();
    if (auto st = get_if<status>(&val))
      has_peer = st->code() == sc::peer_added;
  }

  // Do five ping / pong.
  for (int n = 0; n < 5; n++) {
    // Send event "ping(n)".
    zeek::Event ping("ping", {n});
    ep.publish("/topic/test", ping);

    // Wait for "pong" reply event.
    auto msg = sub.get();
    zeek::Event pong(move_data(msg));
    std::cout << "received " << pong.name() << pong.args() << std::endl;
  }
}
