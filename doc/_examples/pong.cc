// pong.cc

#include <assert.h>
#include <iostream>

#include "broker/broker.hh"
#include "broker/zeek.hh"

using namespace broker;

int main() {
  // Setup endpoint and connect to Zeek.
  endpoint ep;
  auto sub = ep.make_subscriber({"/topic/test"});
  auto ss = ep.make_status_subscriber(true);
  ep.listen("", 9999);

  // Wait until connection is established.
  for (bool has_peer = false; !has_peer;) {
    auto val = ss.get();
    if (auto st = get_if<status>(&val))
      has_peer = st->code() == sc::peer_added;
  }

  // Do five ping / pong.
  for (int n = 0; n < 5; n++) {
    // Wait for a "ping" event.
    auto msg = sub.get();
    auto ping = zeek::Event{std::move(msg)};
    if (ping.valid())
      std::cout << "received " << ping.name() << ping.args() << std::endl;
    else
      std::cout << "received invalid ping message: " << to_string(ping)
                << std::endl;

    // Send event "pong" response.
    ep.publish("/topic/test", zeek::Event{"pong", {n}});
  }
}
