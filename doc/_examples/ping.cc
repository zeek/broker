// ping.cc

#include <assert.h>

#include "broker/broker.hh"
#include "broker/bro.hh"

using namespace broker;

int main() {
    // Setup endpoint and connect to Bro.
    endpoint ep;
    auto sub = ep.make_subscriber({"/topic/test"});
    auto ss = ep.make_status_subscriber(true);
    ep.peer("127.0.0.1", 9999);

    // Wait until connection is established.
    auto st = get_if<status>(ss.get());
    if ( ! (st && st->code() == sc::peer_added) ) {
        std::cerr << "could not connect" << std::endl;
	return 1;
    }

    for ( int n = 0; n < 5; n++ ) {
        // Send event "ping(n)".
        bro::Event ping("ping", {n});
        ep.publish("/topic/test", ping);

        // Wait for "pong" reply event.
        auto msg = sub.get();
	bro::Event pong(msg.second);
        std::cout << "received " << pong.name() << pong.args() << std::endl;
    }

    return 0;
}
