#include "broker/broker.hh"

#define SUITE endpoint
#include "test.hpp"

// A test of "event" style messages -- vectors of data w/ first element
// being the event name.

using namespace broker;

TEST(multihop chain of nodes) {
	/* Overlay configuration
	 * n0 [a]
	 * |
	 * n1 [b]
	 * |  
	 * n2 [c]
	 */

	// init debugging/reporting 
	//broker::report::init();

  context ctx;
  auto node0 = ctx.spawn<blocking>();
  auto node1 = ctx.spawn<blocking>();
  auto node2 = ctx.spawn<blocking>();
	node0.subscribe("a")
	node1.subscribe("b")
	node2.subscribe("c")

	// Sending and receiving
	std::vector<message> pings;
	std::vector<message> pongs;

	// node2 sends ping messages
	for ( int i = 0; i < 4; ++i ) {
		pings.push_back(message{"ping", vector{i, "yo"}});
		node2.publish("a", pings[i]);
	}

	// node0 receives pings and sends pongs
	while ( pongs.size() != 4 ) {
		node0.receive([](const topic& t, data& msg) {
			msg[0] = "pong";
			node0.publish("c", msg);
			pongs.push_back(std::move(msg));
		});
	}

	// node2 receives pongs
	std::vector<message> returned;
	while ( returned.size() != 4 )
		node2.receive([](const topic& t, const data& msg) {
			returned.push_back(std::move(msg))
		});

	CHECK(returned.size() == 4);
	CHECK(returned == pongs);
	for ( int i = 0; i < (int)returned.size(); ++i )
		{
		CHECK(returned[i][0] == "pong");
		const auto& v = *get<vector>(returned[i][1]);
		CHECK(v[0] == i);
		CHECK(v[1] == "yo");
		}
	}
