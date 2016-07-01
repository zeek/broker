#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "broker/report.hh"
#include "testsuite.h"
#include <vector>
#include <string>
#include <iostream>

// A test of "event" style messages -- vectors of data w/ first element
// being the event name.

using namespace broker;

int main(int argc, char** argv)
	{
	init();

	/* Overlay configuration
	 * n0 [a]
	 * |
	 * n1 [b]
	 * |  
	 * n2 [c]
	 */

	// init debugging/reporting 
	broker::report::init();

	int flags = AUTO_PUBLISH | AUTO_ADVERTISE | AUTO_ROUTING;
	// Node 0
	endpoint node0("node0", flags);
	message_queue q0("a", node0, GLOBAL_SCOPE);

	// Node 1
	endpoint node1("node1", flags);
	message_queue q1("b", node1, GLOBAL_SCOPE);
	// connecting
	node1.peer(node0);

	if ( node1.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	// Node 2
	endpoint node2("node2");
	message_queue q2("c", node2, GLOBAL_SCOPE);
	// connecting
	node2.peer(node1);

	if ( node2.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	// Sending and receiving
	std::vector<message> pings;
	std::vector<message> pongs;

	// node2 sends ping messages
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node2.send("a", pings[i], 0x02);
		}

	// node0 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q0.need_pop() )
			{
			msg[0] = "pong";
			node0.send("c", msg, 0x02);
			pongs.push_back(std::move(msg));
			}
		}

	std::vector<message> returned;

	// node2 receives pongs
	while ( returned.size() != 4 )
		for ( auto& msg : q2.need_pop() )
			{
			returned.push_back(std::move(msg));
			}

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	for ( int i = 0; i < (int)returned.size(); ++i )
		{
		BROKER_TEST(returned[i][0] == "pong");
		const auto& v = *get<vector>(returned[i][1]);
		BROKER_TEST(v[0] == i);
		BROKER_TEST(v[1] == "yo");
		}

	return BROKER_TEST_RESULT();
	}
