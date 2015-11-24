#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "broker/report.hh"
#include "testsuite.h"
#include <vector>
#include <string>
#include <iostream>
#include <chrono>
#include <thread>

// A test of "event" style messages -- vectors of data w/ first element
// being the event name.

using namespace broker;
using namespace std::this_thread; // sleep_for, sleep_until
using namespace std::chrono; // nanoseconds, system_clock, seconds

int main(int argc, char** argv)
	{
	init();

	// init debugging/reporting 
	//broker::report::init();

	/* Overlay configuration
	 * n0 [b]
	 * |
	 * n1 [a] --
	 * |       |
	 * n2 [a]  |
	 * |       |
	 * n3 [a] --
	 */

	// Node 0
	endpoint node0("node0");
	message_queue q0("b", node0);
	// Node 1
	endpoint node1("node1");
	message_queue q1("a", node1);
	// Node 2
	endpoint node2("node2");
	message_queue q2("a", node2);
	// Node 3
	endpoint node3("node3");
	message_queue q3("a", node3);

	// Connections 
	node1.peer(node0);
	if ( node1.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node2.peer(node1);
	if ( node2.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
	}

	node3.peer(node2);
	if ( node3.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
	}

	node3.peer(node1);
	if ( node3.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
	}

	// Sending and receiving
	std::vector<message> pings;
	std::vector<message> pongs;

	// node0 sends ping messages
	for ( int i = 0; i < 1; ++i )
		{
		pings.push_back(message{"ping-0", vector{i, "yo"}});
		node0.send("a", pings[i], 0x02);
		}

	/**
	 * node3 receives one ping
	 * if it receives more pings
	 * the routing mechanism created loops
	 */
	std::vector<message> returned;
	int counter = 0;
	while (counter != 4)
		{
		for ( auto& msg : q3.want_pop() )
			{
			returned.push_back(std::move(msg));
			}
		sleep_for(seconds(1));
		counter++;
		}

	BROKER_TEST(returned.size() == 1);

	// node3 sends a ping message
	for ( int i = 0; i < 1; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node3.send("b", pings[i], 0x02);
		}

	counter = 0;
	for ( auto& msg : q0.need_pop() )
		{
		//std::cout << "node1 received msg " << msg[0] << std::endl;
		counter++;
		}

	BROKER_TEST(pings.size() == 2 && counter == 1);

	return BROKER_TEST_RESULT();
	}
