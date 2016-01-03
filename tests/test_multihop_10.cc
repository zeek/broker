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

	/* Overlay configuration as in 
	 * a typical bro cluster setting
	 *
	 *  ---------- -----------
	 *  |        | |         |
	 *	| -------n0[b]------ |
	 *	| |	      |        | |
	 *	| |  ----n1[a]--   | |
	 *	| |  |		|    |   | |
	 *	| --n2[a] |  n3[a]-- |
	 *	|	   |		|	   |     |
	 *	|    ---n4[a]---     |
	 *	|	       | |         |
	 *	---------- ----------
	 *
	 *	node0 (broctl) is connected to all cluster nodes
	 *  single-hop subscriptions only!
	 */

	int flags = AUTO_PUBLISH | AUTO_ADVERTISE;
	// Node 0
	endpoint node0("node0", flags);
	message_queue q0("b", node0);
	// Node 1
	endpoint node1("node1", flags);
	message_queue q1("a", node1);
	// Node 2
	endpoint node2("node2", flags);
	message_queue q2("a", node2);
	// Node 3
	endpoint node3("node3", flags);
	message_queue q3("a", node3);
	// Node 4
	endpoint node4("node4", flags);
	message_queue q4("a", node4);

	// Connections 
	node2.peer(node4);
	if ( node2.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
	}

	node3.peer(node4);
	if ( node3.outgoing_connection_status().need_pop().front().status !=
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

	node3.peer(node1);
	if ( node3.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
	}

	node4.peer(node1);
	if ( node4.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
	}

	node0.peer(node1);
	if ( node0.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node0.peer(node2);
	if ( node0.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node0.peer(node3);
	if ( node0.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node0.peer(node4);
	if ( node0.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	// Sending and receiving
	std::vector<message> pings;
	std::vector<message> pongs;

	// node0 sends a ping message for topic a
	pings.push_back(message{"ping-0", vector{0, "yo"}});
	node0.send("a", pings[0], 0x02, 8);

	sleep_for(seconds(1));
	 // node3 receives one ping
	 // if it receives more pings
	 // the routing mechanism created loops
	for ( auto& msg : q1.need_pop() )
		{
		msg[0] = "pong";
		node1.send("b", msg, 0x02, 8);
		pongs.push_back(std::move(msg));
		}

	for ( auto& msg : q2.need_pop() )
		{
		msg[0] = "pong";
		node2.send("b", msg, 0x02, 8);
		pongs.push_back(std::move(msg));
		}

	for ( auto& msg : q3.need_pop() )
		{
		msg[0] = "pong";
		node3.send("b", msg, 0x02, 8);
		pongs.push_back(std::move(msg));
		}

	for ( auto& msg : q4.need_pop() )
		{
		msg[0] = "pong";
		node4.send("b", msg, 0x02, 8);
		pongs.push_back(std::move(msg));
		}

	// Node0 should receive exactly one answer
	std::vector<message> returned;
	while ( returned.size() != 4 )
		{
		for ( auto& msg : q0.need_pop() )
			{
			returned.push_back(std::move(msg));
			}
		}

	BROKER_TEST(pongs.size() == 4 && returned.size() == 4);

	return BROKER_TEST_RESULT();
	}
