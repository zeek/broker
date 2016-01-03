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

	// init debugging/reporting 
	//broker::report::init();

	/* Overlay configuration
 	 *        n0 [m:a]
	 *        /      \
	 *   n1[m:a]   n2[s:b,m:a]
	 *				       /        \
	 *            n3[s:b] -- n4[s:b,m:a] 
	 * s: single-hop topic
	 * m: multi-hop topic
	 * n3 and n4 have the AUTO_ROUTING flag turned off to prevent a loop
	 */

	int flags = AUTO_PUBLISH | AUTO_ADVERTISE | AUTO_ROUTING;
	// Node 0
	endpoint node0("node0", flags);
	message_queue q0("a", node0, MULTI_HOP);
	// Node 1
	endpoint node1("node1", flags);
	message_queue q1("c", node1, MULTI_HOP);
	// Node 2
	endpoint node2("node2", flags);
	message_queue q2("a", node2, MULTI_HOP);
	message_queue q2_b("b", node2);

	// Turn off AUTO_ROUTING
	flags = AUTO_PUBLISH | AUTO_ADVERTISE;
	// Node 3
	endpoint node3("node3", flags);
	message_queue q3_a("a", node3, MULTI_HOP);
	message_queue q3_b("b", node3);
	// Node 4
	endpoint node4("node4", flags);
	message_queue q4_a("a", node4, MULTI_HOP);
	message_queue q4_b("b", node4);

	// Connections
	node1.peer(node0);
	if ( node1.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node2.peer(node0);
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

	node4.peer(node2);
	if ( node4.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node4.peer(node3);
	if ( node4.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	// Sending and receiving
	std::vector<message> pings;
	std::vector<message> pongs;

	// node0 sends ping messages
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node0.send("a", pings[i], 0x02);
		}

	// node4 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q4_a.need_pop() )
			{
			msg[0] = "pong";
			node4.send("a", msg, 0x02);
			pongs.push_back(std::move(msg));
			}
		}

	std::vector<message> returned;

	// node0 receives pongs
	while ( returned.size() != 4 )
		for ( auto& msg : q0.need_pop() )
			{
			returned.push_back(std::move(msg));
			}

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	pings.clear();
	pongs.clear();
	returned.clear();
	
	// node4 sends ping messages
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node4.send("b", pings[i], 0x02);
		}

	// node3 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q3_b.need_pop() )
			{
			msg[0] = "pong";
			node3.send("b", msg, 0x02);
			pongs.push_back(std::move(msg));
			}
		}

	// node2 receives pongs
	while ( returned.size() != 8 )
		for ( auto& msg : q2_b.need_pop() )
			{
			returned.push_back(std::move(msg));
			}

	BROKER_TEST(returned.size() == 8);
	BROKER_TEST(pongs.size() == 4);

	return BROKER_TEST_RESULT();
	}
