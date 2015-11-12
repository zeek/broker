#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "broker/report.hh"
#include "testsuite.h"
#include <vector>
#include <string>
#include <iostream>
#include <assert.h>

// A test of "event" style messages -- vectors of data w/ first element
// being the event name.

using namespace broker;

int main(int argc, char** argv)
	{
	init();

	// init debugging/reporting 
	//broker::report::init();

	/* Overlay configuration
	 *       n0 [a]
	 *       |
 	 *       n1 [b]
	 *      /  \ 
	 * [c] n2  n3 [b] 
	 *     |   |		  
	 *     |   n4 [d] 
	 *     |   |			
	 *     |---n5 [e] 
	 * 
	 * 1. n0 sends to n5
	 * 2. n5 replies to n0 and n2
	 * 3. n0 publishes e 
	 * 4. n5 unpeers from n2, which partitions overlay and forces other paths
	 * 5. n5 publishes a
	 */

	// Node 0
	endpoint node0("node0");
	message_queue q0("a", node0);
	// Node 1
	endpoint node1("node1");
	message_queue q1("b", node1);
	// Node 2
	endpoint node2("node2");
	message_queue q2("c", node2);
	// Node 3
	endpoint node3("node3");
	message_queue q3("b", node3);
	// Node 4
	endpoint node4("node4");
	message_queue q4("d", node4);
	// Node 5
	endpoint node5("node5");
	message_queue q5("e", node5);

	// Connections
	peering n1n0 = node1.peer(node0);
	if ( node1.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	peering n2n1 = node2.peer(node1);
	if ( node2.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	peering n3n1 = node3.peer(node1);
	if ( node3.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	peering n4n3 = node4.peer(node3);
	if ( node4.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	peering n5n4 = node5.peer(node4);
	if ( node5.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}


	peering n5n2 = node5.peer(node2);
	if ( node5.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	// Sending and receiving
	std::vector<message> pings;
	std::vector<message> pongs;

	// Sending n0 - n3 - n0
	// node0 sends ping messages
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping-1", vector{i, "yo"}});
		node0.send("b", pings[i], 0x02);
		}

	// node3 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q3.need_pop() )
			{
			msg[0] = "pong";
			node3.send("a", msg, 0x02);
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

	returned.clear();
	pings.clear();
	pongs.clear();

	// Sending n0 - n5 - n0
	// node0 sends pings again to node5
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping-2", vector{i, "yo"}});
		node0.send("e", pings[i], 0x02);
		}

	// node5 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q5.need_pop() )
			{
			pongs.push_back(msg);
			returned.push_back(std::move(msg));
			}
		}

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	// Unpeer from node5 to node2
	node5.unpeer(n5n2);

	pings.clear();

	// node0 sends ping messages
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping-3", vector{i, "yo"}});
		node0.send("b", pings[i], 0x02);
		}

	pongs.clear();

	// node1 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q1.need_pop() )
			{
			if(msg[0] == "ping-3")
				{
				msg[0] = "pong-3";
				node1.send("a", msg, 0x02);
				pongs.push_back(std::move(msg));
				}
			}
		}

	returned.clear();

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

	// node0 sends ping messages to node5
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"final-ping", vector{i, "yo"}});
		node0.send("e", pings[i], 0x02);
		}

	// node5 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q5.need_pop() )
			{
			msg[0] = "final-pong";
			node5.send("a", msg, 0x02);
			pongs.push_back(std::move(msg));
			}
		}

	// node0 receives pongs 
	while ( returned.size() != 4 )
		for ( auto& msg : q0.need_pop() )
			{
			returned.push_back(std::move(msg));
			}

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	for ( int i = 0; i < (int)returned.size(); ++i )
		{
		BROKER_TEST(returned[i][0] == "final-pong");
		const auto& v = *get<vector>(returned[i][1]);
		BROKER_TEST(v[0] == i);
		BROKER_TEST(v[1] == "yo");
		}

	return BROKER_TEST_RESULT();
	}
