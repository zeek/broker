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
	 *       n0 [a]
	 *       |
 	 *       n1 [b]
	 *      /  \
	 * [c] n2  n3 [b]
	 *         |
	 *         n4 [d]
	 *         |
	 *         n5 [e]
	 * 
	 * 1. n0 sends to n5
	 * 2. n5 replies to n0 and n2
	 * 3. n3 unpeers from n1, which partitions overlay
	 * 4. n0 publishes e, which has no subscriber in first overlay
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

	// Sending and receiving
	std::vector<message> pings;
	std::vector<message> pongs;

	// Sending n0 - n3 - n0
	// node0 sends ping messages
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node0.send("b", pings[i], 0x02);
		}

	// node3 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q3.need_pop() )
			{
			std::cout << "node3: ping received, msg size " << msg.size() << std::endl;
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
			std::cout << "node0: pong received" << std::endl;
			returned.push_back(std::move(msg));
			}

	returned.clear();
	pongs.clear();

	// Sending n0 - n5 - n0
	// node0 sends pings again to node5
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node0.send("e", pings[i], 0x02);
		}

	// node5 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q5.need_pop() )
			{
			std::cout << "node3: ping received, msg size " << msg.size() << std::endl;
			msg[0] = "pong";
			node5.send("a", msg, 0x02);
			pongs.push_back(std::move(msg));
			}
		}

	// node0 receives pongs 
	while ( returned.size() != 4 )
		for ( auto& msg : q0.need_pop() )
			{
			std::cout << "node0: pong received" << std::endl;
			returned.push_back(std::move(msg));
			}

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	returned.clear();
	pongs.clear();

	// Unpeer from node3 to node1
	node3.unpeer(n3n1);

	// node0 sends ping messages
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node0.send("b", pings[i], 0x02);
		}

	// node1 receives pings and sends pongs
	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q1.need_pop() )
			{
			std::cout << "node3: ping received, msg size " << msg.size() << std::endl;
			msg[0] = "pong";
			node1.send("a", msg, 0x02);
			pongs.push_back(std::move(msg));
			}
		}

	// node0 receives pongs 
	while ( returned.size() != 4 )
		for ( auto& msg : q0.need_pop() )
			{
			std::cout << "node0: pong received" << std::endl;
			returned.push_back(std::move(msg));
			}

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	// node0 sends ping messages to node5
	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back(message{"ping", vector{i, "yo"}});
		node0.send("e", pings[i], 0x02);
		}

	for ( int i = 0; i < (int)returned.size(); ++i )
		{
		BROKER_TEST(returned[i][0] == "pong");
		const auto& v = *get<vector>(returned[i][1]);
		BROKER_TEST(v[0] == i);
		BROKER_TEST(v[1] == "yo");
		}

	return BROKER_TEST_RESULT();
	}
