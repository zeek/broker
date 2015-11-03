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
	 * n0 [a,b]
	 * |
	 * n1 [a,b]
	 * |  
	 * n2 [a,b]
	 *
	 * + Socket communication
	 */

	// init debugging/reporting 
	broker::report::init();

	// Node 0
	endpoint node0("node0");
	node0.listen(9990, "127.0.0.1");
	message_queue q0a("a", node0);
	message_queue q0b("b", node0);

	// Node 1
	endpoint node1("node1");
	node1.listen(9991, "127.0.0.1");;
	message_queue q1a("a", node1);
	message_queue q1b("b", node1);
	// connecting
	node1.peer("127.0.0.1", 9990);

	if ( node1.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	// Node 2
	endpoint node2("node2");
	node1.listen(9992, "127.0.0.1");;
	message_queue q2a("a", node2);
	message_queue q2b("b", node2);
	// connecting
	node2.peer("127.0.0.1", 9991);

	if ( node2.outgoing_connection_status().need_pop().front().status !=
	     outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}


	std::vector<message> msg_a;
	msg_a.push_back(message{"command", "yo"});
	// sending request
	node0.send("a", msg_a[0], 0x02);

	for ( auto& msg : q1a.need_pop() )
		{
		std::cout << "node1 received msg " << msg[0] << std::endl;
		}

	for ( auto& msg : q2a.need_pop() )
		{
		std::cout << "node2 received msg " << msg[0] << std::endl;
		}

		//sending reply
		std::vector<message> msg_b;
		msg_b.push_back(message{"result", "yo"});
		node1.send("b", msg_b[0], 0x02);
		node2.send("b", msg_b[0], 0x02);
		node2.send("b", msg_b[0], 0x02);
		

	//BROKER_TEST(msg_1a == msg_2a);

	int counter = 0;
	while(counter != 3)
		{
		for ( auto& msg : q0b.need_pop() )
			{
			std::cout << "node0 received msg " << msg[0] << std::endl;
			counter++;
			}
		}

	BROKER_TEST(counter == 3);

	return BROKER_TEST_RESULT();
	}
