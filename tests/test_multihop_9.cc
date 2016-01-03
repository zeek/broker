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
     * n0 [a] + [b]
	 * |
     * n1 [a]
	 * |  
     * n2 [a]
	 *
	 * n0 sends message with topic a
	 * n1, n2 send replies for a
	 * n0 adds topic b
	 * n2 sends message with topic b
	 */

	int flags = AUTO_PUBLISH | AUTO_ADVERTISE | AUTO_ROUTING;
	// Node 0
	endpoint node0("node0", flags);
	node0.listen(9990, "127.0.0.1");
	message_queue q0a("a", node0, MULTI_HOP);

	// Node 1
	endpoint node1("node1", flags);
	node1.listen(9991, "127.0.0.1");;
	message_queue q1a("a", node1, MULTI_HOP);
	//message_queue q1b("b", node1);

	// Node 2
	endpoint node2("node2", flags);
	node1.listen(9992, "127.0.0.1");;
	message_queue q2a("a", node2, MULTI_HOP);

	// connecting
	auto n0n1 = node0.peer("127.0.0.1", 9991);

	if ( node0.outgoing_connection_status().need_pop().front().status !=
		 outgoing_connection_status::tag::established)
	{
	BROKER_TEST(false);
	return 1;
	}

	// connecting
  auto n1n2 = node2.peer("127.0.0.1", 9991);

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

	int counter = 0;
	for ( auto& msg : q1a.need_pop() )
		{
		//std::cout << "node1 received msg " << msg[0] << std::endl;
		counter++;
		}

	for ( auto& msg : q2a.need_pop() )
    {
		//std::cout << "node2 received msg " << msg[0] << std::endl;
		counter++;
		}

    //sending reply
    std::vector<message> msg_b;
    msg_b.push_back(message{"result", "yo"});
    node1.send("a", msg_b[0], 0x02);
    node2.send("a", msg_b[0], 0x02);

    while(counter < 4)
		 {
     for ( auto& msg : q0a.need_pop() )
		  {
			//std::cout << "node0 received msg " << msg[0] << std::endl;
			counter++;
			}
		 } 

	BROKER_TEST(counter == 4);

	// add new topic at node0
	message_queue q0b("b", node0, MULTI_HOP);
	sleep_for(seconds(1));

    // sending request
  std::vector<message> msg_important;
  msg_important.push_back(message{"important", "message"});
	node2.send("b", msg_important[0], 0x02);
	node2.send("b", msg_important[0], 0x02);

	while(counter  < 6)
	  {
	  for ( auto& msg : q0b.need_pop() )
		 {
		 counter++;
		 //std::cout << "node0 received msg " << msg[0] << std::endl;
		 }
	  }

	BROKER_TEST(counter == 6);

	node0.unlisten(0);
	node1.unlisten(0);
	node2.unlisten(0);


	return BROKER_TEST_RESULT();
	}
