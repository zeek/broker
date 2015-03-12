#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "testsuite.h"
#include <iostream>
#include <set>

// A test of "print" style messages sent remotely -- a (path, data) pair.

using namespace std;

static void check_contents(broker::message_queue& pq,
                           set<broker::message> expected)
	{
	set<broker::message> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.insert(move(msg));

	BROKER_TEST(actual == expected);
	}

int main(int argc, char** argv)
	{
	broker::init();

	broker::endpoint node0("node0");
	broker::message_queue pq0a("topic_a", node0);
	broker::message_queue pq0c("topic_c", node0);

	if ( ! node0.listen(9999, "127.0.0.1") )
		{
		cerr << node0.last_error() << endl;
		return 1;
		}

	broker::endpoint node1("node1");
	broker::message_queue pq1b("topic_b", node1);
	broker::message_queue pq1c("topic_c", node1);

	node1.peer("127.0.0.1", 9999);

	if ( node1.outgoing_connection_status().need_pop().front().status !=
	     broker::outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	auto a = *broker::address::from_string("192.168.1.1");
	auto v = broker::vector{a, broker::subnet{a, 16},
	                        broker::port{22, broker::port::protocol::tcp}};

	node0.send("topic_a", {"0a", "hi"});
	node0.send("topic_c", {"0c", v});
	node1.send("topic_b", {"1b", "hello"});
	node1.send("topic_c", {"1c", "well met"});
	node0.send("topic_b", {"0b", "bye"});

	check_contents(pq0a, { broker::message{"0a", "hi"} });
	check_contents(pq0c, { broker::message{"0c", v},
	                       broker::message{"1c", "well met" } });
	check_contents(pq1b, { broker::message{"1b", "hello"},
	                       broker::message{"0b", "bye" } });
	check_contents(pq1c, { broker::message{"0c", v},
	                       broker::message{"1c", "well met"} });

	return BROKER_TEST_RESULT();
	}
