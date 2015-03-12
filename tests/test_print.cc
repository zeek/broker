#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "testsuite.h"
#include <vector>
#include <set>
#include <poll.h>

// A test of "print" style messages -- a (path, data) pair.

using namespace std;

static bool check_contents_poll(broker::message_queue& pq,
                                set<broker::message> expected)
	{
	set<broker::message> actual;
	pollfd pfd{pq.fd(), POLLIN, 0};

	while ( actual.size() < expected.size() )
		{
		poll(&pfd, 1, -1);

		for ( auto& msg : pq.want_pop() )
			actual.insert(move(msg));
		}

	return actual == expected;
	}

static bool check_contents(broker::message_queue& pq,
                           set<broker::message> expected)
	{
	set<broker::message> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.insert(move(msg));

	return actual == expected;
	}

int main(int argc, char** argv)
	{
	broker::init();

	broker::endpoint node0("node0");
	broker::message_queue pq_a0("topic_a", node0);

	node0.send("topic_a", {"/", "hello"});
	node0.send("nobody", {"/", "pointless"});
	node0.send("topic_a", {"/", "goodbye"});

	vector<broker::message> node0_msgs;

	while ( node0_msgs.size() < 2 )
		for ( auto& msg : pq_a0.need_pop() )
			node0_msgs.push_back(move(msg));

	BROKER_TEST((node0_msgs[0] == broker::message{"/", "hello"}));
	BROKER_TEST((node0_msgs[1] == broker::message{"/", "goodbye"}));

	broker::message_queue pq_b0("topic_b", node0);
	node0.send("topic_a", {"aaa", "hi"});
	node0.send("nobody", {"", "pointless"});
	node0.send("topic_b", {"bbb", "bye"});

	BROKER_TEST((pq_b0.need_pop().front() == broker::message{"bbb", "bye"}));
	BROKER_TEST((pq_a0.need_pop().front() == broker::message{"aaa", "hi"}));

	broker::endpoint node1("node1");
	broker::endpoint node2("node2");
	broker::message_queue pq_a1("topic_a", node1);
	broker::message_queue pq_b1("topic_b", node1);
	broker::message_queue pq_a2("topic_a", node2);
	node0.peer(node1);
	node0.peer(node2);

	if ( node0.outgoing_connection_status().need_pop().size() < 2 )
		node0.outgoing_connection_status().need_pop();

	node0.send("topic_a", {"0a", node0.name() + " says: hi"}); // to 0, 1, 2
	node0.send("topic_a", {"0a", node0.name() + " says: hello"}); // to 0, 1, 2
	node0.send("nobody", {"", "pointless"});
	node0.send("topic_b", {"0b", node0.name() + " says: bye"}); // to 0, 1
	node0.send("topic_b", {"0b", node0.name() + " says: goodbye"}); // to 0, 1

	node1.send("topic_a", {"1a", node1.name() + " says: hi"}); // to 0, 1
	node1.send("topic_a", {"1a", node1.name() + " says: bye"}); // to 0, 1
	node1.send("topic_b", {"1b", node1.name() + " says: bbye"}); // to 0, 1

	node2.send("topic_a", {"2a", node2.name() + " says: hi"}); // to 0, 2
	node2.send("topic_a", {"2a", node2.name() + " says: bye"}); // to 0, 2
	node2.send("topic_b", {"2b", node2.name() + " says: bbye"}); // to 0

	BROKER_TEST(
	check_contents_poll(pq_a0, {
	                   broker::message{"0a", "node0 says: hi"},
	                   broker::message{"0a", "node0 says: hello"},
	                   broker::message{"1a", "node1 says: hi"},
	                   broker::message{"1a", "node1 says: bye"},
	                   broker::message{"2a", "node2 says: hi"},
	                   broker::message{"2a", "node2 says: bye"}
	               })
	);

	BROKER_TEST(
	check_contents(pq_b0, {
	                   broker::message{"0b", "node0 says: bye"},
	                   broker::message{"0b", "node0 says: goodbye"},
	                   broker::message{"1b", "node1 says: bbye"},
	                   broker::message{"2b", "node2 says: bbye"}
	               })
	);

	BROKER_TEST(
	check_contents_poll(pq_a1, {
	                   broker::message{"0a", "node0 says: hi"},
	                   broker::message{"0a", "node0 says: hello"},
	                   broker::message{"1a", "node1 says: hi"},
	                   broker::message{"1a", "node1 says: bye"}
	               })
	);

	BROKER_TEST(
	check_contents(pq_b1, {
	                   broker::message{"0b", "node0 says: bye"},
	                   broker::message{"0b", "node0 says: goodbye"},
	                   broker::message{"1b", "node1 says: bbye"}
	               })
	);

	BROKER_TEST(
	check_contents_poll(pq_a2, {
	                   broker::message{"0a", "node0 says: hi"},
	                   broker::message{"0a", "node0 says: hello"},
	                   broker::message{"2a", "node2 says: hi"},
	                   broker::message{"2a", "node2 says: bye"}
	               })
	);

	return BROKER_TEST_RESULT();
	}
