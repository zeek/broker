#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/print_queue.hh"
#include "testsuite.hh"
#include <vector>
#include <set>
#include <poll.h>

using namespace std;

static void check_contents_poll(broker::print_queue& pq,
                                set<broker::print_msg> expected)
	{
	set<broker::print_msg> actual;
	pollfd pfd{pq.fd(), POLLIN, 0};

	while ( actual.size() < expected.size() )
		{
		poll(&pfd, 1, -1);

		for ( auto& msg : pq.want_pop() )
			actual.insert(move(msg));
		}

	BROKER_TEST(actual == expected);
	}

static void check_contents(broker::print_queue& pq,
                           set<broker::print_msg> expected)
	{
	set<broker::print_msg> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.insert(move(msg));

	BROKER_TEST(actual == expected);
	}

int main(int argc, char** argv)
	{
	broker::init();

	broker::endpoint node0("node0");
	broker::print_queue pq_a0("topic_a", node0);

	node0.print("topic_a", {"/", "hello"});
	node0.print("nobody", {"/", "pointless"});
	node0.print("topic_a", {"/", "goodbye"});

	vector<broker::print_msg> node0_msgs;

	while ( node0_msgs.size() < 2 )
		for ( auto& msg : pq_a0.need_pop() )
			node0_msgs.push_back(move(msg));

	BROKER_TEST((node0_msgs[0] == broker::print_msg{"/", "hello"}));
	BROKER_TEST((node0_msgs[1] == broker::print_msg{"/", "goodbye"}));

	broker::print_queue pq_b0("topic_b", node0);
	node0.print("topic_a", {"aaa", "hi"});
	node0.print("nobody", {"", "pointless"});
	node0.print("topic_b", {"bbb", "bye"});

	BROKER_TEST((pq_b0.need_pop().front() == broker::print_msg{"bbb", "bye"}));
	BROKER_TEST((pq_a0.need_pop().front() == broker::print_msg{"aaa", "hi"}));

	broker::endpoint node1("node1");
	broker::endpoint node2("node2");
	broker::print_queue pq_a1("topic_a", node1);
	broker::print_queue pq_b1("topic_b", node1);
	broker::print_queue pq_a2("topic_a", node2);
	node0.peer(node1).handshake();
	node0.peer(node2).handshake();

	node0.print("topic_a", {"0a", node0.name() + " says: hi"}); // to 0, 1, 2
	node0.print("topic_a", {"0a", node0.name() + " says: hello"}); // to 0, 1, 2
	node0.print("nobody", {"", "pointless"});
	node0.print("topic_b", {"0b", node0.name() + " says: bye"}); // to 0, 1
	node0.print("topic_b", {"0b", node0.name() + " says: goodbye"}); // to 0, 1

	node1.print("topic_a", {"1a", node1.name() + " says: hi"}); // to 0, 1
	node1.print("topic_a", {"1a", node1.name() + " says: bye"}); // to 0, 1
	node1.print("topic_b", {"1b", node1.name() + " says: bbye"}); // to 0, 1

	node2.print("topic_a", {"2a", node2.name() + " says: hi"}); // to 0, 2
	node2.print("topic_a", {"2a", node2.name() + " says: bye"}); // to 0, 2
	node2.print("topic_b", {"2b", node2.name() + " says: bbye"}); // to 0

	check_contents_poll(pq_a0, {
	                   broker::print_msg{"0a", "node0 says: hi"},
	                   broker::print_msg{"0a", "node0 says: hello"},
	                   broker::print_msg{"1a", "node1 says: hi"},
	                   broker::print_msg{"1a", "node1 says: bye"},
	                   broker::print_msg{"2a", "node2 says: hi"},
	                   broker::print_msg{"2a", "node2 says: bye"}
	               });

	check_contents(pq_b0, {
	                   broker::print_msg{"0b", "node0 says: bye"},
	                   broker::print_msg{"0b", "node0 says: goodbye"},
	                   broker::print_msg{"1b", "node1 says: bbye"},
	                   broker::print_msg{"2b", "node2 says: bbye"}
	               });

	check_contents_poll(pq_a1, {
	                   broker::print_msg{"0a", "node0 says: hi"},
	                   broker::print_msg{"0a", "node0 says: hello"},
	                   broker::print_msg{"1a", "node1 says: hi"},
	                   broker::print_msg{"1a", "node1 says: bye"}
	               });

	check_contents(pq_b1, {
	                   broker::print_msg{"0b", "node0 says: bye"},
	                   broker::print_msg{"0b", "node0 says: goodbye"},
	                   broker::print_msg{"1b", "node1 says: bbye"}
	               });

	check_contents_poll(pq_a2, {
	                   broker::print_msg{"0a", "node0 says: hi"},
	                   broker::print_msg{"0a", "node0 says: hello"},
	                   broker::print_msg{"2a", "node2 says: hi"},
	                   broker::print_msg{"2a", "node2 says: bye"}
	               });

	broker::done();
	return BROKER_TEST_RESULT();
	}
