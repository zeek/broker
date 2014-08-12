#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/print_queue.hh"
#include "testsuite.hh"
#include <iostream>
#include <set>

using namespace std;

static void check_contents(broker::print_queue& pq, set<string> expected)
	{
	set<string> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.insert(move(msg));

	BROKER_TEST(actual == expected);
	}

int main(int argc, char** argv)
	{
	broker::init();

	broker::endpoint node0("node0");
	broker::print_queue pq0a("topic_a", node0);
	broker::print_queue pq0c("topic_c", node0);

	if ( ! node0.listen(9999, "127.0.0.1") )
		{
		cerr << node0.last_error() << endl;
		return 1;
		}

	broker::endpoint node1("node1");
	broker::print_queue pq1b("topic_b", node1);
	broker::print_queue pq1c("topic_c", node1);

	node1.peer("127.0.0.1", 9999).handshake();

	node0.print("topic_a", "hi");
	node0.print("topic_c", "greetings");
	node1.print("topic_b", "hello");
	node1.print("topic_c", "well met");
	node0.print("topic_b", "bye");

	check_contents(pq0a, { "hi" });
	check_contents(pq0c, { "greetings", "well met" });
	check_contents(pq1b, { "hello", "bye" });
	check_contents(pq1c, { "greetings", "well met" });

	broker::done();
	return BROKER_TEST_RESULT();
	}
