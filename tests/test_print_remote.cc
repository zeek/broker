#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/print_queue.hh"
#include "testsuite.hh"
#include <iostream>
#include <set>

using namespace std;

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

	node1.peer("127.0.0.1", 9999);

	if ( node1.peer_status().need_pop().front().status !=
	     broker::peer_status::type::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node0.print("topic_a", {"0a", "hi"});
	node0.print("topic_c", {"0c", "greetings"});
	node1.print("topic_b", {"1b", "hello"});
	node1.print("topic_c", {"1c", "well met"});
	node0.print("topic_b", {"0b", "bye"});

	check_contents(pq0a, { broker::print_msg{"0a", "hi"} });
	check_contents(pq0c, { broker::print_msg{"0c", "greetings"},
	                       broker::print_msg{"1c", "well met" } });
	check_contents(pq1b, { broker::print_msg{"1b", "hello"},
	                       broker::print_msg{"0b", "bye" } });
	check_contents(pq1c, { broker::print_msg{"0c", "greetings"},
	                       broker::print_msg{"1c", "well met"} });

	broker::done();
	return BROKER_TEST_RESULT();
	}
