#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/print_queue.hh"
#include "testsuite.hh"
#include <vector>

using namespace std;
using namespace broker;

static bool check_contents(const print_queue& pq,
                           vector<print_msg> expected)
	{
	vector<print_msg> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.push_back(move(msg));

	return actual == expected;
	}

int main(int argc, char** argv)
	{
	broker::init();

	endpoint node0("node0");
	print_queue pq_topic_a0("topic_a", node0);
	print_queue pq_topic_b0("topic_b", node0);
	print_queue pq_topic_0("topic", node0);
	print_queue pq_all_0("", node0);

	node0.print("topic_a", {"/", "hello"});
	node0.print("very specific", {"/", "grats"});
	node0.print("topic_a", {"/", "goodbye"});

	node0.print("topic_a", {"aaa", "hi"});
	node0.print("even more specific !!1!1!!", {"you're", "greedy"});
	node0.print("topic_b", {"bbb", "bye"});

	node0.print("t", {"tpath", "tmsg"});
	node0.print("topic", {"topicpath", "topicmsg"});

	BROKER_TEST(
	check_contents(pq_topic_a0, {
	               print_msg{"/", "hello"},
	               print_msg{"/", "goodbye"},
	               print_msg{"aaa", "hi"},
	               }));
	BROKER_TEST(
	check_contents(pq_topic_b0, {
	               print_msg{"bbb", "bye"},
	               }));
	BROKER_TEST(
	check_contents(pq_topic_0, {
	               print_msg{"/", "hello"},
	               print_msg{"/", "goodbye"},
	               print_msg{"aaa", "hi"},
	               print_msg{"bbb", "bye"},
	               print_msg{"topicpath", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents(pq_all_0, {
	               print_msg{"/", "hello"},
	               print_msg{"/", "grats"},
	               print_msg{"/", "goodbye"},
	               print_msg{"aaa", "hi"},
	               print_msg{"you're", "greedy"},
	               print_msg{"bbb", "bye"},
	               print_msg{"tpath", "tmsg"},
	               print_msg{"topicpath", "topicmsg"},
	               }));

	broker::done();
	return BROKER_TEST_RESULT();
	}
