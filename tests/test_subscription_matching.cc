#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/print_queue.hh"
#include "testsuite.hh"
#include <vector>
#include <set>

using namespace std;
using namespace broker;

static bool check_contents_ordered(const print_queue& pq,
                           vector<print_msg> expected)
	{
	vector<print_msg> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.push_back(move(msg));

	return actual == expected;
	}

static bool check_contents_unordered(const print_queue& pq,
                                     multiset<print_msg> expected)
	{
	multiset<print_msg> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.insert(move(msg));

	return actual == expected;
	}

static void test_prefix_matching()
	{
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
	check_contents_ordered(pq_topic_a0, {
	               print_msg{"/", "hello"},
	               print_msg{"/", "goodbye"},
	               print_msg{"aaa", "hi"},
	               }));
	BROKER_TEST(
	check_contents_ordered(pq_topic_b0, {
	               print_msg{"bbb", "bye"},
	               }));
	BROKER_TEST(
	check_contents_ordered(pq_topic_0, {
	               print_msg{"/", "hello"},
	               print_msg{"/", "goodbye"},
	               print_msg{"aaa", "hi"},
	               print_msg{"bbb", "bye"},
	               print_msg{"topicpath", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_ordered(pq_all_0, {
	               print_msg{"/", "hello"},
	               print_msg{"/", "grats"},
	               print_msg{"/", "goodbye"},
	               print_msg{"aaa", "hi"},
	               print_msg{"you're", "greedy"},
	               print_msg{"bbb", "bye"},
	               print_msg{"tpath", "tmsg"},
	               print_msg{"topicpath", "topicmsg"},
	               }));
	}

static void test_without_access_control()
	{
	endpoint node0("node0");
	print_queue pq_topic_a0("topic_a", node0);
	print_queue pq_topic_b0("topic_b", node0);
	print_queue pq_topic_0("topic", node0);
	print_queue pq_all_0("", node0);

	endpoint node1("node1");
	print_queue pq_topic_a1("topic_a", node1);
	print_queue pq_topic_1("topic", node1);
	print_queue pq_hal_1("hal", node1);

	endpoint node2("node2");
	print_queue pq_topic_b2("topic_b", node2);
	print_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.peer_status().need_pop();
	node1.peer_status().need_pop();

	node0.print("topic_a", {"0", "hi"});
	node0.print("topic_b", {"0", "bye"});
	node0.print("t", {"0", "tmsg"});
	node0.print("topic", {"0", "topicmsg"});
	node0.print("hal9000", {"0", "What are you doing, Dave?"});

	node1.print("topic_a", {"1", "can't think of anything"});
	node1.print("topic_b", {"1", "still can't think of anything"});
	node1.print("t", {"1", "tmsg"});
	node1.print("topic", {"1", "topicmsg"});
	node1.print("hal9000", {"1", "My mind is going."});

	node2.print("topic_a", {"2", "goodbye"});
	node2.print("topic_b", {"2", "hello"});
	node2.print("t", {"2", "tmsg"});
	node2.print("topic", {"2", "topicmsg"});
	node2.print("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               print_msg{"0", "bye"},
	               print_msg{"1", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "tmsg"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "tmsg"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "My mind is going."},
	               print_msg{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	}

static void test_restricted_publish()
	{
	endpoint node0("node0");
	print_queue pq_topic_a0("topic_a", node0);
	print_queue pq_topic_b0("topic_b", node0);
	print_queue pq_topic_0("topic", node0);
	print_queue pq_all_0("", node0);

	endpoint node1("node1", AUTO_SUBSCRIBE);
	print_queue pq_topic_a1("topic_a", node1);
	print_queue pq_topic_1("topic", node1);
	print_queue pq_hal_1("hal", node1);
	node1.publish({"topic", topic::tag::print});
	node1.publish({"hal9000", topic::tag::print});

	endpoint node2("node2");
	print_queue pq_topic_b2("topic_b", node2);
	print_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.peer_status().need_pop();
	node1.peer_status().need_pop();

	node0.print("topic_a", {"0", "hi"});
	node0.print("topic_b", {"0", "bye"});
	node0.print("t", {"0", "tmsg"});
	node0.print("topic", {"0", "topicmsg"});
	node0.print("hal9000", {"0", "What are you doing, Dave?"});

	node1.print("topic_a", {"1", "can't think of anything"});
	node1.print("topic_b", {"1", "still can't think of anything"});
	node1.print("t", {"1", "tmsg"});
	node1.print("topic", {"1", "topicmsg"});
	node1.print("hal9000", {"1", "My mind is going."});

	node2.print("topic_a", {"2", "goodbye"});
	node2.print("topic_b", {"2", "hello"});
	node2.print("t", {"2", "tmsg"});
	node2.print("topic", {"2", "topicmsg"});
	node2.print("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               print_msg{"0", "hi"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               print_msg{"0", "bye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "tmsg"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "My mind is going."},
	               print_msg{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               print_msg{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	}

static void test_restricted_subscribe()
	{
	endpoint node0("node0");
	print_queue pq_topic_a0("topic_a", node0);
	print_queue pq_topic_b0("topic_b", node0);
	print_queue pq_topic_0("topic", node0);
	print_queue pq_all_0("", node0);

	endpoint node1("node1", AUTO_PUBLISH);
	node1.subscribe({"topic_a", topic::tag::print});
	print_queue pq_topic_a1("topic_a", node1);
	print_queue pq_topic_1("topic", node1);
	print_queue pq_hal_1("hal", node1);
	node1.subscribe({"hal", topic::tag::print});

	endpoint node2("node2");
	print_queue pq_topic_b2("topic_b", node2);
	print_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.peer_status().need_pop();
	node1.peer_status().need_pop();

	node0.print("topic_a", {"0", "hi"});
	node0.print("topic_b", {"0", "bye"});
	node0.print("t", {"0", "tmsg"});
	node0.print("topic", {"0", "topicmsg"});
	node0.print("hal9000", {"0", "What are you doing, Dave?"});

	node1.print("topic_a", {"1", "can't think of anything"});
	node1.print("topic_b", {"1", "still can't think of anything"});
	node1.print("t", {"1", "tmsg"});
	node1.print("topic", {"1", "topicmsg"});
	node1.print("hal9000", {"1", "My mind is going."});

	node2.print("topic_a", {"2", "goodbye"});
	node2.print("topic_b", {"2", "hello"});
	node2.print("t", {"2", "tmsg"});
	node2.print("topic", {"2", "topicmsg"});
	node2.print("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               print_msg{"0", "bye"},
	               print_msg{"1", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "tmsg"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "tmsg"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "My mind is going."},
	               print_msg{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	}

static void test_send_self_only()
	{
	endpoint node0("node0");
	print_queue pq_topic_a0("topic_a", node0);
	print_queue pq_topic_b0("topic_b", node0);
	print_queue pq_topic_0("topic", node0);
	print_queue pq_all_0("", node0);

	endpoint node1("node1");
	print_queue pq_topic_a1("topic_a", node1);
	print_queue pq_topic_1("topic", node1);
	print_queue pq_hal_1("hal", node1);

	endpoint node2("node2");
	print_queue pq_topic_b2("topic_b", node2);
	print_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.peer_status().need_pop();
	node1.peer_status().need_pop();

	node0.print("topic_a", {"0", "hi"});
	node0.print("topic_b", {"0", "bye"});
	node0.print("t", {"0", "tmsg"});
	node0.print("topic", {"0", "topicmsg"});
	node0.print("hal9000", {"0", "What are you doing, Dave?"});

	node1.print("topic_a", {"1", "can't think of anything"}, SELF);
	node1.print("topic_b", {"1", "still can't think of anything"}, SELF);
	node1.print("t", {"1", "tmsg"}, SELF);
	node1.print("topic", {"1", "topicmsg"}, SELF);
	node1.print("hal9000", {"1", "My mind is going."}, SELF);

	node2.print("topic_a", {"2", "goodbye"});
	node2.print("topic_b", {"2", "hello"});
	node2.print("t", {"2", "tmsg"});
	node2.print("topic", {"2", "topicmsg"});
	node2.print("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               print_msg{"0", "hi"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               print_msg{"0", "bye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "tmsg"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"0", "What are you doing, Dave?"},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "My mind is going."},
	               print_msg{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               print_msg{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	}

static void test_send_peers_only()
	{
	endpoint node0("node0");
	print_queue pq_topic_a0("topic_a", node0);
	print_queue pq_topic_b0("topic_b", node0);
	print_queue pq_topic_0("topic", node0);
	print_queue pq_all_0("", node0);

	endpoint node1("node1");
	print_queue pq_topic_a1("topic_a", node1);
	print_queue pq_topic_1("topic", node1);
	print_queue pq_hal_1("hal", node1);

	endpoint node2("node2");
	print_queue pq_topic_b2("topic_b", node2);
	print_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.peer_status().need_pop();
	node1.peer_status().need_pop();

	node0.print("topic_a", {"0", "hi"});
	node0.print("topic_b", {"0", "bye"});
	node0.print("t", {"0", "tmsg"});
	node0.print("topic", {"0", "topicmsg"});
	node0.print("hal9000", {"0", "What are you doing, Dave?"});

	node1.print("topic_a", {"1", "can't think of anything"}, PEERS);
	node1.print("topic_b", {"1", "still can't think of anything"}, PEERS);
	node1.print("t", {"1", "tmsg"}, PEERS);
	node1.print("topic", {"1", "topicmsg"}, PEERS);
	node1.print("hal9000", {"1", "My mind is going."}, PEERS);

	node2.print("topic_a", {"2", "goodbye"});
	node2.print("topic_b", {"2", "hello"});
	node2.print("t", {"2", "tmsg"});
	node2.print("topic", {"2", "topicmsg"});
	node2.print("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               print_msg{"0", "bye"},
	               print_msg{"1", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "tmsg"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "tmsg"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               print_msg{"0", "hi"},
	               print_msg{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	}

static void test_send_unsolicited()
	{
	endpoint node0("node0");
	print_queue pq_topic_a0("topic_a", node0);
	print_queue pq_topic_b0("topic_b", node0);
	print_queue pq_topic_0("topic", node0);
	print_queue pq_all_0("", node0);

	endpoint node1("node1");
	print_queue pq_topic_a1("topic_a", node1);
	print_queue pq_topic_1("topic", node1);
	print_queue pq_hal_1("hal", node1);

	endpoint node2("node2", AUTO_PUBLISH);
	print_queue pq_topic_b2("topic_b", node2);
	print_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.peer_status().need_pop();
	node1.peer_status().need_pop();

	node0.print("topic_a", {"0", "hi"});
	node0.print("topic_b", {"0", "bye"});
	node0.print("t", {"0", "tmsg"});
	node0.print("topic", {"0", "topicmsg"});
	node0.print("hal9000", {"0", "What are you doing, Dave?"});

	node1.print("topic_a", {"1", "can't think of anything"});
	node1.print("topic_b", {"1", "still can't think of anything"});
	node1.print("t", {"1", "tmsg"});
	node1.print("topic", {"1", "topicmsg"});
	node1.print("hal9000", {"1", "My mind is going."});

	node1.print("topic_a", {"1u", "can't think of anything"},
	            SELF|PEERS|UNSOLICITED);
	node1.print("topic_b", {"1u", "still can't think of anything"},
	            SELF|PEERS|UNSOLICITED);
	node1.print("t", {"1u", "tmsg"}, SELF|PEERS|UNSOLICITED);
	node1.print("topic", {"1u", "topicmsg"}, SELF|PEERS|UNSOLICITED);
	node1.print("hal9000", {"1u", "My mind is going."}, SELF|PEERS|UNSOLICITED);

	node2.print("topic_a", {"2", "goodbye"});
	node2.print("topic_b", {"2", "hello"});
	node2.print("t", {"2", "tmsg"});
	node2.print("topic", {"2", "topicmsg"});
	node2.print("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1u", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               print_msg{"0", "bye"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1u", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"1u", "can't think of anything"},
	               print_msg{"1u", "still can't think of anything"},
	               print_msg{"1u", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "tmsg"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "tmsg"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"1", "My mind is going."},
	               print_msg{"1u", "can't think of anything"},
	               print_msg{"1u", "still can't think of anything"},
	               print_msg{"1u", "tmsg"},
	               print_msg{"1u", "topicmsg"},
	               print_msg{"1u", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               print_msg{"0", "hi"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1u", "can't think of anything"},
	               print_msg{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               print_msg{"0", "hi"},
	               print_msg{"0", "bye"},
	               print_msg{"0", "topicmsg"},
	               print_msg{"1", "can't think of anything"},
	               print_msg{"1", "still can't think of anything"},
	               print_msg{"1", "topicmsg"},
	               print_msg{"1u", "can't think of anything"},
	               print_msg{"1u", "still can't think of anything"},
	               print_msg{"1u", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               print_msg{"0", "What are you doing, Dave?"},
	               print_msg{"1", "My mind is going."},
	               print_msg{"1u", "My mind is going."},
	               print_msg{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               print_msg{"1u", "still can't think of anything"},
	               print_msg{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               print_msg{"1u", "can't think of anything"},
	               print_msg{"1u", "still can't think of anything"},
	               print_msg{"1u", "topicmsg"},
	               print_msg{"2", "goodbye"},
	               print_msg{"2", "hello"},
	               print_msg{"2", "topicmsg"},
	               }));
	}

int main(int argc, char** argv)
	{
	broker::init();
	test_prefix_matching();
	test_without_access_control();
	test_restricted_publish();
	test_restricted_subscribe();
	test_send_self_only();
	test_send_peers_only();
	test_send_unsolicited();
	broker::done();
	return BROKER_TEST_RESULT();
	}
