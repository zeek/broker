#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "testsuite.h"
#include <vector>
#include <set>

using namespace std;
using namespace broker;

static bool check_contents_ordered(const message_queue& pq,
                                   std::vector<message> expected)
	{
	std::vector<message> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.push_back(move(msg));

	return actual == expected;
	}

static bool check_contents_unordered(const message_queue& pq,
                                     multiset<message> expected)
	{
	multiset<message> actual;

	while ( actual.size() < expected.size() )
		for ( auto& msg : pq.need_pop() )
			actual.insert(move(msg));

	return actual == expected;
	}

static void test_prefix_matching()
	{
	endpoint node0("node0");
	message_queue pq_topic_a0("topic_a", node0);
	message_queue pq_topic_b0("topic_b", node0);
	message_queue pq_topic_0("topic", node0);
	message_queue pq_all_0("", node0);

	node0.send("topic_a", {"/", "hello"});
	node0.send("very specific", {"/", "grats"});
	node0.send("topic_a", {"/", "goodbye"});

	node0.send("topic_a", {"aaa", "hi"});
	node0.send("even more specific !!1!1!!", {"you're", "greedy"});
	node0.send("topic_b", {"bbb", "bye"});

	node0.send("t", {"tpath", "tmsg"});
	node0.send("topic", {"topicpath", "topicmsg"});

	BROKER_TEST(
	check_contents_ordered(pq_topic_a0, {
	               message{"/", "hello"},
	               message{"/", "goodbye"},
	               message{"aaa", "hi"},
	               }));
	message msg = {"bbb", "bye"};
	std::vector<message> expect;
	expect.emplace_back(msg);
	BROKER_TEST(
	check_contents_ordered(pq_topic_b0, expect));
	BROKER_TEST(
	check_contents_ordered(pq_topic_0, {
	               message{"/", "hello"},
	               message{"/", "goodbye"},
	               message{"aaa", "hi"},
	               message{"bbb", "bye"},
	               message{"topicpath", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_ordered(pq_all_0, {
	               message{"/", "hello"},
	               message{"/", "grats"},
	               message{"/", "goodbye"},
	               message{"aaa", "hi"},
	               message{"you're", "greedy"},
	               message{"bbb", "bye"},
	               message{"tpath", "tmsg"},
	               message{"topicpath", "topicmsg"},
	               }));
	}

static void test_without_access_control()
	{
	endpoint node0("node0");
	message_queue pq_topic_a0("topic_a", node0);
	message_queue pq_topic_b0("topic_b", node0);
	message_queue pq_topic_0("topic", node0);
	message_queue pq_all_0("", node0);

	endpoint node1("node1");
	message_queue pq_topic_a1("topic_a", node1);
	message_queue pq_topic_1("topic", node1);
	message_queue pq_hal_1("hal", node1);

	endpoint node2("node2");
	message_queue pq_topic_b2("topic_b", node2);
	message_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.outgoing_connection_status().need_pop();
	node1.outgoing_connection_status().need_pop();

	node0.send("topic_a", {"0", "hi"});
	node0.send("topic_b", {"0", "bye"});
	node0.send("t", {"0", "tmsg"});
	node0.send("topic", {"0", "topicmsg"});
	node0.send("hal9000", {"0", "What are you doing, Dave?"});

	node1.send("topic_a", {"1", "can't think of anything"});
	node1.send("topic_b", {"1", "still can't think of anything"});
	node1.send("t", {"1", "tmsg"});
	node1.send("topic", {"1", "topicmsg"});
	node1.send("hal9000", {"1", "My mind is going."});

	node2.send("topic_a", {"2", "goodbye"});
	node2.send("topic_b", {"2", "hello"});
	node2.send("t", {"2", "tmsg"});
	node2.send("topic", {"2", "topicmsg"});
	node2.send("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               message{"0", "bye"},
	               message{"1", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "tmsg"},
	               message{"0", "topicmsg"},
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "tmsg"},
	               message{"1", "topicmsg"},
	               message{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               message{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "My mind is going."},
	               message{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               message{"1", "still can't think of anything"},
	               message{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	}

static void test_restricted_publish()
	{
	endpoint node0("node0");
	message_queue pq_topic_a0("topic_a", node0);
	message_queue pq_topic_b0("topic_b", node0);
	message_queue pq_topic_0("topic", node0);
	message_queue pq_all_0("", node0);

	endpoint node1("node1", AUTO_ADVERTISE);
	message_queue pq_topic_a1("topic_a", node1);
	message_queue pq_topic_1("topic", node1);
	message_queue pq_hal_1("hal", node1);
	node1.publish("topic");
	node1.publish("hal9000");

	endpoint node2("node2");
	message_queue pq_topic_b2("topic_b", node2);
	message_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.outgoing_connection_status().need_pop();
	node1.outgoing_connection_status().need_pop();

	node0.send("topic_a", {"0", "hi"});
	node0.send("topic_b", {"0", "bye"});
	node0.send("t", {"0", "tmsg"});
	node0.send("topic", {"0", "topicmsg"});
	node0.send("hal9000", {"0", "What are you doing, Dave?"});

	node1.send("topic_a", {"1", "can't think of anything"});
	node1.send("topic_b", {"1", "still can't think of anything"});
	node1.send("t", {"1", "tmsg"});
	node1.send("topic", {"1", "topicmsg"});
	node1.send("hal9000", {"1", "My mind is going."});

	node2.send("topic_a", {"2", "goodbye"});
	node2.send("topic_b", {"2", "hello"});
	node2.send("t", {"2", "tmsg"});
	node2.send("topic", {"2", "topicmsg"});
	node2.send("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               message{"0", "hi"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               message{"0", "bye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "tmsg"},
	               message{"0", "topicmsg"},
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "topicmsg"},
	               message{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               message{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "My mind is going."},
	               message{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               message{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	}

static void test_restricted_subscribe()
	{
	endpoint node0("node0");
	message_queue pq_topic_a0("topic_a", node0);
	message_queue pq_topic_b0("topic_b", node0);
	message_queue pq_topic_0("topic", node0);
	message_queue pq_all_0("", node0);

	endpoint node1("node1", AUTO_PUBLISH);
	node1.advertise("topic_a");
	message_queue pq_topic_a1("topic_a", node1);
	message_queue pq_topic_1("topic", node1);
	message_queue pq_hal_1("hal", node1);
	node1.advertise("hal");

	endpoint node2("node2");
	message_queue pq_topic_b2("topic_b", node2);
	message_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.outgoing_connection_status().need_pop();
	node1.outgoing_connection_status().need_pop();

	node0.send("topic_a", {"0", "hi"});
	node0.send("topic_b", {"0", "bye"});
	node0.send("t", {"0", "tmsg"});
	node0.send("topic", {"0", "topicmsg"});
	node0.send("hal9000", {"0", "What are you doing, Dave?"});

	node1.send("topic_a", {"1", "can't think of anything"});
	node1.send("topic_b", {"1", "still can't think of anything"});
	node1.send("t", {"1", "tmsg"});
	node1.send("topic", {"1", "topicmsg"});
	node1.send("hal9000", {"1", "My mind is going."});

	node2.send("topic_a", {"2", "goodbye"});
	node2.send("topic_b", {"2", "hello"});
	node2.send("t", {"2", "tmsg"});
	node2.send("topic", {"2", "topicmsg"});
	node2.send("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               message{"0", "bye"},
	               message{"1", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "tmsg"},
	               message{"0", "topicmsg"},
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "tmsg"},
	               message{"1", "topicmsg"},
	               message{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               message{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "My mind is going."},
	               message{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               message{"1", "still can't think of anything"},
	               message{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	}

static void test_send_self_only()
	{
	endpoint node0("node0");
	message_queue pq_topic_a0("topic_a", node0);
	message_queue pq_topic_b0("topic_b", node0);
	message_queue pq_topic_0("topic", node0);
	message_queue pq_all_0("", node0);

	endpoint node1("node1");
	message_queue pq_topic_a1("topic_a", node1);
	message_queue pq_topic_1("topic", node1);
	message_queue pq_hal_1("hal", node1);

	endpoint node2("node2");
	message_queue pq_topic_b2("topic_b", node2);
	message_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.outgoing_connection_status().need_pop();
	node1.outgoing_connection_status().need_pop();

	node0.send("topic_a", {"0", "hi"});
	node0.send("topic_b", {"0", "bye"});
	node0.send("t", {"0", "tmsg"});
	node0.send("topic", {"0", "topicmsg"});
	node0.send("hal9000", {"0", "What are you doing, Dave?"});

	node1.send("topic_a", {"1", "can't think of anything"}, SELF);
	node1.send("topic_b", {"1", "still can't think of anything"}, SELF);
	node1.send("t", {"1", "tmsg"}, SELF);
	node1.send("topic", {"1", "topicmsg"}, SELF);
	node1.send("hal9000", {"1", "My mind is going."}, SELF);

	node2.send("topic_a", {"2", "goodbye"});
	node2.send("topic_b", {"2", "hello"});
	node2.send("t", {"2", "tmsg"});
	node2.send("topic", {"2", "topicmsg"});
	node2.send("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               message{"0", "hi"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               message{"0", "bye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "tmsg"},
	               message{"0", "topicmsg"},
	               message{"0", "What are you doing, Dave?"},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               message{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "My mind is going."},
	               message{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               message{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	}

static void test_send_peers_only()
	{
	endpoint node0("node0");
	message_queue pq_topic_a0("topic_a", node0);
	message_queue pq_topic_b0("topic_b", node0);
	message_queue pq_topic_0("topic", node0);
	message_queue pq_all_0("", node0);

	endpoint node1("node1");
	message_queue pq_topic_a1("topic_a", node1);
	message_queue pq_topic_1("topic", node1);
	message_queue pq_hal_1("hal", node1);

	endpoint node2("node2");
	message_queue pq_topic_b2("topic_b", node2);
	message_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.outgoing_connection_status().need_pop();
	node1.outgoing_connection_status().need_pop();

	node0.send("topic_a", {"0", "hi"});
	node0.send("topic_b", {"0", "bye"});
	node0.send("t", {"0", "tmsg"});
	node0.send("topic", {"0", "topicmsg"});
	node0.send("hal9000", {"0", "What are you doing, Dave?"});

	node1.send("topic_a", {"1", "can't think of anything"}, PEERS);
	node1.send("topic_b", {"1", "still can't think of anything"}, PEERS);
	node1.send("t", {"1", "tmsg"}, PEERS);
	node1.send("topic", {"1", "topicmsg"}, PEERS);
	node1.send("hal9000", {"1", "My mind is going."}, PEERS);

	node2.send("topic_a", {"2", "goodbye"});
	node2.send("topic_b", {"2", "hello"});
	node2.send("t", {"2", "tmsg"});
	node2.send("topic", {"2", "topicmsg"});
	node2.send("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               message{"0", "bye"},
	               message{"1", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "tmsg"},
	               message{"0", "topicmsg"},
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "tmsg"},
	               message{"1", "topicmsg"},
	               message{"1", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               message{"0", "hi"},
	               message{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               message{"0", "What are you doing, Dave?"},
	               message{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               message{"1", "still can't think of anything"},
	               message{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	}

static void test_send_unsolicited()
	{
	endpoint node0("node0");
	message_queue pq_topic_a0("topic_a", node0);
	message_queue pq_topic_b0("topic_b", node0);
	message_queue pq_topic_0("topic", node0);
	message_queue pq_all_0("", node0);

	endpoint node1("node1");
	message_queue pq_topic_a1("topic_a", node1);
	message_queue pq_topic_1("topic", node1);
	message_queue pq_hal_1("hal", node1);

	endpoint node2("node2", AUTO_PUBLISH);
	message_queue pq_topic_b2("topic_b", node2);
	message_queue pq_topic_2("topic", node2);

	node0.peer(node1);
	node1.peer(node2);

	node0.outgoing_connection_status().need_pop();
	node1.outgoing_connection_status().need_pop();

	node0.send("topic_a", {"0", "hi"});
	node0.send("topic_b", {"0", "bye"});
	node0.send("t", {"0", "tmsg"});
	node0.send("topic", {"0", "topicmsg"});
	node0.send("hal9000", {"0", "What are you doing, Dave?"});

	node1.send("topic_a", {"1", "can't think of anything"});
	node1.send("topic_b", {"1", "still can't think of anything"});
	node1.send("t", {"1", "tmsg"});
	node1.send("topic", {"1", "topicmsg"});
	node1.send("hal9000", {"1", "My mind is going."});

	node1.send("topic_a", {"1u", "can't think of anything"},
	            SELF|PEERS|UNSOLICITED);
	node1.send("topic_b", {"1u", "still can't think of anything"},
	            SELF|PEERS|UNSOLICITED);
	node1.send("t", {"1u", "tmsg"}, SELF|PEERS|UNSOLICITED);
	node1.send("topic", {"1u", "topicmsg"}, SELF|PEERS|UNSOLICITED);
	node1.send("hal9000", {"1u", "My mind is going."}, SELF|PEERS|UNSOLICITED);

	node2.send("topic_a", {"2", "goodbye"});
	node2.send("topic_b", {"2", "hello"});
	node2.send("t", {"2", "tmsg"});
	node2.send("topic", {"2", "topicmsg"});
	node2.send("hal9000", {"2", "I can feel it."});

	BROKER_TEST(
	check_contents_unordered(pq_topic_a0, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               message{"1u", "can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_b0, {
	               message{"0", "bye"},
	               message{"1", "still can't think of anything"},
	               message{"1u", "still can't think of anything"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"1u", "can't think of anything"},
	               message{"1u", "still can't think of anything"},
	               message{"1u", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_all_0, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "tmsg"},
	               message{"0", "topicmsg"},
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "tmsg"},
	               message{"1", "topicmsg"},
	               message{"1", "My mind is going."},
	               message{"1u", "can't think of anything"},
	               message{"1u", "still can't think of anything"},
	               message{"1u", "tmsg"},
	               message{"1u", "topicmsg"},
	               message{"1u", "My mind is going."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_a1, {
	               message{"0", "hi"},
	               message{"1", "can't think of anything"},
	               message{"1u", "can't think of anything"},
	               message{"2", "goodbye"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_1, {
	               message{"0", "hi"},
	               message{"0", "bye"},
	               message{"0", "topicmsg"},
	               message{"1", "can't think of anything"},
	               message{"1", "still can't think of anything"},
	               message{"1", "topicmsg"},
	               message{"1u", "can't think of anything"},
	               message{"1u", "still can't think of anything"},
	               message{"1u", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_hal_1, {
	               message{"0", "What are you doing, Dave?"},
	               message{"1", "My mind is going."},
	               message{"1u", "My mind is going."},
	               message{"2", "I can feel it."},
	               }));

	BROKER_TEST(
	check_contents_unordered(pq_topic_b2, {
	               message{"1u", "still can't think of anything"},
	               message{"2", "hello"},
	               }));
	BROKER_TEST(
	check_contents_unordered(pq_topic_2, {
	               message{"1u", "can't think of anything"},
	               message{"1u", "still can't think of anything"},
	               message{"1u", "topicmsg"},
	               message{"2", "goodbye"},
	               message{"2", "hello"},
	               message{"2", "topicmsg"},
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
	return BROKER_TEST_RESULT();
	}
