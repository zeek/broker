#include "broker/broker.h"
#include "testsuite.h"
#include <poll.h>

typedef struct test_message {
	broker_endpoint* node;
	broker_string* topic;
	broker_message* msg;
} test_message;

broker_data* make_test_msg(broker_endpoint* node, const char* path, const char* str,
                   broker_string* topic, broker_vector* msg, test_message* tmsg)
	{
	BROKER_TEST(broker_string_set_cstring(
	            broker_data_as_string(broker_vector_lookup(msg, 0)),
	            path));
	BROKER_TEST(broker_string_set_cstring(
	            broker_data_as_string(broker_vector_lookup(msg, 1)),
	            str));

	tmsg->node = node;
	tmsg->topic = topic;
	tmsg->msg = broker_vector_copy(msg);
	return broker_data_from_vector(msg);
	}

int check_contents_poll(broker_message_queue* q, broker_set* expected)
	{
	broker_set* actual = broker_set_create();
	struct pollfd pfd = {broker_message_queue_fd(q), POLLIN, 0};

	while ( broker_set_size(actual) < broker_set_size(expected) )
		{
		poll(&pfd, 1, -1);

		broker_deque_of_message* msgs = broker_message_queue_need_pop(q);
		int n = broker_deque_of_message_size(msgs);
		int i;

		for ( i = 0; i < n; ++i )
			{
			broker_message* m = broker_deque_of_message_at(msgs, i);
			broker_data* a = broker_data_from_vector(m);
			BROKER_TEST(broker_set_insert(actual, a));
			}

		broker_deque_of_message_delete(msgs);
		}

	return broker_set_eq(actual, expected);
	}

int main()
	{
	BROKER_TEST(broker_init(0) == 0);

	broker_endpoint* node0 = broker_endpoint_create("node0");
	broker_string* topic_a = broker_string_create("topic_a");
	broker_string* topic_b = broker_string_create("topic_b");
	broker_string* topic_c = broker_string_create("topic_c");
	broker_message_queue* pq0a = broker_message_queue_create(topic_a, node0);
	broker_message_queue* pq0c = broker_message_queue_create(topic_c, node0);

	if ( ! broker_endpoint_listen(node0, 9999, "127.0.0.1", 1) )
		{
		fprintf(stderr, "%s\n", broker_endpoint_last_error(node0));
		return 1;
		}

	broker_endpoint* node1 = broker_endpoint_create("node1");
	broker_message_queue* pq1b = broker_message_queue_create(topic_b, node1);
	broker_message_queue* pq1c = broker_message_queue_create(topic_c, node1);

	broker_endpoint_peer_remotely(node1, "127.0.0.1", 9999, .5);

	broker_deque_of_outgoing_connection_status* d =
	        broker_outgoing_connection_status_queue_need_pop(
	            broker_endpoint_outgoing_connection_status(node1));

	broker_outgoing_connection_status_tag t =
	        broker_outgoing_connection_status_get(
	            broker_deque_of_outgoing_connection_status_at(d, 0));

	if ( t != broker_outgoing_connection_status_tag_established )
		{
		BROKER_TEST(0);
		return 1;
		}

	broker_string* str = broker_string_create("");
	broker_message* msg = broker_vector_create();
	BROKER_TEST(broker_vector_insert(msg, broker_data_from_string(str), 0));
	BROKER_TEST(broker_vector_insert(msg, broker_data_from_string(str), 1));
	broker_set* pq0a_expected = broker_set_create();
	broker_set* pq0c_expected = broker_set_create();
	broker_set* pq1b_expected = broker_set_create();
	broker_set* pq1c_expected = broker_set_create();

	test_message msgs[5];
	int off = 0;
	broker_data* msg_data;

	msg_data = make_test_msg(node0, "0a", "hi", topic_a, msg, msgs + off++);
	BROKER_TEST(broker_set_insert(pq0a_expected, msg_data));
	msg_data = make_test_msg(node0, "0c", "lazy", topic_c, msg, msgs + off++);
	BROKER_TEST(broker_set_insert(pq0c_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq1c_expected, msg_data));
	msg_data = make_test_msg(node1, "1b", "hello", topic_b, msg, msgs + off++);
	BROKER_TEST(broker_set_insert(pq1b_expected, msg_data));
	msg_data = make_test_msg(node1, "1c", "well met", topic_c, msg, msgs + off++);
	BROKER_TEST(broker_set_insert(pq0c_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq1c_expected, msg_data));
	msg_data = make_test_msg(node0, "0b", "bye", topic_b, msg, msgs + off++);
	BROKER_TEST(broker_set_insert(pq1b_expected, msg_data));
	int i;

	for ( i = 0; i < sizeof(msgs) / sizeof(test_message); ++i )
		BROKER_TEST(broker_endpoint_send(msgs[i].node, msgs[i].topic, msgs[i].msg));

	BROKER_TEST(check_contents_poll(pq0a, pq0a_expected));
	BROKER_TEST(check_contents_poll(pq0c, pq0c_expected));
	BROKER_TEST(check_contents_poll(pq1b, pq1b_expected));
	BROKER_TEST(check_contents_poll(pq1c, pq1c_expected));

	return BROKER_TEST_RESULT();
	}
