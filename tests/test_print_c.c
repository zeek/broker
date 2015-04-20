#include "broker/broker.h"
#include "testsuite.h"
#include <poll.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>

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
		int rc = poll(&pfd, 1, -1);

		if ( rc < 0 )
			{
			fprintf(stderr, "poll() failure: %s\n", strerror(errno));
			exit(1);
			}

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
	broker_string* topic_nobody = broker_string_create("nobody");
	broker_message_queue* pq_a0 = broker_message_queue_create(topic_a, node0);

	broker_message* msg = broker_vector_create();
	broker_string* path = broker_string_create("/");
	broker_string* str_hello = broker_string_create("hello");
	broker_string* str_pointless = broker_string_create("pointless");
	broker_string* str_goodbye = broker_string_create("goodbye");
	broker_data* path_data = broker_data_from_string(path);
	broker_data* str_hello_data = broker_data_from_string(str_hello);
	broker_data* str_pointless_data = broker_data_from_string(str_pointless);
	broker_data* str_goodbye_data = broker_data_from_string(str_goodbye);
	BROKER_TEST(broker_vector_insert(msg, path_data, 0));
	BROKER_TEST(broker_vector_insert(msg, str_hello_data, 1));
	BROKER_TEST(broker_endpoint_send(node0, topic_a, msg));
	BROKER_TEST(broker_vector_replace(msg, str_pointless_data, 1));
	BROKER_TEST(broker_endpoint_send(node0, topic_nobody, msg));
	BROKER_TEST(broker_vector_replace(msg, str_goodbye_data, 1));
	BROKER_TEST(broker_endpoint_send(node0, topic_a, msg));

	int msgs_recvd = 0;

	while ( msgs_recvd < 2 )
		{
		broker_deque_of_message* node0_msgs = broker_message_queue_need_pop(pq_a0);
		int n = broker_deque_of_message_size(node0_msgs);
		int i;

		for ( i = 0; i < n; ++i )
			{
			++msgs_recvd;
			broker_message* m = broker_deque_of_message_at(node0_msgs, i);

			if ( msgs_recvd == 1 )
				BROKER_TEST(broker_vector_replace(msg, str_hello_data, 1));
			else
				BROKER_TEST(broker_vector_replace(msg, str_goodbye_data, 1));

			BROKER_TEST(broker_vector_eq(msg, m));
			}

		broker_deque_of_message_delete(node0_msgs);
		}

	BROKER_TEST(msgs_recvd == 2);

	broker_message_queue* pq_b0 = broker_message_queue_create(topic_b, node0);

	BROKER_TEST(broker_vector_replace(msg, str_hello_data, 1));
	BROKER_TEST(broker_endpoint_send(node0, topic_a, msg));
	BROKER_TEST(broker_vector_replace(msg, str_pointless_data, 1));
	BROKER_TEST(broker_endpoint_send(node0, topic_nobody, msg));
	BROKER_TEST(broker_vector_replace(msg, str_goodbye_data, 1));
	BROKER_TEST(broker_endpoint_send(node0, topic_b, msg));

	broker_deque_of_message* node0_msgs = broker_message_queue_need_pop(pq_a0);
	BROKER_TEST(broker_deque_of_message_size(node0_msgs) == 1);
	broker_message* m = broker_deque_of_message_at(node0_msgs, 0);
	BROKER_TEST(broker_vector_replace(msg, str_hello_data, 1));
	BROKER_TEST(broker_vector_eq(msg, m));
	broker_deque_of_message_delete(node0_msgs);

	node0_msgs = broker_message_queue_need_pop(pq_b0);
	BROKER_TEST(broker_deque_of_message_size(node0_msgs) == 1);
	m = broker_deque_of_message_at(node0_msgs, 0);
	BROKER_TEST(broker_vector_replace(msg, str_goodbye_data, 1));
	BROKER_TEST(broker_vector_eq(msg, m));
	broker_deque_of_message_delete(node0_msgs);

	broker_endpoint* node1 = broker_endpoint_create("node1");
	broker_endpoint* node2 = broker_endpoint_create("node2");
	broker_message_queue* pq_a1 = broker_message_queue_create(topic_a, node1);
	broker_message_queue* pq_b1 = broker_message_queue_create(topic_b, node1);
	broker_message_queue* pq_a2 = broker_message_queue_create(topic_a, node2);
	broker_peering* peer0to1 = broker_endpoint_peer_locally(node0, node1);
	broker_peering* peer0to2 = broker_endpoint_peer_locally(node0, node2);

	int connections = 0;
	const broker_outgoing_connection_status_queue* ocsq =
	        broker_endpoint_outgoing_connection_status(node0);

	while ( connections < 2 )
		{
		broker_deque_of_outgoing_connection_status* d =
		        broker_outgoing_connection_status_queue_need_pop(ocsq);
		connections += broker_deque_of_outgoing_connection_status_size(d);
		broker_deque_of_outgoing_connection_status_delete(d);
		}

	broker_string* str_0a = broker_string_create("0a");
	broker_string* str_0b = broker_string_create("0b");
	broker_string* str_1a = broker_string_create("1a");
	broker_string* str_1b = broker_string_create("1b");
	broker_string* str_2a = broker_string_create("2a");
	broker_string* str_2b = broker_string_create("2b");
	broker_data* str_0a_data = broker_data_from_string(str_0a);
	broker_data* str_0b_data = broker_data_from_string(str_0b);
	broker_data* str_1a_data = broker_data_from_string(str_1a);
	broker_data* str_1b_data = broker_data_from_string(str_1b);
	broker_data* str_2a_data = broker_data_from_string(str_2a);
	broker_data* str_2b_data = broker_data_from_string(str_2b);

	test_message msgs_to_send[11];
	broker_set* pq_a0_expected = broker_set_create();
	broker_set* pq_a1_expected = broker_set_create();
	broker_set* pq_a2_expected = broker_set_create();
	broker_set* pq_b0_expected = broker_set_create();
	broker_set* pq_b1_expected = broker_set_create();

	broker_data* msg_data;
	int off = 0;
	msg_data = make_test_msg(node0, "0a", "node0 says: hi", topic_a, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_a0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a1_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a2_expected, msg_data));
	msg_data = make_test_msg(node0, "0a", "node0 says: hello", topic_a, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_a0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a1_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a2_expected, msg_data));
	msg_data = make_test_msg(node0, "", "pointless", topic_nobody, msg, msgs_to_send + off++);
	msg_data = make_test_msg(node0, "0b", "node0 says: bye", topic_b, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_b0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_b1_expected, msg_data));
	msg_data = make_test_msg(node0, "0b", "node0 says: goodbye", topic_b, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_b0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_b1_expected, msg_data));

	msg_data = make_test_msg(node1, "1a", "node1 says: hi", topic_a, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_a0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a1_expected, msg_data));
	msg_data = make_test_msg(node1, "1a", "node1 says: bye", topic_a, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_a0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a1_expected, msg_data));
	msg_data = make_test_msg(node1, "1b", "node1 says: bbye", topic_b, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_b0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_b1_expected, msg_data));

	msg_data = make_test_msg(node2, "2a", "node2 says: hi", topic_a, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_a0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a2_expected, msg_data));
	msg_data = make_test_msg(node2, "2a", "node2 says: bye", topic_a, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_a0_expected, msg_data));
	BROKER_TEST(broker_set_insert(pq_a2_expected, msg_data));
	msg_data = make_test_msg(node2, "2b", "node2 says: bbye", topic_b, msg, msgs_to_send + off++);
	BROKER_TEST(broker_set_insert(pq_b0_expected, msg_data));
	int i;

	for ( i = 0; i < sizeof(msgs_to_send) / sizeof(test_message); ++i )
		BROKER_TEST(broker_endpoint_send(msgs_to_send[i].node, msgs_to_send[i].topic, msgs_to_send[i].msg));

	BROKER_TEST(check_contents_poll(pq_a0, pq_a0_expected));
	BROKER_TEST(check_contents_poll(pq_a1, pq_a1_expected));
	BROKER_TEST(check_contents_poll(pq_a2, pq_a2_expected));
	BROKER_TEST(check_contents_poll(pq_b0, pq_b0_expected));
	BROKER_TEST(check_contents_poll(pq_b1, pq_b1_expected));

	return BROKER_TEST_RESULT();
	}
