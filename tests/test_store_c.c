#include "broker/broker.h"
#include "testsuite.h"
#include <poll.h>
#include <unistd.h>

void wait_for(broker_store_frontend* f, broker_data* k, int exists)
	{
	broker_store_query* q =
	        broker_store_query_create(broker_store_query_tag_exists, k);

	for ( ; ; )
		{
		broker_store_result* res = broker_store_frontend_request_blocking(f, q);
		BROKER_TEST(broker_store_result_get_status(res) ==
		            broker_store_result_status_success);

		int e = broker_store_result_bool(res);
		broker_store_result_delete(res);

		if ( e == exists )
			break;

		usleep(1000);
		}

	broker_store_query_delete(q);
	}

int compare(broker_store_frontend* f, broker_table* expect)
	{
	broker_table* actual = broker_table_create();
	broker_store_query* key_query =
	        broker_store_query_create(broker_store_query_tag_keys, 0);
	broker_store_result* res = broker_store_frontend_request_blocking(f, key_query);
	BROKER_TEST(broker_store_result_get_status(res) ==
	            broker_store_result_status_success);
	const broker_vector* keys = broker_store_result_vector(res);
	broker_vector_const_iterator* it = broker_vector_const_iterator_create(keys);

	while ( ! broker_vector_const_iterator_at_last(keys, it) )
		{
		const broker_data* key = broker_vector_const_iterator_value(it);
		broker_store_query* lookup_query =
		        broker_store_query_create(broker_store_query_tag_lookup, key);
		broker_store_result* lookup_res =
		        broker_store_frontend_request_blocking(f, lookup_query);
		BROKER_TEST(broker_store_result_get_status(lookup_res) ==
	                broker_store_result_status_success);
		const broker_data* val = broker_store_result_data(lookup_res);
		BROKER_TEST(broker_table_insert(actual, key, val));
		broker_store_result_delete(lookup_res);
		broker_store_query_delete(lookup_query);
		broker_vector_const_iterator_next(keys, it);
		}

	broker_vector_const_iterator_delete(it);
	broker_store_result_delete(res);
	broker_store_query_delete(key_query);
	int rval = broker_table_eq(actual, expect);
	broker_table_delete(actual);
	return rval;
	}

typedef struct {
	broker_data* key;
	broker_data* val;
} entry;

int main()
	{
	BROKER_TEST(broker_init(0) == 0);
	broker_endpoint* node = broker_endpoint_create("node0");
	broker_string* master_name = broker_string_create("mystore");
	broker_store_frontend* m = broker_store_master_create_memory(node, master_name);

	broker_string* str = broker_string_create("");
	broker_message* msg = broker_vector_create();
	BROKER_TEST(broker_vector_insert(msg, broker_data_from_string(str), 0));
	BROKER_TEST(broker_vector_insert(msg, broker_data_from_string(str), 1));

	entry entries[5];
	entries[0].key = broker_data_from_string(broker_string_create("1"));
	entries[0].val = broker_data_from_string(broker_string_create("one"));
	entries[1].key = broker_data_from_string(broker_string_create("2"));
	entries[1].val = broker_data_from_string(broker_string_create("two"));
	entries[2].key = broker_data_from_string(broker_string_create("3"));
	entries[2].val = broker_data_from_string(broker_string_create("three"));
	entries[3].key = broker_data_from_string(broker_string_create("4"));
	entries[3].val = broker_data_from_string(broker_string_create("four"));
	entries[4].key = broker_data_from_string(broker_string_create("5"));
	entries[4].val = broker_data_from_string(broker_string_create("five"));

	broker_table* dataset = broker_table_create();
	int i;

	for ( i = 0; i < 3; ++i )
		{
		BROKER_TEST(broker_table_insert(dataset, entries[i].key, entries[i].val));
		BROKER_TEST(broker_store_frontend_insert(m, entries[i].key, entries[i].val));
		}

	broker_store_frontend* c = broker_store_clone_create_memory(node, master_name, .2);
	BROKER_TEST(broker_table_insert(dataset, entries[3].key, entries[3].val));
	BROKER_TEST(broker_store_frontend_insert(c, entries[3].key, entries[3].val));
	wait_for(c, entries[3].key, 1);

	BROKER_TEST(compare(c, dataset));
	BROKER_TEST(compare(m, dataset));

	BROKER_TEST(broker_table_insert(dataset, entries[4].key, entries[4].val));
	BROKER_TEST(broker_store_frontend_insert(m, entries[4].key, entries[4].val));
	wait_for(c, entries[4].key, 1);

	BROKER_TEST(compare(c, dataset));
	BROKER_TEST(compare(m, dataset));

	BROKER_TEST(broker_table_remove(dataset, entries[4].key));
	BROKER_TEST(broker_store_frontend_erase(c, entries[4].key));
	wait_for(c, entries[4].key, 0);

	BROKER_TEST(compare(c, dataset));
	BROKER_TEST(compare(m, dataset));

	broker_store_query* size_query =
	        broker_store_query_create(broker_store_query_tag_size, 0);
	broker_store_result* size_res =
	        broker_store_frontend_request_blocking(c, size_query);
	BROKER_TEST(broker_store_result_get_status(size_res) ==
	            broker_store_result_status_success);
	BROKER_TEST(broker_store_result_count(size_res) == broker_table_size(dataset));
	size_res = broker_store_frontend_request_blocking(m, size_query);
	BROKER_TEST(broker_store_result_get_status(size_res) ==
	            broker_store_result_status_success);
	BROKER_TEST(broker_store_result_count(size_res) == broker_table_size(dataset));

	broker_store_frontend* f = broker_store_frontend_create(node, master_name);
	broker_store_frontend_request_nonblocking(f, size_query, 60, (void*)13);

	broker_deque_of_store_response* d =
	  broker_store_response_queue_need_pop(broker_store_frontend_responses(f));
	broker_store_response* resp = broker_deque_of_store_response_at(d, 0);
	broker_store_result* res = broker_store_response_get_result(resp);
	void* cookie = broker_store_response_get_cookie(resp);
	BROKER_TEST(cookie = (void*)13);
	BROKER_TEST(broker_store_result_get_status(res) ==
	            broker_store_result_status_success);
	BROKER_TEST(broker_store_result_count(res) == broker_table_size(dataset));

	broker_store_frontend_clear(c);
	wait_for(c, entries[0].key, 0);

	size_res = broker_store_frontend_request_blocking(m, size_query);
	BROKER_TEST(broker_store_result_get_status(size_res) ==
	            broker_store_result_status_success);
	BROKER_TEST(broker_store_result_count(size_res) == 0);
	size_res = broker_store_frontend_request_blocking(c, size_query);
	BROKER_TEST(broker_store_result_get_status(size_res) ==
	            broker_store_result_status_success);
	BROKER_TEST(broker_store_result_count(size_res) == 0);

	return BROKER_TEST_RESULT();
	}
