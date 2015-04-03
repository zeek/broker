#include "broker/broker.h"
#include "testsuite.h"
#include "string.h"

void test_bool()
	{
	broker_data* a = broker_data_create();
	broker_data* b = broker_data_from_bool(1);
	broker_data* c = broker_data_copy(b);
	BROKER_TEST(broker_data_which(a) == broker_data_type_bool);
	BROKER_TEST(broker_data_which(b) == broker_data_type_bool);
	BROKER_TEST(broker_data_which(c) == broker_data_type_bool);
	BROKER_TEST(! broker_bool_true(broker_data_as_bool(a)));
	BROKER_TEST(broker_bool_true(broker_data_as_bool(b)));
	broker_bool_set(broker_data_as_bool(a), 1);
	BROKER_TEST(broker_bool_true(broker_data_as_bool(a)));
	broker_data_delete(a);
	broker_data_delete(b);
	broker_data_delete(c);
	}

void test_count()
	{
	broker_data* a = broker_data_from_count(0);
	broker_data* b = broker_data_from_count(123456789);
	broker_data* c = broker_data_copy(b);
	BROKER_TEST(broker_data_which(a) == broker_data_type_count);
	BROKER_TEST(broker_data_which(b) == broker_data_type_count);
	BROKER_TEST(broker_data_which(c) == broker_data_type_count);
	BROKER_TEST(*broker_data_as_count(a) == 0);
	BROKER_TEST(*broker_data_as_count(b) == 123456789);
	BROKER_TEST(broker_data_eq(b, c));
	BROKER_TEST(! broker_data_lt(b, c));
	BROKER_TEST(broker_data_lt(a, b));
	*broker_data_as_count(c) = 7;
	BROKER_TEST(! broker_data_eq(b, c));
	BROKER_TEST(*broker_data_as_count(c) == 7);
	broker_data_delete(a);
	broker_data_delete(b);
	broker_data_delete(c);
	}

void test_integer()
	{
	broker_data* a = broker_data_from_integer(0);
	broker_data* b = broker_data_from_integer(-123456789);
	broker_data* c = broker_data_copy(b);
	BROKER_TEST(broker_data_which(a) == broker_data_type_integer);
	BROKER_TEST(broker_data_which(b) == broker_data_type_integer);
	BROKER_TEST(broker_data_which(c) == broker_data_type_integer);
	BROKER_TEST(*broker_data_as_integer(a) == 0);
	BROKER_TEST(*broker_data_as_integer(b) == -123456789);
	BROKER_TEST(broker_data_eq(b, c));
	BROKER_TEST(! broker_data_lt(b, c));
	BROKER_TEST(broker_data_lt(b, a));
	*broker_data_as_integer(c) = -7;
	BROKER_TEST(! broker_data_eq(b, c));
	BROKER_TEST(*broker_data_as_integer(c) == -7);
	broker_data_delete(a);
	broker_data_delete(b);
	broker_data_delete(c);
	}

void test_real()
	{
	broker_data* a = broker_data_from_real(0.1);
	broker_data* b = broker_data_from_real(-123456789);
	broker_data* c = broker_data_copy(b);
	BROKER_TEST(broker_data_which(a) == broker_data_type_real);
	BROKER_TEST(broker_data_which(b) == broker_data_type_real);
	BROKER_TEST(broker_data_which(c) == broker_data_type_real);
	BROKER_TEST(*broker_data_as_real(a) == 0.1);
	BROKER_TEST(*broker_data_as_real(b) == -123456789);
	BROKER_TEST(broker_data_eq(b, c));
	BROKER_TEST(! broker_data_lt(b, c));
	BROKER_TEST(broker_data_lt(b, a));
	*broker_data_as_real(c) = -7;
	BROKER_TEST(! broker_data_eq(b, c));
	BROKER_TEST(*broker_data_as_real(c) == -7);
	broker_data_delete(a);
	broker_data_delete(b);
	broker_data_delete(c);
	}

void test_string()
	{
	broker_string* a = broker_string_create("");
	broker_string* b = broker_string_create("hello, world");
	broker_string* c = broker_string_copy(b);
	broker_string* d = broker_string_from_data("1234567890", 5);
	BROKER_TEST(broker_string_size(a) == 0);
	BROKER_TEST(broker_string_size(b) == 12);
	BROKER_TEST(broker_string_size(c) == 12);
	BROKER_TEST(broker_string_size(d) == 5);
	BROKER_TEST(broker_string_eq(b, c));
	BROKER_TEST(! broker_string_eq(c, d));
	BROKER_TEST(broker_string_set_cstring(c, "aaaaa"));
	BROKER_TEST(broker_string_lt(c, b));
	BROKER_TEST(broker_string_set_cstring(c, "zzzzz"));
	BROKER_TEST(broker_string_lt(b, c));

	broker_data* db = broker_data_from_string(b);
	broker_data* dd = broker_data_from_string(d);
	BROKER_TEST(broker_data_which(db) == broker_data_type_string);
	BROKER_TEST(broker_data_which(dd) == broker_data_type_string);
	BROKER_TEST(! broker_data_eq(db, dd));
	BROKER_TEST(broker_string_set_cstring(broker_data_as_string(dd),
	                                      "hello, world"));
	BROKER_TEST(broker_data_eq(db, dd));
	BROKER_TEST(! strcmp(broker_string_data(broker_data_as_string(dd)),
	                     "hello, world"));
	BROKER_TEST(broker_string_data(d)[3] == '4');

	broker_string_delete(a);
	broker_string_delete(b);
	broker_string_delete(c);
	broker_string_delete(d);
	broker_data_delete(db);
	broker_data_delete(dd);
	}

void test_time()
	{
	broker_time_point* a = broker_time_point_create(0);
	broker_time_point* b = broker_time_point_create(123.456);
	broker_time_point* c = broker_time_point_copy(b);
	broker_time_point* n = broker_time_point_now();
	BROKER_TEST(broker_time_point_eq(b, c));
	BROKER_TEST(! broker_time_point_eq(a, c));
	BROKER_TEST(broker_time_point_lt(b, n));
	BROKER_TEST(! broker_time_point_lt(c, b));
	broker_time_point_set(c, 456.789);
	BROKER_TEST(broker_time_point_lt(b, c));
	BROKER_TEST(broker_time_point_value(c) == 456.789);

	broker_data* db = broker_data_from_time(b);
	broker_data* dc = broker_data_from_time(c);
	BROKER_TEST(broker_data_which(db) == broker_data_type_time);
	BROKER_TEST(broker_data_which(dc) == broker_data_type_time);
	BROKER_TEST(! broker_data_eq(db, dc));
	broker_time_point_set(broker_data_as_time(db), 456.789);
	BROKER_TEST(broker_data_eq(db, dc));
	BROKER_TEST(broker_time_point_value(broker_data_as_time(dc)) == 456.789);

	broker_time_point_delete(a);
	broker_time_point_delete(b);
	broker_time_point_delete(c);
	broker_time_point_delete(n);
	broker_data_delete(db);
	broker_data_delete(dc);
	}

void test_duration()
	{
	broker_time_duration* a = broker_time_duration_create(0);
	broker_time_duration* b = broker_time_duration_create(123.456);
	broker_time_duration* c = broker_time_duration_copy(b);
	BROKER_TEST(broker_time_duration_eq(b, c));
	BROKER_TEST(! broker_time_duration_eq(a, c));
	BROKER_TEST(! broker_time_duration_lt(c, b));
	broker_time_duration_set(c, 456.789);
	BROKER_TEST(broker_time_duration_lt(b, c));
	BROKER_TEST(broker_time_duration_value(c) == 456.789);

	broker_data* db = broker_data_from_duration(b);
	broker_data* dc = broker_data_from_duration(c);
	BROKER_TEST(broker_data_which(db) == broker_data_type_duration);
	BROKER_TEST(broker_data_which(dc) == broker_data_type_duration);
	BROKER_TEST(! broker_data_eq(db, dc));
	broker_time_duration_set(broker_data_as_duration(db), 456.789);
	BROKER_TEST(broker_data_eq(db, dc));
	BROKER_TEST(broker_time_duration_value(broker_data_as_duration(dc)) ==
	            456.789);

	broker_time_duration_delete(a);
	broker_time_duration_delete(b);
	broker_time_duration_delete(c);
	broker_data_delete(db);
	broker_data_delete(dc);
	}

void test_enum()
	{
	broker_enum_value* a = broker_enum_value_create("hello");
	broker_enum_value* b = broker_enum_value_create("world");
	broker_enum_value* c = broker_enum_value_copy(b);
	BROKER_TEST(broker_enum_value_eq(b, c));
	BROKER_TEST(! broker_enum_value_eq(a, c));
	BROKER_TEST(broker_enum_value_lt(a, b));
	BROKER_TEST(! strcmp(broker_enum_value_name(a), "hello"));
	BROKER_TEST(broker_enum_value_set(c, "the ultimate bioweapon"));
	BROKER_TEST(! broker_enum_value_eq(b, c));
	BROKER_TEST(! strcmp(broker_enum_value_name(c), "the ultimate bioweapon"));

	broker_data* da = broker_data_from_enum(a);
	broker_data* db = broker_data_from_enum(b);
	BROKER_TEST(broker_data_which(da) == broker_data_type_enum_value);
	BROKER_TEST(broker_data_which(db) == broker_data_type_enum_value);
	BROKER_TEST(! broker_data_eq(da, db));
	BROKER_TEST(broker_data_lt(da, db));
	BROKER_TEST(broker_enum_value_set(broker_data_as_enum(da), "world"));
	BROKER_TEST(! strcmp(broker_enum_value_name(broker_data_as_enum(da)),
	                     "world"));
	BROKER_TEST(! strcmp(broker_enum_value_name(a), "hello"));

	broker_enum_value_delete(a);
	broker_enum_value_delete(b);
	broker_enum_value_delete(c);
	broker_data_delete(da);
	broker_data_delete(db);
	}

void test_set()
	{
	broker_set* a = broker_set_create();
	broker_data* zero = broker_data_from_count(0);
	broker_data* one = broker_data_from_count(1);
	broker_data* two = broker_data_from_count(2);
	BROKER_TEST(broker_set_size(a) == 0);
	BROKER_TEST(broker_set_insert(a, zero));
	BROKER_TEST(! broker_set_insert(a, zero));
	BROKER_TEST(broker_set_size(a) == 1);
	BROKER_TEST(broker_set_insert(a, one));
	BROKER_TEST(broker_set_insert(a, two));
	BROKER_TEST(broker_set_size(a) == 3);
	BROKER_TEST(broker_set_remove(a, one));
	BROKER_TEST(! broker_set_remove(a, one));
	BROKER_TEST(broker_set_contains(a, zero));
	BROKER_TEST(! broker_set_contains(a, one));
	BROKER_TEST(broker_set_contains(a, two));
	BROKER_TEST(broker_set_insert(a, one));
	BROKER_TEST(broker_set_contains(a, one));

	broker_set* b = broker_set_copy(a);
	broker_set_clear(a);
	BROKER_TEST(broker_set_size(a) == 0);
	broker_set_iterator* it = broker_set_iterator_create(b);
	uint64_t i = 0;

	while ( ! broker_set_iterator_at_last(b, it) )
		{
		BROKER_TEST(i ==
		            *broker_data_as_count_const(broker_set_iterator_value(it)));
		++i;
		broker_set_iterator_next(b, it);
		}

	BROKER_TEST(i == 3);
	broker_data_delete(zero);
	broker_data_delete(one);
	broker_data_delete(two);
	broker_set_delete(a);
	broker_set_delete(b);
	}

void test_table()
	{
	broker_table* a = broker_table_create();
	broker_data* zero = broker_data_from_count(0);
	broker_data* one = broker_data_from_count(1);
	broker_data* two = broker_data_from_count(2);
	broker_data* izero = broker_data_from_integer(0);
	broker_data* ione = broker_data_from_integer(-1);
	broker_data* itwo = broker_data_from_integer(-2);
	BROKER_TEST(broker_table_size(a) == 0);
	BROKER_TEST(broker_table_insert(a, zero, izero));
	BROKER_TEST(! broker_table_insert(a, zero, ione));
	BROKER_TEST(broker_table_size(a) == 1);
	BROKER_TEST(broker_table_insert(a, one, ione));
	BROKER_TEST(broker_table_insert(a, two, itwo));
	BROKER_TEST(broker_table_size(a) == 3);
	BROKER_TEST(broker_table_remove(a, one));
	BROKER_TEST(! broker_table_lookup(a, one));
	BROKER_TEST(! broker_table_remove(a, one));
	BROKER_TEST(broker_table_contains(a, zero));
	BROKER_TEST(! broker_table_contains(a, one));
	BROKER_TEST(broker_table_contains(a, two));
	BROKER_TEST(broker_table_insert(a, one, ione));
	BROKER_TEST(broker_table_contains(a, one));

	broker_table* b = broker_table_copy(a);
	broker_table_clear(a);
	BROKER_TEST(broker_table_size(a) == 0);
	broker_table_iterator* it = broker_table_iterator_create(b);
	uint64_t c = 0;
	int64_t i = 0;

	while ( ! broker_table_iterator_at_last(b, it) )
		{
		broker_data* k = broker_data_from_count(c);
		BROKER_TEST(*broker_data_as_count(k) == c);
		broker_table_entry e = broker_table_iterator_value(it);
		broker_data* v = broker_table_lookup(b, k);
		BROKER_TEST(broker_data_eq(e.key, k));
		BROKER_TEST(*broker_data_as_integer(v) == i);
		BROKER_TEST(*broker_data_as_integer(e.val) == i);
		--i;
		++c;
		broker_table_iterator_next(b, it);
		broker_data_delete(k);
		}

	BROKER_TEST(c == 3);
	broker_data_delete(zero);
	broker_data_delete(one);
	broker_data_delete(two);
	broker_data_delete(izero);
	broker_data_delete(ione);
	broker_data_delete(itwo);
	broker_table_delete(a);
	broker_table_delete(b);
	}

void test_vector()
	{
	broker_vector* a = broker_vector_create();
	broker_data* zero = broker_data_from_count(0);
	broker_data* one = broker_data_from_count(1);
	broker_data* two = broker_data_from_count(2);
	BROKER_TEST(broker_vector_size(a) == 0);
	BROKER_TEST(broker_vector_insert(a, zero, 0));
	BROKER_TEST(broker_vector_size(a) == 1);
	BROKER_TEST(broker_vector_insert(a, one, 1));
	BROKER_TEST(broker_vector_insert(a, two, 2));
	BROKER_TEST(broker_vector_size(a) == 3);
	BROKER_TEST(broker_vector_remove(a, 1));
	BROKER_TEST(broker_vector_size(a) == 2);
	BROKER_TEST(broker_vector_insert(a, two, 1));
	BROKER_TEST(broker_vector_replace(a, one, 1));

	broker_vector* b = broker_vector_copy(a);
	broker_vector_clear(a);
	BROKER_TEST(broker_vector_size(a) == 0);
	broker_vector_iterator* it = broker_vector_iterator_create(b);
	uint64_t i = 0;

	while ( ! broker_vector_iterator_at_last(b, it) )
		{
		broker_data* v = broker_vector_iterator_value(it);
		BROKER_TEST(i == *broker_data_as_count(v));
		BROKER_TEST(broker_data_eq(broker_vector_lookup(b, i), v));
		++i;
		broker_vector_iterator_next(b, it);
		}

	BROKER_TEST(i == 3);
	broker_data_delete(zero);
	broker_data_delete(one);
	broker_data_delete(two);
	broker_vector_delete(a);
	broker_vector_delete(b);
	}

void test_record()
	{
	broker_record* a = broker_record_create(3);
	broker_data* zero = broker_data_from_count(0);
	broker_data* one = broker_data_from_count(1);
	broker_data* two = broker_data_from_count(2);
	BROKER_TEST(broker_record_size(a) == 3);
	BROKER_TEST(! broker_record_lookup(a, 0));
	BROKER_TEST(! broker_record_lookup(a, 1));
	BROKER_TEST(! broker_record_lookup(a, 2));
	BROKER_TEST(broker_record_assign(a, zero, 0));
	BROKER_TEST(broker_record_size(a) == 3);
	BROKER_TEST(broker_record_assign(a, two, 1));
	BROKER_TEST(broker_record_assign(a, two, 2));
	BROKER_TEST(broker_record_size(a) == 3);
	BROKER_TEST(broker_record_assign(a, one, 1));

	broker_record* b = broker_record_copy(a);
	broker_record_iterator* it = broker_record_iterator_create(b);
	uint64_t i = 0;

	while ( ! broker_record_iterator_at_last(b, it) )
		{
		broker_data* v = broker_record_iterator_value(it);
		BROKER_TEST(i == *broker_data_as_count(v));
		BROKER_TEST(broker_data_eq(broker_record_lookup(b, i), v));
		++i;
		broker_record_iterator_next(b, it);
		}

	BROKER_TEST(i == 3);
	broker_data_delete(zero);
	broker_data_delete(one);
	broker_data_delete(two);
	broker_record_delete(a);
	broker_record_delete(b);
	}

int main()
	{
	test_bool();
	test_count();
	test_integer();
	test_real();
	test_string();
	// network types (address, subnet, port) have their own test
	test_time();
	test_duration();
	test_enum();
	test_set();
	test_table();
	test_vector();
	test_record();
	return BROKER_TEST_RESULT();
	}
