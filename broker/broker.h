#ifndef BROKER_BROKER_H
#define BROKER_BROKER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// TODO: automate how version levels are set here.
#define BROKER_VERSION_MAJOR 0
#define BROKER_VERSION_MINOR 1
#define BROKER_VERSION_PATCH 0

/**
 * The version of the broker messaging protocol.  Endpoints can only
 * exchange messages if they use the same version.
 */
const int BROKER_PROTOCOL_VERSION = 0;

/**
 * Initialize the broker library.  This should be called once before using
 * anything else that's provided by the library.
 * @param flags tune behavior of the library.
 * @return 0 if library is initialized, else an error code that can
 *         be supplied to broker_strerror().
 */
int broker_init(int flags);

/**
 * Shutdown the broker library.  No functionality provided by the library
 * is guaranteed to work after the call, not even destructors of broker-related
 * objects on the stack, so be careful of that.  Note that it's not required
 * to call this at all if the application just intends to exit.
 */
void broker_done();

/**
 * @return a textual representation of a broker error code.
 */
const char* broker_strerror(int broker_errno);

/**
 * Reentrant version of broker::strerror().
 */
void broker_strerror_r(int broker_errno, char* buf, size_t buflen);

// TODO: document all the C API

typedef enum {
	broker_data_type_bool,
	broker_data_type_count,   // uint64_t
	broker_data_type_integer, // int64_t
	broker_data_type_real,    // double
	broker_data_type_string,
	broker_data_type_address,
	broker_data_type_subnet,
	broker_data_type_port,
	broker_data_type_time,
	broker_data_type_duration,
	broker_data_type_enum_value,
	broker_data_type_set,
	broker_data_type_table,
	broker_data_type_vector,
	broker_data_type_record,
} broker_data_type;

struct broker_data;
typedef struct broker_data broker_data;

struct broker_string;
typedef struct broker_string broker_string;

struct broker_address;
typedef struct broker_address broker_address;

struct broker_subnet;
typedef struct broker_subnet broker_subnet;

typedef enum {
	broker_port_protocol_unknown,
	broker_port_protocol_tcp,
	broker_port_protocol_udp,
	broker_port_protocol_icmp,
} broker_port_protocol;

struct broker_port;
typedef struct broker_port broker_port;

struct broker_time_point;
typedef struct broker_time_point broker_time_point;

struct broker_time_duration;
typedef struct broker_time_duration broker_time_duration;

struct broker_enum_value;
typedef struct broker_enum_value broker_enum_value;

struct broker_set;
typedef struct broker_set broker_set;
struct broker_set_iterator;
typedef struct broker_set_iterator broker_set_iterator;

struct broker_table;
typedef struct broker_table broker_table;
struct broker_table_iterator;
typedef struct broker_table_iterator broker_table_iterator;

typedef struct {
	const broker_data* key;
	broker_data* val;
} broker_table_entry;

struct broker_vector;
typedef struct broker_vector broker_vector;
struct broker_vector_iterator;
typedef struct broker_vector_iterator broker_vector_iterator;

struct broker_record;
typedef struct broker_record broker_record;
struct broker_record_iterator;
typedef struct broker_record_iterator broker_record_iterator;

struct broker_bool;
typedef struct broker_bool broker_bool;

int broker_bool_true(const broker_bool* b);
void broker_bool_set(broker_bool* b, int true_or_false);

broker_data* broker_data_create();
void broker_data_delete(broker_data* d);
broker_data* broker_data_copy(const broker_data* d);
broker_data_type broker_data_which(const broker_data* d);
broker_string* broker_data_to_string(const broker_data* d);
int broker_data_eq(const broker_data* a, const broker_data* b);
int broker_data_lt(const broker_data* a, const broker_data* b);
size_t broker_data_hash(const broker_data* d);
broker_data* broker_data_from_bool(int);
broker_data* broker_data_from_count(uint64_t);
broker_data* broker_data_from_integer(int64_t);
broker_data* broker_data_from_real(double);
broker_data* broker_data_from_string(const broker_string*);
broker_data* broker_data_from_address(const broker_address*);
broker_data* broker_data_from_subnet(const broker_subnet*);
broker_data* broker_data_from_port(const broker_port*);
broker_data* broker_data_from_time(const broker_time_point*);
broker_data* broker_data_from_duration(const broker_time_duration*);
broker_data* broker_data_from_enum(const broker_enum_value*);
broker_data* broker_data_from_set(const broker_set*);
broker_data* broker_data_from_table(const broker_table*);
broker_data* broker_data_from_vector(const broker_vector*);
broker_data* broker_data_from_record(const broker_record*);
broker_bool* broker_data_as_bool(broker_data*);
uint64_t* broker_data_as_count(broker_data*);
int64_t* broker_data_as_integer(broker_data*);
double* broker_data_as_real(broker_data*);
broker_string* broker_data_as_string(broker_data*);
broker_address* broker_data_as_address(broker_data*);
broker_subnet* broker_data_as_subnet(broker_data*);
broker_port* broker_data_as_port(broker_data*);
broker_time_point* broker_data_as_time(broker_data*);
broker_time_duration* broker_data_as_duration(broker_data*);
broker_enum_value* broker_data_as_enum(broker_data*);
broker_set* broker_data_as_set(broker_data*);
broker_table* broker_data_as_table(broker_data*);
broker_vector* broker_data_as_vector(broker_data*);
broker_record* broker_data_as_record(broker_data*);
const broker_bool* broker_data_as_bool_const(const broker_data*);
const uint64_t* broker_data_as_count_const(const broker_data*);
const int64_t* broker_data_as_integer_const(const broker_data*);
const double* broker_data_as_real_const(const broker_data*);
const broker_string* broker_data_as_string_const(const broker_data*);
const broker_address* broker_data_as_address_const(const broker_data*);
const broker_subnet* broker_data_as_subnet_const(const broker_data*);
const broker_port* broker_data_as_port_const(const broker_data*);
const broker_time_point* broker_data_as_time_const(const broker_data*);
const broker_time_duration* broker_data_as_duration_const(const broker_data*);
const broker_enum_value* broker_data_as_enum_const(const broker_data*);
const broker_set* broker_data_as_set_const(const broker_data*);
const broker_table* broker_data_as_table_const(const broker_data*);
const broker_vector* broker_data_as_vector_const(const broker_data*);
const broker_record* broker_data_as_record_const(const broker_data*);

broker_string* broker_string_create(const char* cstring);
void broker_string_delete(broker_string* s);
broker_string* broker_string_copy(const broker_string* s);
broker_string* broker_string_from_data(const char* s, size_t len);
int broker_string_set_cstring(broker_string* s, const char* cstr);
int broker_string_set_data(broker_string* s, const char* data, size_t len);
const char* broker_string_data(const broker_string* s);
size_t broker_string_size(const broker_string* s);
int broker_string_eq(const broker_string* a, const broker_string* b);
int broker_string_lt(const broker_string* a, const broker_string* b);
size_t broker_string_hash(const broker_string* s);

broker_address* broker_address_create();
void broker_address_delete(broker_address* a);
broker_address* broker_address_copy(const broker_address* a);
int broker_address_from_string(broker_address** dst, const char* s);
broker_address* broker_address_from_v4_host_bytes(const uint32_t* bytes);
broker_address* broker_address_from_v4_network_bytes(const uint32_t* bytes);
broker_address* broker_address_from_v6_host_bytes(const uint32_t* bytes);
broker_address* broker_address_from_v6_network_bytes(const uint32_t* bytes);
void broker_address_set(broker_address* dst, broker_address* src);
int broker_address_mask(broker_address* a, uint8_t top_bits_to_keep);
int broker_address_is_v4(const broker_address* a);
int broker_address_is_v6(const broker_address* a);
const uint8_t* broker_address_bytes(const broker_address* a);
broker_string* broker_address_to_string(const broker_address* a);
int broker_address_eq(const broker_address* a, const broker_address* b);
int broker_address_lt(const broker_address* a, const broker_address* b);
size_t broker_address_hash(const broker_address* a);

broker_subnet* broker_subnet_create();
void broker_subnet_delete(broker_subnet* s);
broker_subnet* broker_subnet_copy(const broker_subnet* s);
broker_subnet* broker_subnet_from(const broker_address* a, uint8_t len);
void broker_subnet_set(broker_subnet* dst, broker_subnet* src);
const broker_address* broker_subnet_network(const broker_subnet* s);
uint8_t broker_subnet_length(const broker_subnet* s);
int broker_subnet_contains(const broker_subnet* s, const broker_address* a);
broker_string* broker_subnet_to_string(const broker_subnet* s);
int broker_subnet_eq(const broker_subnet* a, const broker_subnet* b);
int broker_subnet_lt(const broker_subnet* a, const broker_subnet* b);
size_t broker_subnet_hash(const broker_subnet* a);

broker_port* broker_port_create();
void broker_port_delete(broker_port* p);
broker_port* broker_port_copy(const broker_port* p);
broker_port* broker_port_from(uint16_t num, broker_port_protocol p);
void broker_port_set_number(broker_port* dst, uint16_t number);
void broker_port_set_protocol(broker_port* dst, broker_port_protocol p);
uint16_t broker_port_number(const broker_port* p);
broker_port_protocol broker_port_type(const broker_port* p);
broker_string* broker_port_to_string(const broker_port* p);
int broker_port_eq(const broker_port* a, const broker_port* b);
int broker_port_lt(const broker_port* a, const broker_port* b);
size_t broker_port_hash(const broker_port* p);

broker_time_point* broker_time_point_create(double seconds_from_epoch);
void broker_time_point_delete(broker_time_point* t);
broker_time_point* broker_time_point_copy(const broker_time_point* t);
broker_time_point* broker_time_point_now();
double broker_time_point_value(const broker_time_point* t);
void broker_time_point_set(broker_time_point* t, double seconds_from_epoch);
int broker_time_point_eq(const broker_time_point* a,
                         const broker_time_point* b);
int broker_time_point_lt(const broker_time_point* a,
                         const broker_time_point* b);
size_t broker_time_point_hash(const broker_time_point* p);

broker_time_duration* broker_time_duration_create(double seconds);
void broker_time_duration_delete(broker_time_duration* t);
broker_time_duration* broker_time_duration_copy(const broker_time_duration* t);
double broker_time_duration_value(const broker_time_duration* t);
void broker_time_duration_set(broker_time_duration* t, double seconds);
int broker_time_duration_eq(const broker_time_duration* a,
                            const broker_time_duration* b);
int broker_time_duration_lt(const broker_time_duration* a,
                            const broker_time_duration* b);
size_t broker_time_duration_hash(const broker_time_duration* p);

broker_enum_value* broker_enum_value_create(const char* name);
void broker_enum_value_delete(broker_enum_value* e);
broker_enum_value* broker_enum_value_copy(const broker_enum_value* e);
const char* broker_enum_value_name(const broker_enum_value* e);
int broker_enum_value_set(broker_enum_value* e, const char* name);
int broker_enum_value_eq(const broker_enum_value* a,
                         const broker_enum_value* b);
int broker_enum_value_lt(const broker_enum_value* a,
                         const broker_enum_value* b);
size_t broker_enum_value_hash(const broker_enum_value* p);

broker_set* broker_set_create();
void broker_set_delete(broker_set* s);
broker_set* broker_set_copy(const broker_set* s);
void broker_set_clear(broker_set* s);
size_t broker_set_size(const broker_set* s);
int broker_set_contains(const broker_set* s, const broker_data* key);
int broker_set_insert(broker_set* s, const broker_data* key);
int broker_set_remove(broker_set* s, const broker_data* key);
int broker_set_eq(const broker_set* a, const broker_set* b);
int broker_set_lt(const broker_set* a, const broker_set* b);
size_t broker_set_hash(const broker_set* a);
broker_set_iterator* broker_set_iterator_create(broker_set* s);
void broker_set_iterator_delete(broker_set_iterator* it);
int broker_set_iterator_at_last(broker_set* s, broker_set_iterator* it);
int broker_set_iterator_next(broker_set* s, broker_set_iterator* it);
const broker_data* broker_set_iterator_value(broker_set_iterator* it);

broker_table* broker_table_create();
void broker_table_delete(broker_table* t);
broker_table* broker_table_copy(const broker_table* t);
void broker_table_clear(broker_table* t);
size_t broker_table_size(const broker_table* t);
int broker_table_contains(const broker_table* t, const broker_data* key);
int broker_table_insert(broker_table* t, const broker_data* key,
                        const broker_data* value);
int broker_table_remove(broker_table* t, const broker_data* key);
broker_data* broker_table_lookup(broker_table* t, const broker_data* key);
int broker_table_eq(const broker_table* a, const broker_table* b);
int broker_table_lt(const broker_table* a, const broker_table* b);
size_t broker_table_hash(const broker_table* a);
broker_table_iterator* broker_table_iterator_create(broker_table* t);
void broker_table_iterator_delete(broker_table_iterator* it);
int broker_table_iterator_at_last(broker_table* t, broker_table_iterator* it);
int broker_table_iterator_next(broker_table* t, broker_table_iterator* it);
broker_table_entry broker_table_iterator_value(broker_table_iterator* it);

broker_vector* broker_vector_create();
void broker_vector_delete(broker_vector* v);
broker_vector* broker_vector_copy(const broker_vector* v);
void broker_vector_clear(broker_vector* v);
size_t broker_vector_size(const broker_vector* v);
int broker_vector_insert(broker_vector* v, const broker_data* d, size_t idx);
int broker_vector_replace(broker_vector* v, const broker_data* d, size_t idx);
int broker_vector_remove(broker_vector* v, size_t idx);
broker_data* broker_vector_lookup(broker_vector* v, size_t idx);
int broker_vector_eq(const broker_vector* a, const broker_vector* b);
int broker_vector_lt(const broker_vector* a, const broker_vector* b);
size_t broker_vector_hash(const broker_vector* a);
broker_vector_iterator* broker_vector_iterator_create(broker_vector* v);
void broker_vector_iterator_delete(broker_vector_iterator* it);
int broker_vector_iterator_at_last(broker_vector* v,
                                   broker_vector_iterator* it);
int broker_vector_iterator_next(broker_vector* v, broker_vector_iterator* it);
broker_data* broker_vector_iterator_value(broker_vector_iterator* it);

broker_record* broker_record_create(size_t size);
void broker_record_delete(broker_record* r);
broker_record* broker_record_copy(const broker_record* r);
size_t broker_record_size(const broker_record* r);
int broker_record_assign(broker_record* r, const broker_data* d, size_t idx);
broker_data* broker_record_lookup(broker_record* r, size_t idx);
int broker_record_eq(const broker_record* a, const broker_record* b);
int broker_record_lt(const broker_record* a, const broker_record* b);
size_t broker_record_hash(const broker_record* a);
broker_record_iterator* broker_record_iterator_create(broker_record* r);
void broker_record_iterator_delete(broker_record_iterator* it);
int broker_record_iterator_at_last(broker_record* r, broker_record_iterator* it);
int broker_record_iterator_next(broker_record* r, broker_record_iterator* it);
broker_data* broker_record_iterator_value(broker_record_iterator* it);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // BROKER_BROKER_H
