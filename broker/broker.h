#ifndef BROKER_BROKER_H
#define BROKER_BROKER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BROKER_VERSION "0.4-13"

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

/**
 * All data types supported by Broker's data model.
 */
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

/**
 * An opaque data value, which may be any of the types in @see broker_data_type.
 */
struct broker_data;
typedef struct broker_data broker_data;

/**
 * A string data value.
 */
struct broker_string;
typedef struct broker_string broker_string;

/**
 * An IPv4 or IPv6 address value.
 */
struct broker_address;
typedef struct broker_address broker_address;

/**
 * An IPv4 or IPv6 subnet value.
 */
struct broker_subnet;
typedef struct broker_subnet broker_subnet;

/**
 * All the transport protocols that port data values may use.
 */
typedef enum {
	broker_port_protocol_unknown,
	broker_port_protocol_tcp,
	broker_port_protocol_udp,
	broker_port_protocol_icmp,
} broker_port_protocol;

/**
 * A port value.
 */
struct broker_port;
typedef struct broker_port broker_port;

/**
 * A value representing an absolute point in time.
 */
struct broker_time_point;
typedef struct broker_time_point broker_time_point;

/**
 * A value representing an interval of time.
 */
struct broker_time_duration;
typedef struct broker_time_duration broker_time_duration;

/**
 * An value representing an enumeration (a string that the application maps
 * to an integer whose actual value is arbitrary).
 */
struct broker_enum_value;
typedef struct broker_enum_value broker_enum_value;

/**
 * An ordered set of data values.
 */
struct broker_set;
typedef struct broker_set broker_set;

/**
 * An iterator over a set which may be used to modify the set's elements.
 */
struct broker_set_iterator;
typedef struct broker_set_iterator broker_set_iterator;

/**
 * An iterator over a set which may only be used to inspect the set's elements,
 * but not modify them.
 */
struct broker_set_const_iterator;
typedef struct broker_set_const_iterator broker_set_const_iterator;

/**
 * An ordered, associative container that maps data keys to data values.
 */
struct broker_table;
typedef struct broker_table broker_table;

/**
 * An iterator over a table which may be used to modify the table's elements.
 */
struct broker_table_iterator;
typedef struct broker_table_iterator broker_table_iterator;

/**
 * An iterator over a table which may only be used to inspect the table's
 * elements, but not modify them.
 */
struct broker_table_const_iterator;
typedef struct broker_table_const_iterator broker_table_const_iterator;

/**
 * An entry in a table.  The key may not be modified (as it could invalidate
 * the current ordering of the table's contents), but the associated value  may.
 */
typedef struct {
	const broker_data* key;
	broker_data* val;
} broker_table_entry;

/**
 * An entry in a table which may only be used to inspect the key and associated
 * value, but not modify them.
 */
typedef struct {
	const broker_data* key;
	const broker_data* val;
} broker_const_table_entry;

/**
 * A sequence of data values.
 */
struct broker_vector;
typedef struct broker_vector broker_vector;

/**
 * An iterator over a vector which may be used to modify its elements.
 */
struct broker_vector_iterator;
typedef struct broker_vector_iterator broker_vector_iterator;

/**
 * An iterator over a vector which may only be used to inspect its contents,
 * but not modify them.
 */
struct broker_vector_const_iterator;
typedef struct broker_vector_const_iterator broker_vector_const_iterator;

/**
 * A sequence of data values, which may contain nil elements.
 */
struct broker_record;
typedef struct broker_record broker_record;

/**
 * An iterator over a record which may be used to modify its elements.
 */
struct broker_record_iterator;
typedef struct broker_record_iterator broker_record_iterator;

/**
 * An iterator over a record which may be used to inspect its contents, but
 * not modify them.
 */
struct broker_record_const_iterator;
typedef struct broker_record_const_iterator broker_record_const_iterator;

/**
 * A data value that is either true or false.
 */
struct broker_bool;
typedef struct broker_bool broker_bool;

/**
 * @return zero if the argument is the false boolean value, else non-zero.
 */
int broker_bool_true(const broker_bool* b);

/**
 * Modify a boolean data's value to be either true or false.
 * @param b the boolean data value to modify.
 * @param true_or_false pass in zero to set \a b to false, or not-zero to set
 * it to true.
 */
void broker_bool_set(broker_bool* b, int true_or_false);

/**
 * Create a new data value.
 * @return a new data value whose memory the caller must manage (e.g.
 * call @see broker_data_delete to reclaim).  Or null if sufficient
 * memory could not be allocated.
 */
broker_data* broker_data_create();

/**
 * Reclaim the memory allocated to a data value.
 * @param d the data value to reclaim.
 */
void broker_data_delete(broker_data* d);

/**
 * Make a copy of a data value.
 * @param d the data value to copy.
 * @return a new data value whose memory the caller must manage (e.g.
 * call @see broker_data_delete to reclaim).
 * Or null if sufficient memory could not be allocated.
 */
broker_data* broker_data_copy(const broker_data* d);

/**
 * @return the particular data type of the argument.
 */
broker_data_type broker_data_which(const broker_data* d);

/**
 * @return the argument converted to a human-readable string.  The caller
 * must manage the return value's memory (e.g. call @see broker_string_delete
 * to reclaim).
 */
broker_string* broker_data_to_string(const broker_data* d);

/**
 * @return not-zero if the two arguments are equal to each other, else zero.
 */
int broker_data_eq(const broker_data* a, const broker_data* b);

/**
 * @return not-zero if the first argument, \a a, is less than the second
 * argument, \a b, else zero.
 */
int broker_data_lt(const broker_data* a, const broker_data* b);

/**
 * @return the hash value for the argument.
 */
size_t broker_data_hash(const broker_data* d);

/**
 * Create new data value from a boolean.
 * @return new boolean data whose value is true if the argument is non-zero
 * or false if it is zero.  The caller must manage the memory of the returned
 * object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_bool(int);

/**
 * Create new data value from an unsigned 64-bit integer.
 * @return new count data whose value is the same as the argument.  The caller
 * must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_count(uint64_t);

/**
 * Create new data value from a signed 64-bit integer.
 * @return new integer data whose value is the same as the argument.  The caller
 * must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_integer(int64_t);

/**
 * Create new data value from a double-precision floating point number.
 * @return new real data whose value is the same as the argument.  The caller
 * must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_real(double);

/**
 * Create new data value from a string.
 * @return new string data whose value is copied from the argument.  The caller
 * must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_string(const broker_string*);

/**
 * Create new data value from an address.
 * @return new address data whose value is copied from the argument.  The caller
 * must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_address(const broker_address*);

/**
 * Create new data value from a subnet.
 * @return new subnet data whose value is copied from the argument.  The caller
 * must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_subnet(const broker_subnet*);

/**
 * Create new data value from a port.
 * @return new port data whose value is copied from the argument.  The caller
 * must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_port(const broker_port*);

/**
 * Create new data value from a time point.
 * @return new time point data whose value is copied from the argument.  The
 * caller must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_time(const broker_time_point*);

/**
 * Create new data value from a time duration.
 * @return new time duration data whose value is copied from the argument.  The
 * caller must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_duration(const broker_time_duration*);

/**
 * Create new data value from an enum.
 * @return new enum data whose value is copied from the argument.  The
 * caller must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_enum(const broker_enum_value*);

/**
 * Create new data value from a set.
 * @return new set data whose value is copied from the argument.  The
 * caller must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_set(const broker_set*);

/**
 * Create new data value from a table.
 * @return new table data whose value is copied from the argument.  The
 * caller must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_table(const broker_table*);

/**
 * Create new data value from a vector.
 * @return new vector data whose value is copied from the argument.  The
 * caller must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_vector(const broker_vector*);

/**
 * Create new data value from a record.
 * @return new record data whose value is copied from the argument.  The
 * caller must manage the memory of the returned object.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_data* broker_data_from_record(const broker_record*);

/**
 * "Cast" a data value to a bool.
 * @return the argument as a boolean object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
broker_bool* broker_data_as_bool(broker_data*);

/**
 * "Cast" a data value to a count.
 * @return the argument as a count object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
uint64_t* broker_data_as_count(broker_data*);

/**
 * "Cast" a data value to an integer.
 * @return the argument as an integer object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
int64_t* broker_data_as_integer(broker_data*);

/**
 * "Cast" a data value to a real number.
 * @return the argument as a real number object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
double* broker_data_as_real(broker_data*);

/**
 * "Cast" a data value to a string.
 * @return the argument as a string object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
broker_string* broker_data_as_string(broker_data*);

/**
 * "Cast" a data value to an address.
 * @return the argument as an address object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
broker_address* broker_data_as_address(broker_data*);

/**
 * "Cast" a data value to a subnet.
 * @return the argument as a subnet object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
broker_subnet* broker_data_as_subnet(broker_data*);

/**
 * "Cast" a data value to a port.
 * @return the argument as a port object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
broker_port* broker_data_as_port(broker_data*);

/**
 * "Cast" a data value to a time point.
 * @return the argument as a time point object.  No memory management required
 * by caller, the returned value is valid for the same lifetime as data supplied
 * as the input argument and may be used to modify the value of the data.
 */
broker_time_point* broker_data_as_time(broker_data*);

/**
 * "Cast" a data value to a time duration.
 * @return the argument as a time duration object.  No memory management
 * required by caller, the returned value is valid for the same lifetime as data
 * supplied as the input argument and may be used to modify the value of the
 * data.
 */
broker_time_duration* broker_data_as_duration(broker_data*);

/**
 * "Cast" a data value to an enum.
 * @return the argument as an enum object.  No memory management
 * required by caller, the returned value is valid for the same lifetime as data
 * supplied as the input argument and may be used to modify the value of the
 * data.
 */
broker_enum_value* broker_data_as_enum(broker_data*);

/**
 * "Cast" a data value to a set.
 * @return the argument as a set object.  No memory management
 * required by caller, the returned value is valid for the same lifetime as data
 * supplied as the input argument and may be used to modify the value of the
 * data.
 */
broker_set* broker_data_as_set(broker_data*);

/**
 * "Cast" a data value to a table.
 * @return the argument as a table object.  No memory management
 * required by caller, the returned value is valid for the same lifetime as data
 * supplied as the input argument and may be used to modify the value of the
 * data.
 */
broker_table* broker_data_as_table(broker_data*);

/**
 * "Cast" a data value to a vector.
 * @return the argument as a vector object.  No memory management
 * required by caller, the returned value is valid for the same lifetime as data
 * supplied as the input argument and may be used to modify the value of the
 * data.
 */
broker_vector* broker_data_as_vector(broker_data*);

/**
 * "Cast" a data value to a record.
 * @return the argument as a record object.  No memory management
 * required by caller, the returned value is valid for the same lifetime as data
 * supplied as the input argument and may be used to modify the value of the
 * data.
 */
broker_record* broker_data_as_record(broker_data*);

/**
 * Same as @see broker_data_as_bool, but the data value may not be modified.
 */
const broker_bool* broker_data_as_bool_const(const broker_data*);

/**
 * Same as @see broker_data_as_count, but the data value may not be modified.
 */
const uint64_t* broker_data_as_count_const(const broker_data*);

/**
 * Same as @see broker_data_as_integer, but the data value may not be modified.
 */
const int64_t* broker_data_as_integer_const(const broker_data*);

/**
 * Same as @see broker_data_as_real, but the data value may not be modified.
 */
const double* broker_data_as_real_const(const broker_data*);

/**
 * Same as @see broker_data_as_string, but the data value may not be modified.
 */
const broker_string* broker_data_as_string_const(const broker_data*);

/**
 * Same as @see broker_data_as_address, but the data value may not be modified.
 */
const broker_address* broker_data_as_address_const(const broker_data*);

/**
 * Same as @see broker_data_as_subnet, but the data value may not be modified.
 */
const broker_subnet* broker_data_as_subnet_const(const broker_data*);

/**
 * Same as @see broker_data_as_port, but the data value may not be modified.
 */
const broker_port* broker_data_as_port_const(const broker_data*);

/**
 * Same as @see broker_data_as_time, but the data value may not be modified.
 */
const broker_time_point* broker_data_as_time_const(const broker_data*);

/**
 * Same as @see broker_data_as_duration, but the data value may not be modified.
 */
const broker_time_duration* broker_data_as_duration_const(const broker_data*);

/**
 * Same as @see broker_data_as_enum, but the data value may not be modified.
 */
const broker_enum_value* broker_data_as_enum_const(const broker_data*);

/**
 * Same as @see broker_data_as_set, but the data value may not be modified.
 */
const broker_set* broker_data_as_set_const(const broker_data*);

/**
 * Same as @see broker_data_as_table, but the data value may not be modified.
 */
const broker_table* broker_data_as_table_const(const broker_data*);

/**
 * Same as @see broker_data_as_vector, but the data value may not be modified.
 */
const broker_vector* broker_data_as_vector_const(const broker_data*);

/**
 * Same as @see broker_data_as_record, but the data value may not be modified.
 */
const broker_record* broker_data_as_record_const(const broker_data*);

/**
 * Create a new string object.
 * @param cstring a null-terminated string to copy and use as the new string
 * object's value.
 * @return a new string object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_string* broker_string_create(const char* cstring);

/**
 * Reclaim the memory used by a string object.
 * @param s the string object to reclaim.
 */
void broker_string_delete(broker_string* s);

/**
 * Create a copy of a string object.
 * @param s the string object to copy.
 * @return a new string object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_string* broker_string_copy(const broker_string* s);

/**
 * Create a new string object from a sequence of bytes.
 * @param s a pointer to the start of a memory location containing the bytes
 * to copy.
 * @param len the number of bytes to copy.
 * @return a new string object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_string* broker_string_from_data(const char* s, size_t len);

/**
 * Set the value of a string object to a copy of a c-string.
 * @param s The string value to modify.
 * @param cstr A null-terminated string to use as the new string value.
 * @return not-zero on success, or zero on failure (e.g. failed to allocate
 * required memory) and the string is left unmodified.
 */
int broker_string_set_cstring(broker_string* s, const char* cstr);

/**
 * Set the value of a string object to a copy of a sequence of bytes.
 * @param s The string value to modify.
 * @param data a pointer to the first byte in a sequence.
 * @param len the number of bytes to copy.
 * @return not-zero on success, or zero on failure (e.g. failed to allocate
 * required memory) and the string is left unmodified.
 */
int broker_string_set_data(broker_string* s, const char* data, size_t len);

/**
 * @return a pointer to the beginning of the string data.
 */
const char* broker_string_data(const broker_string* s);

/**
 * @return the number of bytes in the string.
 */
size_t broker_string_size(const broker_string* s);

/**
 * @return not-zero if the two strings are equal or else zero.
 */
int broker_string_eq(const broker_string* a, const broker_string* b);

/**
 * @return not-zero if the first argument, \a a, is less than the second,
 * else zero.
 */
int broker_string_lt(const broker_string* a, const broker_string* b);

/**
 * @return the hash value for a string.
 */
size_t broker_string_hash(const broker_string* s);

/**
 * Create a new address object.
 * @return a new address object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_address* broker_address_create();

/**
 * Reclaim the memory used by an address object.
 * @param a the address object to reclaim.
 */
void broker_address_delete(broker_address* a);

/**
 * Create a copy of an address object.
 * @param a the object to copy.
 * @return a new address object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_address* broker_address_copy(const broker_address* a);

/**
 * Sets the value of an address object based on a human-readable string form
 * of an IP address (dotted decimal for IPv4 or colon-delimited-hexadecimal
 * for IPv6).
 * @param dst the address to modify, if passing the address of a null
 * pointer, a new address will be allocated and the pointer set to it.
 * @param s the string form of an IP address.
 * @return not-zero on success or zero if the call failed and didn't modify
 * the address (e.g. invalid IP string format).
 */
int broker_address_from_string(broker_address** dst, const char* s);

/**
 * Create an address object from IPv4 bytes in host order.
 * @param bytes a pointer to 4-bytes of data which represent an IPv4 address
 * in host order.
 * @return a new address object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_address* broker_address_from_v4_host_bytes(const uint32_t* bytes);

/**
 * Create an address object from IPv4 bytes in network order.
 * @param bytes a pointer to 4-bytes of data which represent an IPv4 address
 * in network order.
 * @return a new address object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_address* broker_address_from_v4_network_bytes(const uint32_t* bytes);

/**
 * Create an address object from IPv6 bytes in host order.
 * @param bytes a pointer to 16-bytes of data which represent an IPv6 address
 * in host order.
 * @return a new address object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_address* broker_address_from_v6_host_bytes(const uint32_t* bytes);

/**
 * Create an address object from IPv6 bytes in network order.
 * @param bytes a pointer to 16-bytes of data which represent an IPv6 address
 * in network order.
 * @return a new address object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_address* broker_address_from_v6_network_bytes(const uint32_t* bytes);

/**
 * Set the value of a broker address to that of another.
 * @param dst the address to replace.
 * @param src the address to copy into \a dst.
 */
void broker_address_set(broker_address* dst, broker_address* src);

/**
 * @see broker::address::mask()
 */
int broker_address_mask(broker_address* a, uint8_t top_bits_to_keep);

/**
 * @return not-zero if the address is IPv4, else zero.
 */
int broker_address_is_v4(const broker_address* a);

/**
 * @return not-zero if the address is IPv6, else zero.
 */
int broker_address_is_v6(const broker_address* a);

/**
 * @return a pointer to the first byte of the IP address.
 */
const uint8_t* broker_address_bytes(const broker_address* a);

/**
 * Convert an address to a human-readable string format.
 * @param a the address to stringify.
 * @return a human-readable IP string.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_string* broker_address_to_string(const broker_address* a);

/**
 * @return not-zero if the first argument is equal to the second, else zero.
 */
int broker_address_eq(const broker_address* a, const broker_address* b);

/**
 * @return not-zero if the first argument is less than the second, else zero.
 */
int broker_address_lt(const broker_address* a, const broker_address* b);

/**
 * @return the hash of the address.
 */
size_t broker_address_hash(const broker_address* a);

/**
 * Create a new subnet object.
 * @return a new subnet object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_subnet* broker_subnet_create();

/**
 * Reclaim the memory used by a subnet object.
 * @param s the subnet object to reclaim.
 */
void broker_subnet_delete(broker_subnet* s);

/**
 * Create a copy of a subnet object.
 * @param s the subnet object to copy.
 * @return a new subnet object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_subnet* broker_subnet_copy(const broker_subnet* s);

/**
 * Create a new subnet object from an address and prefix length.
 * @param a the address to use for the subnet.
 * @param len the number of prefix bits of the address to use for the subnet.
 * @return a new subnet object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_subnet* broker_subnet_from(const broker_address* a, uint8_t len);

/**
 * Set the value of a subnet object to that of another.
 * @param dst the subnet to replace.
 * @param src the subnet to copy.
 */
void broker_subnet_set(broker_subnet* dst, broker_subnet* src);

/**
 * @return the address portion of a subnet.
 */
const broker_address* broker_subnet_network(const broker_subnet* s);

/**
 * @return the prefix length of a subnet.
 */
uint8_t broker_subnet_length(const broker_subnet* s);

/**
 * @see broker::subnet::contains
 */
int broker_subnet_contains(const broker_subnet* s, const broker_address* a);

/**
 * Convert a subnet into a human-readable string format.
 * @param s the subnet to stringify.
 * @return a new string that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_string* broker_subnet_to_string(const broker_subnet* s);

/**
 * @return not-zero if the two subnets are equal, else zero.
 */
int broker_subnet_eq(const broker_subnet* a, const broker_subnet* b);

/**
 * @return not-zero if the first subnet is less than the second, else zero.
 */
int broker_subnet_lt(const broker_subnet* a, const broker_subnet* b);

/**
 * @return the hash value of a subnet.
 */
size_t broker_subnet_hash(const broker_subnet* a);

/**
 * Create a new port object.
 * @return a new port object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_port* broker_port_create();

/**
 * Reclaim the memory of a port object.
 * @param p the port to reclaim.
 */
void broker_port_delete(broker_port* p);

/**
 * @return a new port object that's a copy of the argument and up to the
 * caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_port* broker_port_copy(const broker_port* p);

/**
 * Create a port object from a protocol and number.
 * @param num the port number.
 * @param p a transport protocol.
 * @return a new port object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_port* broker_port_from(uint16_t num, broker_port_protocol p);

/**
 * Modify the number of a port.
 * @param dst the port object to modify.
 * @param number the new port number to use.
 */
void broker_port_set_number(broker_port* dst, uint16_t number);

/**
 * Modify the protocol of a port.
 * @param dst the port object to modify.
 * @param p the new protocol to use.
 */
void broker_port_set_protocol(broker_port* dst, broker_port_protocol p);

/**
 * @return the port number of a port object.
 */
uint16_t broker_port_number(const broker_port* p);

/**
 * @return the protocol of a port object.
 */
broker_port_protocol broker_port_type(const broker_port* p);

/**
 * Convert a port object to a human-readable string format.
 * @param p the port to stringify.
 * @return a new string object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_string* broker_port_to_string(const broker_port* p);

/**
 * @return not-zero if the two ports are equal, else zero.
 */
int broker_port_eq(const broker_port* a, const broker_port* b);

/**
 * @return not-zero if the first argument is less than the second, else zero.
 */
int broker_port_lt(const broker_port* a, const broker_port* b);

/**
 * @return the hash of the port.
 */
size_t broker_port_hash(const broker_port* p);

/**
 * Create a new time point object.
 * @param seconds_from_epoch the number of seconds since the Unix epoch to use
 * as the absolute point in time.
 * @return a new time point object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_time_point* broker_time_point_create(double seconds_from_epoch);

/**
 * Reclaim a time point object.
 * @param t the object to reclaim.
 */
void broker_time_point_delete(broker_time_point* t);

/**
 * Create a copy of a time point object.
 * @param t the object to copy.
 * @return a new time point object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_time_point* broker_time_point_copy(const broker_time_point* t);

/**
 * Create a new time point object representing the point in time of the call.
 * @return a new time point object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_time_point* broker_time_point_now();

/**
 * @return the value of the time point object represent as a floating point
 * number.
 */
double broker_time_point_value(const broker_time_point* t);

/**
 * Modify the value of a time point object.
 * @param t the object to modify.
 * @param seconds_from_epoch the new value of the object to use.
 */
void broker_time_point_set(broker_time_point* t, double seconds_from_epoch);

/**
 * @return not-zero if the two time points are equal, else zero.
 */
int broker_time_point_eq(const broker_time_point* a,
                         const broker_time_point* b);

/**
 * @return not-zero if the first argument is less than the second, else zero.
 */
int broker_time_point_lt(const broker_time_point* a,
                         const broker_time_point* b);

/**
 * @return the hash of the time point.
 */
size_t broker_time_point_hash(const broker_time_point* p);

/**
 * Create a new time duration object.
 * @param seconds the number of seconds representing the duration.
 * @return a new duration object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_time_duration* broker_time_duration_create(double seconds);

/**
 * Reclaim a duration object.
 * @param t the object to reclaim.
 */
void broker_time_duration_delete(broker_time_duration* t);

/**
 * Create a copy of a duration object.
 * @param t the duration to copy.
 * @return a new duration object that's up to the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_time_duration* broker_time_duration_copy(const broker_time_duration* t);

/**
 * @return the number of seconds representing the duration.
 */
double broker_time_duration_value(const broker_time_duration* t);

/**
 * Modify the value of a duration object.
 * @param t the duration to modify.
 * @param seconds the new number of seconds to use for the duration value.
 */
void broker_time_duration_set(broker_time_duration* t, double seconds);

/**
 * @return not-zero if the two durations are equal, else zero.
 */
int broker_time_duration_eq(const broker_time_duration* a,
                            const broker_time_duration* b);

/**
 * @return not-zero if the first duration is less than the second, else zero.
 */
int broker_time_duration_lt(const broker_time_duration* a,
                            const broker_time_duration* b);

/**
 * @return the hash of the duration.
 */
size_t broker_time_duration_hash(const broker_time_duration* p);

/**
 * Create a new enum object.
 * @param name a null-terminated string to copy and use as the enum's name.
 * @return a new enum object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_enum_value* broker_enum_value_create(const char* name);

/**
 * Reclaim an enum object.
 * @param e an enum to reclaim.
 */
void broker_enum_value_delete(broker_enum_value* e);

/**
 * Create a copy of an enum object.
 * @param e an enum to copy.
 * @return a new enum object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_enum_value* broker_enum_value_copy(const broker_enum_value* e);

/**
 * @return the name associated with the enum.
 */
const char* broker_enum_value_name(const broker_enum_value* e);

/**
 * Modify an enum object's name.
 * @param e the enum to modify.
 * @param name the new name to copy and use for the enum's name.
 * @return not-zero on success, or zero if sufficient memory could not be
 * allocated and the enum is left unaltered.
 */
int broker_enum_value_set(broker_enum_value* e, const char* name);

/**
 * @return not-zero if the two enums are equal, else zero.
 */
int broker_enum_value_eq(const broker_enum_value* a,
                         const broker_enum_value* b);

/**
 * @return not-zero if the first argument is less than the second, else zero.
 */
int broker_enum_value_lt(const broker_enum_value* a,
                         const broker_enum_value* b);

/**
 * @return the hash of the enum.
 */
size_t broker_enum_value_hash(const broker_enum_value* p);

/**
 * Create a new set object.
 * @return a new set object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_set* broker_set_create();

/**
 * Reclaim a set object.
 * @param s The set to reclaim.
 */
void broker_set_delete(broker_set* s);

/**
 * Create a copy of a set object.
 * @param s the set to copy.
 * @return a new set object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_set* broker_set_copy(const broker_set* s);

/**
 * Remove all elements from a set.
 * @param s the set to clear.
 */
void broker_set_clear(broker_set* s);

/**
 * @return the number of elements in a set.
 */
size_t broker_set_size(const broker_set* s);

/**
 * Check if a set contains an equivalent data object.
 * @param s the set which to check for key existence.
 * @param key a data object to check for existence within the set.
 * @return not-zero if it's in the set, else zero.
 */
int broker_set_contains(const broker_set* s, const broker_data* key);

/**
 * Insert a new element into a set.
 * @param s the set to modify.
 * @param key the data to insert (by copying it).
 * @return a positive number on successful insert, zero if the element
 * already existed, or a negative number if sufficient memory could not be
 * allocated.
 */
int broker_set_insert(broker_set* s, const broker_data* key);

/**
 * Remove an element from a set.
 * @param s the set to modify.
 * @param key the data to remove.
 * @return the number of elements removed from the set (0 or 1).
 */
int broker_set_remove(broker_set* s, const broker_data* key);

/**
 * @return not-zero if the two sets are equal, else zero.
 */
int broker_set_eq(const broker_set* a, const broker_set* b);

/**
 * @return not-zero if the first argument is less than the second, else zero.
 */
int broker_set_lt(const broker_set* a, const broker_set* b);

/**
 * @return the hash of the set.
 */
size_t broker_set_hash(const broker_set* a);

/**
 * @return a set iterator object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_set_iterator* broker_set_iterator_create(broker_set* s);

/**
 * Reclaim a set iterator.
 * @param it the set iterator object to reclaim.
 */
void broker_set_iterator_delete(broker_set_iterator* it);

/**
 * Check if an iterator is at the end and cannot be incremented further.
 * @param s the container used to create the iterator.
 * @param it the iterator to check.
 * @return not-zero if the iterator is at the end of iteration, else zero.
 */
int broker_set_iterator_at_last(broker_set* s, broker_set_iterator* it);

/**
 * Move the iterator to the next element.
 * @param s the container used to create the iterator.
 * @param it the iterator to move.
 * @return not-zero if, after moving, the iterator is at the end of iteration,
 * else zero.
 */
int broker_set_iterator_next(broker_set* s, broker_set_iterator* it);

/**
 * @return the element the iterator currently points to.
 */
const broker_data* broker_set_iterator_value(broker_set_iterator* it);

/**
 * @see broker_set_iterator_create
 */
broker_set_const_iterator*
broker_set_const_iterator_create(const broker_set* s);

/**
 * @see broker_set_iterator_delete
 */
void broker_set_const_iterator_delete(broker_set_const_iterator* it);

/**
 * @see broker_set_iterator_at_last
 */
int broker_set_const_iterator_at_last(const broker_set* s,
                                      broker_set_const_iterator* it);

/**
 * @see broker_set_iterator_next
 */
int broker_set_const_iterator_next(const broker_set* s,
                                   broker_set_const_iterator* it);

/**
 * @see broker_set_iterator_value
 */
const broker_data*
broker_set_const_iterator_value(broker_set_const_iterator* it);

/**
 * Create a new table object.
 * @return a new table object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_table* broker_table_create();

/**
 * Reclaim a table.
 * @param t the table to reclaim.
 */
void broker_table_delete(broker_table* t);

/**
 * Create a copy of a table object.
 * @param t the table to copy.
 * @return a new table object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_table* broker_table_copy(const broker_table* t);

/**
 * Remove all elements from a table.
 * @param t the table to modify.
 */
void broker_table_clear(broker_table* t);

/**
 * @return the number of elements in a table.
 */
size_t broker_table_size(const broker_table* t);

/**
 * Check if a key exists in a table.
 * @param t the table to check.
 * @param key the key to check for.
 * @return not-zero if the key is in the table, else zero.
 */
int broker_table_contains(const broker_table* t, const broker_data* key);

/**
 * Insert a key and associated value in to a table.
 * @param t the table to modify (by copying it).
 * @param key the key to insert (by copying it).
 * @param value the value to insert.
 * @return a positive number if the insertion took place, zero if it did not,
 * or -1 if sufficient memory could not be allocated for the insertion.
 */
int broker_table_insert(broker_table* t, const broker_data* key,
                        const broker_data* value);

/**
 * Remove an entry from a table.
 * @param t the table to modify.
 * @param key the key to remove along with its associated value.
 * @return the number of elements removed (0 or 1).
 */
int broker_table_remove(broker_table* t, const broker_data* key);

/**
 * Retrieve a value from the table by its associated key.
 * @param t the table to inspect.
 * @param key the key used to originally insert the value.
 * @return a pointer to the value associated with the key (still owned by the
 * table object), or null if the key did not exist.
 */
broker_data* broker_table_lookup(broker_table* t, const broker_data* key);

/**
 * @return not-zero if the two tables are equal, else zero.
 */
int broker_table_eq(const broker_table* a, const broker_table* b);

/**
 * @return not-zero if the first argument is less than the second, else zero.
 */
int broker_table_lt(const broker_table* a, const broker_table* b);

/**
 * @return the hash of the table.
 */
size_t broker_table_hash(const broker_table* a);

/**
 * @return a table iterator object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_table_iterator* broker_table_iterator_create(broker_table* t);

/**
 * Reclaim the iterator.
 */
void broker_table_iterator_delete(broker_table_iterator* it);

/**
 * Check if an iterator is at the end and cannot be incremented further.
 * @param t the container used to create the iterator.
 * @param it the iterator to check.
 * @return not-zero if the iterator is at the end of iteration, else zero.
 */
int broker_table_iterator_at_last(broker_table* t, broker_table_iterator* it);

/**
 * Move the iterator to the next element.
 * @param t the container used to create the iterator.
 * @param it the iterator to move.
 * @return not-zero if, after moving, the iterator is at the end of iteration,
 * else zero.
 */
int broker_table_iterator_next(broker_table* t, broker_table_iterator* it);

/**
 * @return the element the iterator currently points to.
 */
broker_table_entry broker_table_iterator_value(broker_table_iterator* it);

/**
 * @see broker_table_iterator_create
 */
broker_table_const_iterator*
broker_table_const_iterator_create(const broker_table* t);

/**
 * @see broker_table_iterator_delete
 */
void broker_table_const_iterator_delete(broker_table_const_iterator* it);

/**
 * @see broker_table_iterator_at_last
 */
int broker_table_const_iterator_at_last(const broker_table* t,
                                        broker_table_const_iterator* it);

/**
 * @see broker_table_iterator_next
 */
int broker_table_const_iterator_next(const broker_table* t,
                                     broker_table_const_iterator* it);

/**
 * @see broker_table_iterator_value
 */
broker_const_table_entry
broker_table_const_iterator_value(broker_table_const_iterator* it);

/**
 * Create a new vector object.
 * @return a new vector object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_vector* broker_vector_create();

/**
 * Reclaim a vector object.
 */
void broker_vector_delete(broker_vector* v);

/**
 * Create a copy of a vector.
 * @param v the vector to copy.
 * @return a new vector object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_vector* broker_vector_copy(const broker_vector* v);

/**
 * Remove all elements from a vector.
 * @param v the vector to modify.
 */
void broker_vector_clear(broker_vector* v);

/**
 * @return the number of elements in a vector.
 */
size_t broker_vector_size(const broker_vector* v);

/**
 * Reserve memory in the vector for some number of elements.
 * @param v the vector to modify.
 * @param size the capacity of the vector to use (if it's less than the
 * current size of the vector the call is a no-op).
 * @return not-zero if the call succeeds, or zero if sufficient memory could
 * not be allocated.
 */
int broker_vector_reserve(broker_vector* v, size_t size);

/**
 * Insert an element into a vector.
 * @param v the vector to modify.
 * @param d the element to insert into the vector (by copying it).
 * @param idx the index to insert the element at.  Must be in the range from 0
 * to the current number of elements in the vector.  The vector is automatically
 * resized to fit the new element and other contents are shifted right.
 * @return not-zero if the call succeeds, or zero if sufficient memory could
 * not be allocated.
 */
int broker_vector_insert(broker_vector* v, const broker_data* d, size_t idx);

/**
 * Replace an element in a vector.
 * @param v the vector to modify.
 * @param d the element to insert into the vector (by copying it).
 * @param idx the index to replace (must be a valid index).
 * @return not-zero if the call succeeds, or zero if sufficient memory could
 * not be allocated.
 */
int broker_vector_replace(broker_vector* v, const broker_data* d, size_t idx);

/**
 * Remove an element from a vector.
 * @param v the vector to modify.
 * @param idx the index to remove (must be a valid index).
 * @return the call currently can never fail and always returns 1.
 */
int broker_vector_remove(broker_vector* v, size_t idx);

/**
 * Look up an element in a vector.
 * @param v the vector to inspect.
 * @param idx the index to lookup (must be a valid index).
 * @return a pointer to the element (the vector still owns it).
 */
broker_data* broker_vector_lookup(broker_vector* v, size_t idx);

/**
 * @return not-zero if the two vectors are equal, else zero.
 */
int broker_vector_eq(const broker_vector* a, const broker_vector* b);

/**
 * @return not-zero if the first argument is less than the second.
 */
int broker_vector_lt(const broker_vector* a, const broker_vector* b);

/**
 * @return the hash of the vector.
 */
size_t broker_vector_hash(const broker_vector* a);

/**
 * @return a vector iterator object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_vector_iterator* broker_vector_iterator_create(broker_vector* v);

/**
 * Reclaim the iterator.
 */
void broker_vector_iterator_delete(broker_vector_iterator* it);

/**
 * Check if an iterator is at the end and cannot be incremented further.
 * @param v the container used to create the iterator.
 * @param it the iterator to check.
 * @return not-zero if the iterator is at the end of iteration, else zero.
 */
int broker_vector_iterator_at_last(broker_vector* v,
                                   broker_vector_iterator* it);

/**
 * Move the iterator to the next element.
 * @param v the container used to create the iterator.
 * @param it the iterator to move.
 * @return not-zero if, after moving, the iterator is at the end of iteration,
 * else zero.
 */
int broker_vector_iterator_next(broker_vector* v, broker_vector_iterator* it);

/**
 * @return the element the iterator currently points to.
 */
broker_data* broker_vector_iterator_value(broker_vector_iterator* it);

/**
 * @see broker_vector_iterator_create
 */
broker_vector_const_iterator*
broker_vector_const_iterator_create(const broker_vector* v);

/**
 * @see broker_vector_iterator_delete
 */
void broker_vector_const_iterator_delete(broker_vector_const_iterator* it);

/**
 * @see broker_vector_iterator_at_last
 */
int broker_vector_const_iterator_at_last(const broker_vector* v,
                                   broker_vector_const_iterator* it);

/**
 * @see broker_vector_iterator_next
 */
int broker_vector_const_iterator_next(const broker_vector* v,
                                      broker_vector_const_iterator* it);

/**
 * @see broker_vector_iterator_value
 */
const broker_data*
broker_vector_const_iterator_value(broker_vector_const_iterator* it);

/**
 * Create a new record object.
 * @param size the number of fields in the record.
 * @return a new record object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_record* broker_record_create(size_t size);

/**
 * Reclaim a record object.
 */
void broker_record_delete(broker_record* r);

/**
 * Copy a record object.
 * @param r the record to copy.
 * @return a new record object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_record* broker_record_copy(const broker_record* r);

/**
 * @return the number of fields in the record.
 */
size_t broker_record_size(const broker_record* r);

/**
 * Assign a value to a record field.
 * @param r the record to modify.
 * @param d the data to assign (by copying it) or a null pointer to set the
 * field to a nil value.
 * @param idx the index of the field to assign.
 * @return not-zero on success or zero if sufficent memory could not be
 * allocated.
 */
int broker_record_assign(broker_record* r, const broker_data* d, size_t idx);

/**
 * Lookup a field in a record.
 * @param r the record to inspect.
 * @param idx the index of the field to retrieve.
 * @return a pointer to the field's value (the record still owns it), which
 * may be null.
 */
broker_data* broker_record_lookup(broker_record* r, size_t idx);

/**
 * @return not-zero if the two records are equal, else zero.
 */
int broker_record_eq(const broker_record* a, const broker_record* b);

/**
 * @return not-zero if the first argument is less than the second, else zero.
 */
int broker_record_lt(const broker_record* a, const broker_record* b);

/**
 * @return the hash of the record.
 */
size_t broker_record_hash(const broker_record* a);

/**
 * @return a record iterator object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_record_iterator* broker_record_iterator_create(broker_record* r);

/**
 * Reclaim a record iterator.
 */
void broker_record_iterator_delete(broker_record_iterator* it);

/**
 * Check if an iterator is at the end and cannot be incremented further.
 * @param r the container used to create the iterator.
 * @param it the iterator to check.
 * @return not-zero if the iterator is at the end of iteration, else zero.
 */
int broker_record_iterator_at_last(broker_record* r,
                                   broker_record_iterator* it);

/**
 * Move the iterator to the next element.
 * @param r the container used to create the iterator.
 * @param it the iterator to move.
 * @return not-zero if, after moving, the iterator is at the end of iteration,
 * else zero.
 */
int broker_record_iterator_next(broker_record* r, broker_record_iterator* it);

/**
 * @return the element the iterator currently points to.
 */
broker_data* broker_record_iterator_value(broker_record_iterator* it);

/**
 * @see broker_record_iterator_create
 */
broker_record_const_iterator*
broker_record_const_iterator_create(const broker_record* r);

/**
 * @see broker_record_iterator_delete
 */
void broker_record_const_iterator_delete(broker_record_const_iterator* it);

/**
 * @see broker_record_iterator_at_last
 */
int broker_record_const_iterator_at_last(const broker_record* r,
                                         broker_record_const_iterator* it);

/**
 * @see broker_record_iterator_next
 */
int broker_record_const_iterator_next(const broker_record* r,
                                      broker_record_const_iterator* it);

/**
 * @see broker_record_iterator_value
 */
const broker_data*
broker_record_const_iterator_value(broker_record_const_iterator* it);

/**
 * A message that can be sent between broker endpoints.  They are the same
 * as vectors -- a sequence of arbitrary data types.
 */
typedef broker_vector broker_message;

/**
 * An object which reflects a peering between two broker endpoints.
 */
struct broker_peering;
typedef struct broker_peering broker_peering;

/**
 * Create an uninitialized peering object.
 * @return a new peering object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_peering* broker_peering_create();

/**
 * Reclaim a peering object.
 */
void broker_peering_delete(broker_peering* p);

/**
 * Create a copy of a peering object.
 * @param p the object to copy.
 * @return a new peering object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_peering* broker_peering_copy(const broker_peering* p);

/**
 * @return not-zero if the peering is between a local endpoint and a remote
 * endpoint.
 */
int broker_peering_is_remote(const broker_peering* p);

/**
 * @return a string reflecting the address of the remote side of the peering.
 */
const char* broker_peering_remote_host(const broker_peering* p);

/**
 * @return the port used by the remote side of the peering.
 */
uint16_t broker_peering_remote_port(const broker_peering* p);

/**
 * @return not-zero if the peering object is valid (one actually
 * produced by connecting two endpoints), else zero.
 */
int broker_peering_is_initialized(const broker_peering* p);

/**
 * @return not-zero if the two objects are equal, else zero.
 */
int broker_peering_eq(const broker_peering* a, const broker_peering* b);

/**
 * @return the hash of the peering object.
 */
size_t broker_peering_hash(const broker_peering* a);

/**
 * Reflects the status of an outgoing connection attempt between two endpoints.
 */
struct broker_outgoing_connection_status;
typedef struct broker_outgoing_connection_status
        broker_outgoing_connection_status;

/**
 * Whether the connection was established or not.
 */
typedef enum {
	broker_outgoing_connection_status_tag_established,
	broker_outgoing_connection_status_tag_disconnected,
	broker_outgoing_connection_status_tag_incompatible,
} broker_outgoing_connection_status_tag;

/**
 * @return the peering object associated with the connection between two
 * endpoints.
 */
const broker_peering*
broker_outgoing_connection_status_peering(const broker_outgoing_connection_status*);

/**
 * @return the result of the connection attempt.
 */
broker_outgoing_connection_status_tag
broker_outgoing_connection_status_get(const broker_outgoing_connection_status*);

/**
 * @return the name the acceptor endpoint has chosen for itself.  This
 * is only known if the connection was fully established.
 */
const char*
broker_outgoing_connection_status_peer_name(const broker_outgoing_connection_status*);

/**
 * Reflects the status of an incoming connection attempt between two endpoints.
 */
struct broker_incoming_connection_status;
typedef struct broker_incoming_connection_status
        broker_incoming_connection_status;

/**
 * Whether a new peer has connected or got disconnected.
 */
typedef enum {
	broker_incoming_connection_status_tag_established,
	broker_incoming_connection_status_tag_disconnected,
} broker_incoming_connection_status_tag;

/**
 * @return the status of an incoming connection.
 */
broker_incoming_connection_status_tag
broker_incoming_connection_status_get(const broker_incoming_connection_status*);

/**
 * @brief broker_incoming_connection_status_peer_name
 * @return the name the connector endpoint has chosen for itself.
 */
const char*
broker_incoming_connection_status_peer_name(const broker_incoming_connection_status*);

/**
 * A double-ended queue of broker messages.
 */
struct broker_deque_of_message;
typedef struct broker_deque_of_message broker_deque_of_message;

/**
 * Reclaim the container and all messages in it.
 */
void broker_deque_of_message_delete(broker_deque_of_message* d);

/**
 * @return the number of messages in the container.
 */
size_t broker_deque_of_message_size(const broker_deque_of_message* d);

/**
 * Retrieve a single message from the container.
 * @param d the container of messages to inspect.
 * @param idx the index of the message to retrieve (must be a valid index).
 * @return a pointer to the retrieved message which is still owned by the
 * container.
 */
broker_message*
broker_deque_of_message_at(broker_deque_of_message* d, size_t idx);

/**
 * Remove a message from the container.
 * @param d the container to modify.
 * @param idx the index of the message to remove (must be a valid index).
 * Deques are best at removing from the ends of the container.
 */
void broker_deque_of_message_erase(broker_deque_of_message* d, size_t idx);

/**
 * @see broker::endpoint
 */
struct broker_endpoint;
typedef struct broker_endpoint broker_endpoint;

/**
 * @see broker::message_queue
 */
struct broker_message_queue;
typedef struct broker_message_queue broker_message_queue;

/**
 * Create a new message queue.
 * @param topic_prefix the subscription topic to use.  All messages sent via
 * endpoint \a e or one of its peers that use a topic prefixed by this will
 * be copied in to this queue.
 * @param e the endpoint to attach the message queue.
 * @return the new message queue for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_message_queue*
broker_message_queue_create(const broker_string* topic_prefix,
                            const broker_endpoint* e);

/**
 * @return the topic prefix originally used to create the queue.
 */
const broker_string*
broker_message_queue_topic_prefix(const broker_message_queue* q);

/**
 * @return a file descriptor that is ready for reading when the queue is
 * non-empty.  Suitable for use with poll, select, etc.
 */
int broker_message_queue_fd(const broker_message_queue* q);

/**
 * @return the contents of the queue at time of call, which may be empty.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_message*
broker_message_queue_want_pop(const broker_message_queue* q);

/**
 * @return the contents of the queue at time of call, which must contain
 * at least one item.  The call will block until at least one item is ready.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_message*
broker_message_queue_need_pop(const broker_message_queue* q);

/**
 * A double-ended queue of incoming connection status messages.
 */
struct broker_deque_of_incoming_connection_status;
typedef struct broker_deque_of_incoming_connection_status
        broker_deque_of_incoming_connection_status;

/**
 * Reclaim the container.
 */
void broker_deque_of_incoming_connection_status_delete(
        broker_deque_of_incoming_connection_status* d);

/**
 * @return the number of messages in the container.
 */
size_t broker_deque_of_incoming_connection_status_size(
        const broker_deque_of_incoming_connection_status* d);

/**
 * Get a message from the container.
 * @param d the container to inspect.
 * @param idx the index to retrieve.
 * @return a pointer to the retrieved message, which is still owned by the
 * container.
 */
broker_incoming_connection_status*
broker_deque_of_incoming_connection_status_at(
        broker_deque_of_incoming_connection_status* d, size_t idx);

/**
 * Remove a message from the container.
 * @param d the container to modify.
 * @param idx the index of the message to remove.
 */
void broker_deque_of_incoming_connection_status_erase(
        broker_deque_of_incoming_connection_status* d, size_t idx);

/**
 * @see broker::queue<broker::incoming_connection_status>
 */
struct broker_incoming_connection_status_queue;
typedef struct broker_incoming_connection_status_queue
        broker_incoming_connection_status_queue;

/**
 * @return a file descriptor that is ready for reading when the queue is
 * non-empty.  Suitable for use with poll, select, etc.
 */
int broker_incoming_connection_status_queue_fd(
        const broker_incoming_connection_status_queue* q);

/**
 * @return the contents of the queue at time of call, which may be empty.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_incoming_connection_status*
broker_incoming_connection_status_queue_want_pop(
        const broker_incoming_connection_status_queue* q);

/**
 * @return the contents of the queue at time of call, which must contain
 * at least one item.  The call will block until at least one item is ready.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_incoming_connection_status*
broker_incoming_connection_status_queue_need_pop(
        const broker_incoming_connection_status_queue* q);

/**
 * A double-ended queue of outgoing connection status messages.
 */
struct broker_deque_of_outgoing_connection_status;
typedef struct broker_deque_of_outgoing_connection_status
        broker_deque_of_outgoing_connection_status;

/**
 * Reclaim the container.
 */
void broker_deque_of_outgoing_connection_status_delete(
        broker_deque_of_outgoing_connection_status* d);

/**
 * @return the number of messages in the container.
 */
size_t broker_deque_of_outgoing_connection_status_size(
        const broker_deque_of_outgoing_connection_status* d);

/**
 * Get a message from the container.
 * @param d the container to inspect.
 * @param idx the index to retrieve.
 * @return a pointer to the retrieved message, which is still owned by the
 * container.
 */
broker_outgoing_connection_status*
broker_deque_of_outgoing_connection_status_at(
        broker_deque_of_outgoing_connection_status* d, size_t idx);

/**
 * Remove a message from the container.
 * @param d the container to modify.
 * @param idx the index of the message to remove.
 */
void broker_deque_of_outgoing_connection_status_erase(
        broker_deque_of_outgoing_connection_status* d, size_t idx);


struct broker_outgoing_connection_status_queue;
typedef struct broker_outgoing_connection_status_queue
        broker_outgoing_connection_status_queue;

/**
 * @see broker::queue<broker::outgoing_connection_status>
 */
int broker_outgoing_connection_status_queue_fd(
        const broker_outgoing_connection_status_queue* q);

/**
 * @return the contents of the queue at time of call, which may be empty.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_outgoing_connection_status*
broker_outgoing_connection_status_queue_want_pop(
        const broker_outgoing_connection_status_queue* q);

/**
 * @return the contents of the queue at time of call, which must contain
 * at least one item.  The call will block until at least one item is ready.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_outgoing_connection_status*
broker_outgoing_connection_status_queue_need_pop(
        const broker_outgoing_connection_status_queue* q);

/**
 * @see broker::AUTO_PUBLISH
 */
const int BROKER_AUTO_PUBLISH = 0x01;

/**
 * @see broker::AUTO_ADVERTISE
 */
const int BROKER_AUTO_ADVERTISE = 0x02;

/**
 * @see broker::SELF
 */
const int BROKER_SELF = 0x01;

/**
 * @see broker::PEERS
 */
const int BROKER_PEERS = 0x02;

/**
 * @see broker::UNSOLICITED
 */
const int BROKER_UNSOLICITED = 0x04;

/**
 * @see broker::endpoint::endpoint
 * @return a new endpoint object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_endpoint* broker_endpoint_create(const char* name);

/**
 * @see broker::endpoint::endpoint
 * @return a new endpoint object for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_endpoint* broker_endpoint_create_with_flags(const char* name, int flags);

/**
 * Reclaim an endpoint object.
 */
void broker_endpoint_delete(broker_endpoint* e);

/**
 * @return the name assigned to the endpoint when it was originally created.
 */
const char* broker_endpoint_name(const broker_endpoint* e);

/**
 * @return the flags the endpoint currently uses.
 */
int broker_endpoint_flags(const broker_endpoint* e);

/**
 * Set new flags for the endpoint to use.
 */
void broker_endpoint_set_flags(broker_endpoint* e, int flags);

/**
 * @see broker::endpoint::last_errno
 */
int broker_endpoint_last_errno(const broker_endpoint* e);

/**
 * @see broker::endpoint::last_error
 */
const char* broker_endpoint_last_error(const broker_endpoint* e);

/**
 * @see broker::endpoint::listen
 */
int broker_endpoint_listen(broker_endpoint* e, uint16_t port, const char* addr,
                           int reuse_addr);

/**
 * @see broker::endpoint::peer(std::string, uint16_t, std::chrono::duration<double>)
 * @return a new peering object.
 */
broker_peering* broker_endpoint_peer_remotely(broker_endpoint* e,
                                              const char* addr, uint16_t port,
                                              double retry_interval);
/**
 * @see broker::endpoint::peer(const endpoint&)
 * @return a new peering object.
 */
broker_peering* broker_endpoint_peer_locally(broker_endpoint* self,
                                             const broker_endpoint* other);

/**
 * @see broker::endpoint::unpeer
 */
int broker_endpoint_unpeer(broker_endpoint* e, const broker_peering* p);

/**
 * @see broker::endpoint::outgoing_connection_status
 * @return a pointer to a queue that receives notifications of when an
 * endpoint's outgoing connections have status updates.
 */
const broker_outgoing_connection_status_queue*
broker_endpoint_outgoing_connection_status(const broker_endpoint* e);

/**
 * @see broker::endpoint::incoming_connection_status
 * @return a pointer to a queue that receives notifications of when an
 * endpoint's incoming connections have status updates.
 */
const broker_incoming_connection_status_queue*
broker_endpoint_incoming_connection_status(const broker_endpoint* e);

/**
 * @see broker::endpoint::send
 * @param e the endpoint under which the message will be sent.
 * @param topic the topic associated with the message.
 * @param msg the contents of the message to send.
 * @return not-zero on success, else zero if sufficient memory could not be
 * allocated for copying the topic or message.
 */
int broker_endpoint_send(broker_endpoint* e, const broker_string* topic,
                         const broker_message* msg);

/**
 * @see broker::endpoint::send
 * @param e the endpoint under which the message will be sent.
 * @param topic the topic associated with the message.
 * @param msg the contents of the message to send.
 * @param flags tunes the behavior of how the message is sent to peers.
 * @return not-zero on success, else zero if sufficient memory could not be
 * allocated for copying the topic or message.
 */
int broker_endpoint_send_with_flags(broker_endpoint* e,
                                    const broker_string* topic,
                                    const broker_message* msg, int flags);

/**
 * @see broker::endpoint::publish
 * @return not-zero on success, else zero if failed to allocate memory for
 * copying the topic string.
 */
int broker_endpoint_publish(broker_endpoint* e, const broker_string* topic);

/**
 * @see broker::endpoint::unpublish
 * @return not-zero on success, else zero if failed to allocate memory for
 * copying the topic string.
 */
int broker_endpoint_unpublish(broker_endpoint* e, const broker_string* topic);

/**
 * @see broker::endpoint::advertise
 * @return not-zero on success, else zero if failed to allocate memory for
 * copying the topic string.
 */
int broker_endpoint_advertise(broker_endpoint* e, const broker_string* topic);

/**
 * @see broker::endpoint::unadvertise
 * @return not-zero on success, else zero if failed to allocate memory for
 * copying the topic string.
 */
int broker_endpoint_unadvertise(broker_endpoint* e,
                                const broker_string* topic);

/**
 * @see broker::report::level
 */
typedef enum {
	broker_report_level_debug,
	broker_report_level_info,
	broker_report_level_warn,
	broker_report_level_error,
} broker_report_level;

/**
 * @see broker::report::manager
 * @return a pointer to the singleton endpoint that handles report messages.
 */
broker_endpoint* broker_report_manager();

/**
 * @see broker::report::default_queue
 * @return a pointer to the singleton report queue that receives all
 * report messages.
 */
broker_message_queue* broker_report_default_queue();

/**
 * @see broker::report::init
 */
int broker_report_init(int with_default_queue);

/**
 * @see broker::report::done
 */
void broker_report_done();

/**
 * @see broker::report::send
 * @return not-zero if the message was sent, or zero if memory could not be
 * allocated to copy the topic or message content.
 */
int broker_report_send(broker_report_level lvl, const broker_string* subtopic,
                       const broker_string* msg);

/**
 * @see broker::store::expiration_time
 */
struct broker_store_expiration_time;
typedef struct broker_store_expiration_time broker_store_expiration_time;

/**
 * The type of data store value expiry.
 */
typedef enum {
	broker_store_expiration_time_tag_since_last_modification,
	broker_store_expiration_time_tag_absolute,
} broker_store_expiration_time_tag;

/**
 * Create a new expiration time for a data store entry.
 * @param interval the amount of time in seconds since the last modification
 * after which the entry will be expired.
 * @param time the last-modified time (in seconds from Unix epoch).
 * @return a new expiration time for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_expiration_time*
broker_store_expiration_time_create_relative(double interval, double time);

/**
 * Create a new expiration time for a data store entry.
 * @param time the absolute time at which the entry will be expired (in
 * seconds from Unix epoch).
 * @return a new expiration time for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_expiration_time*
broker_store_expiration_time_create_absolute(double time);

/**
 * Reclaim the expiration time object.
 */
void broker_store_expiration_time_delete(broker_store_expiration_time* t);

/**
 * @return the type of expiration.
 */
broker_store_expiration_time_tag
broker_store_expiration_time_get_tag(const broker_store_expiration_time* t);

/**
 * @return for relative expirations, a pointer to the expiry interval.  For
 * absolute expirations, a pointer to the absolute expiry time.
 */
double* broker_store_expiration_time_get(broker_store_expiration_time* t);

/**
 * @return for relative expirations, a pointer to the last-modified time.
 */
double* broker_store_expiration_time_last_mod(broker_store_expiration_time* t);

/**
 * @return not-zero if the two expiration times are equal, else zero.
 */
int broker_store_expiration_time_eq(const broker_store_expiration_time* a,
                                    const broker_store_expiration_time* b);

/**
 * The types of data store queries that can be made.
 */
typedef enum {
	broker_store_query_tag_pop_left,
	broker_store_query_tag_pop_right,
	broker_store_query_tag_lookup,
	broker_store_query_tag_exists,
	broker_store_query_tag_keys,
	broker_store_query_tag_size,
	broker_store_query_tag_snapshot,
} broker_store_query_tag;

/**
 * @see broker::store::query
 */
struct broker_store_query;
typedef struct broker_store_query broker_store_query;

/**
 * Create a new data store query object.
 * @param tag the type of query to perform.
 * @param key for queries that operate on data store keys, this is the key to
 * use.  For those that don't require key information, this may be null.
 * @return a new data store query for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_query* broker_store_query_create(broker_store_query_tag tag,
                                              const broker_data* key);

/**
 * Reclaim a query object.
 */
void broker_store_query_delete(broker_store_query* q);

/**
 * @return the type of a data store query.
 */
broker_store_query_tag broker_store_query_get_tag(const broker_store_query* q);

/**
 * @return a pointer to the key associated with a query object (it's still
 * owned by the query).
 */
broker_data* broker_store_query_key(broker_store_query* q);

/**
 * @see broker::store::result
 */
struct broker_store_result;
typedef struct broker_store_result broker_store_result;

/**
 * The types of results that a data store query can yield.
 */
typedef enum {
	// "Exists" is also used as lookup result when key doesn't exist or
	// when popping an empty list.
	broker_store_result_tag_exists, // bool
	broker_store_result_tag_size, // count
	broker_store_result_tag_lookup_or_pop, // data
	broker_store_result_tag_keys, // vector
	broker_store_result_tag_snapshot,
} broker_store_result_tag;

/**
 * Whether a data store query was actually fully performed.
 */
typedef enum {
	broker_store_result_status_success,
	broker_store_result_status_failure,
	broker_store_result_status_timeout,
} broker_store_result_status;

/**
 * Reclaim the result of a data store query.
 */
void broker_store_result_delete(broker_store_result* r);

/**
 * @return the status of a data store query.
 */
broker_store_result_status
broker_store_result_get_status(const broker_store_result* r);

/**
 * @return the type of results yielded from a data store query.
 */
broker_store_result_tag
broker_store_result_which(const broker_store_result* r);

/**
 * @return the results yields from a data store query "casted" to a boolean.
 */
int broker_store_result_bool(const broker_store_result* r);

/**
 * @return the results yields from a data store query "casted" to a count.
 */
uint64_t broker_store_result_count(const broker_store_result* r);

/**
 * @return the results yields from a data store query "casted" to a broker
 * data value.
 */
const broker_data* broker_store_result_data(const broker_store_result* r);

/**
 * @return the results yields from a data store query "casted" to a broker
 * data vector.
 */
const broker_vector* broker_store_result_vector(const broker_store_result* r);

/**
 * @see broker::store::response
 */
struct broker_store_response;
typedef struct broker_store_response broker_store_response;

/**
 * Get the query that the data store response is replying to.
 * @return a pointer to the original query associated with the response
 * (still owned by the response object).
 */
broker_store_query*
broker_store_response_get_query(broker_store_response* r);

/**
 * Get the results yielded by performing a data store query.
 * @return a pointer to the query results (still owned by the response object).
 */
broker_store_result*
broker_store_response_get_result(broker_store_response* r);

/**
 * Get the cookie associated with the data store query associated with this
 * response.
 * @return the cookie (opaque memory address) that was used for the query
 * that the response is replying to.
 */
void* broker_store_response_get_cookie(broker_store_response* r);

/**
 * A double-ended queue of data store responses.
 */
struct broker_deque_of_store_response;
typedef struct broker_deque_of_store_response broker_deque_of_store_response;

/**
 * Reclaim the container and all messages in it.
 */
void broker_deque_of_store_response_delete(broker_deque_of_store_response* d);

/**
 * @return the number of messages in the container.
 */
size_t
broker_deque_of_store_response_size(const broker_deque_of_store_response* d);

/**
 * Retrieve a single message from the container.
 * @param d the container of messages to inspect.
 * @param idx the index of the message to retrieve (must be a valid index).
 * @return a pointer to the retrieved message which is still owned by the
 * container.
 */
broker_store_response*
broker_deque_of_store_response_at(broker_deque_of_store_response* d,
                                  size_t idx);

/**
 * Remove a message from the container.
 * @param d the container to modify.
 * @param idx the index of the message to remove (must be a valid index).
 * Deques are best at removing from the ends of the container.
 */
void broker_deque_of_store_response_erase(broker_deque_of_store_response* d,
                                          size_t idx);

/**
 * @see broker::store::response_queue
 */
struct broker_store_response_queue;
typedef struct broker_store_response_queue broker_store_response_queue;

/**
 * @return a file descriptor that is ready for reading when the queue is
 * non-empty.  Suitable for use with poll, select, etc.
 */
int broker_store_response_queue_fd(const broker_store_response_queue* q);

/**
 * @return the contents of the queue at time of call, which may be empty.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_store_response*
broker_store_response_queue_want_pop(const broker_store_response_queue* q);

/**
 * @return the contents of the queue at time of call, which must contain
 * at least one item.  The call will block until at least one item is ready.
 * The caller must manage the memory allocated to the container object.
 */
broker_deque_of_store_response*
broker_store_response_queue_need_pop(const broker_store_response_queue* q);

/**
 * @see broker::store::frontend
 */
struct broker_store_frontend;
typedef struct broker_store_frontend broker_store_frontend;

/**
 * @see broker::store::frontend
 * @return a new frontend data store for the caller to manage.
 * Or returns null if sufficient memory could not be allocated.
 */
broker_store_frontend*
broker_store_frontend_create(const broker_endpoint* e,
                             const broker_string* master_name);

/**
 * Reclaim a data store frontend.
 */
void broker_store_frontend_delete(broker_store_frontend* f);

/**
 * @return the name of the associated master data store.
 */
const broker_string* broker_store_frontend_id(const broker_store_frontend* f);

/**
 * @see broker::frontend::responses
 * @return a pointer to a queue containing responses to queries against the
 * store.
 */
const broker_store_response_queue*
broker_store_frontend_responses(const broker_store_frontend* f);

/**
 * @see broker::frontend::insert
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key/value.
 */
int broker_store_frontend_insert(const broker_store_frontend* f,
                                 const broker_data* k,
                                 const broker_data* v);

/**
 * @see broker::frontend::insert
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key/value/expiration.
 */
int
broker_store_frontend_insert_with_expiry(const broker_store_frontend* f,
                                         const broker_data* k,
                                         const broker_data* v,
                                         const broker_store_expiration_time* e);

/**
 * @see broker::frontend::erase
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key.
 */
int broker_store_frontend_erase(const broker_store_frontend* f,
                                const broker_data* k);

/**
 * @see broker::frontend::clear
 */
void broker_store_frontend_clear(const broker_store_frontend* f);

/**
 * @see broker::frontend::increment
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key.
 */
int broker_store_frontend_increment(const broker_store_frontend* f,
                                    const broker_data* k, int64_t by);

/**
 * @see broker::frontend::add_to_set
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key/element.
 */
int broker_store_frontend_add_to_set(const broker_store_frontend* f,
                                     const broker_data* k,
                                     const broker_data* element);

/**
 * @see broker::frontend::remove_from_set
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key/element.
 */
int broker_store_frontend_remove_from_set(const broker_store_frontend* f,
                                          const broker_data* k,
                                          const broker_data* element);

/**
 * @see broker::frontend::push_left
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key/element.
 */
int broker_store_frontend_push_left(const broker_store_frontend* f,
                                    const broker_data* k,
                                    const broker_vector* items);

/**
 * @see broker::frontend::push_right
 * @return not-zero on success, or zero if memory could not be allocated to
 * copy the key/element.
 */
int broker_store_frontend_push_right(const broker_store_frontend* f,
                                     const broker_data* k,
                                     const broker_vector* items);

/**
 * @see broker::frontend::request(query)
 * @return the result of the query which the caller must manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_result*
broker_store_frontend_request_blocking(const broker_store_frontend* f,
                                       const broker_store_query* q);

/**
 * @see broker::frontend::request(query, std::chrono::duration<double>, void*)
 * @return not-zero if the asynchronous query was made successfully (the
 * result will eventually be available in the frontend's response queue), or
 * zero if memory could not be allocated to copy data in to the query.
 */
int broker_store_frontend_request_nonblocking(const broker_store_frontend* f,
                                              const broker_store_query* q,
                                              double timeout, void* cookie);

/**
 * @see broker::store::sqlite_backend
 */
struct broker_store_sqlite_backend;
typedef struct broker_store_sqlite_backend broker_store_sqlite_backend;

/**
 * Create a new sqlite data store backend.
 * @return a new sqlite data store backend for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_sqlite_backend* broker_store_sqlite_backend_create();

/**
 * Reclaim a backend.
 */
void broker_store_sqlite_backend_delete(broker_store_sqlite_backend* b);

/**
 * @see broker::store::sqlite_backend::open (default pragma argument is used)
 * @param b the backend to open
 * @param path the path to use for persistent sqlite storage.
 * @return same as @see broker::store::sqlite_backend::open
 */
int broker_store_sqlite_backend_open(broker_store_sqlite_backend* b,
                                     const char* path);

/**
 * @see broker::store::sqlite_backend::pragma
 */
int broker_store_sqlite_backend_pragma(broker_store_sqlite_backend* b,
                                       const char* pragma);

/**
 * @see broker::store::sqlite_backend::last_error_code
 */
int broker_store_sqlite_backend_last_error_code(
        const broker_store_sqlite_backend* b);

/**
 * @see broker::store::sqlite_backend::last_error
 */
const char* broker_store_sqlite_backend_last_error(
        const broker_store_sqlite_backend* b);

/**
 * @see broker::store::rocksdb_backend
 */
struct broker_store_rocksdb_backend;
typedef struct broker_store_rocksdb_backend broker_store_rocksdb_backend;

/**
 * Create a new rocksdb data store backend.
 * @return a new rocksdb data store backend for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_rocksdb_backend* broker_store_rocksdb_backend_create();

/**
 * Reclaim a backend.
 */
void broker_store_rocksdb_backend_delete(broker_store_rocksdb_backend* b);

/**
 * Open the rocksdb database.
 * @param b the rocksdb backend to open.
 * @param path the path to supply for persistent rocksdb storage.
 * @param create_if_missing set to 0 if the database should not be created
 * if it doesn't yet exist.
 * @return same as @see broker::store::rocksdb_backend::open
 */
int broker_store_rocksdb_backend_open(broker_store_rocksdb_backend* b,
                                     const char* path, int create_if_missing);

/**
 * @see broker::store::rocksdb_backend::last_error
 */
const char* broker_store_rocksdb_backend_last_error(
        const broker_store_rocksdb_backend* b);

/**
 * Create a new master data store using the in-memory storage backend.
 * @param e the endpoint to attach the master store to.
 * @param name a unique name to give the master data store.
 * @return a new store object for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_frontend*
broker_store_master_create_memory(const broker_endpoint* e,
                                  const broker_string* name);

/**
 * Create a new master data store using the sqlite storage backend.
 * @param e the endpoint to attach the master store to.
 * @param name a unique name to give the master data store.
 * @param b the backend to use.  On a successful call, it assumes ownership
 * of the backend.  The caller may call "delete" on it whenever, but that is the
 * only valid operation that they may perform.
 * @return a new store object for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_frontend*
broker_store_master_create_sqlite(const broker_endpoint* e,
                                  const broker_string* name,
                                  broker_store_sqlite_backend* b);

/**
 * Create a new master data store using the rocksdb storage backend (if it is
 * supported).
 * @param e the endpoint to attach the master store to.
 * @param name a unique name to give the master data store.
 * @param b the backend to use.  On a successful call, it assumes ownership
 * of the backend.  The caller may call "delete" on it whenever, but that is the
 * only valid operation that they may perform.
 * @return a new store object for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_frontend*
broker_store_master_create_rocksdb(const broker_endpoint* e,
                                   const broker_string* name,
                                   broker_store_rocksdb_backend* b);

/**
 * Create a new data store clone using the in-memory storage backend.
 * @param e the endpoint to attach the store to.
 * @param name the name of the master data store to clone.
 * @param resync_interval number of seconds at which to re-attempt
 * synchronizing with the master data store should the connection be lost.
 * @return a new store object for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_frontend*
broker_store_clone_create_memory(const broker_endpoint* e,
                                 const broker_string* name,
                                 double resync_interval);

/**
 * Create a new data store clone using the sqlite storage backend.
 * @param e the endpoint to attach the store to.
 * @param name the name of the master data store to clone.
 * @param resync_interval number of seconds at which to re-attempt
 * synchronizing with the master data store should the connection be lost.
 * @param b the backend to use.  On a successful call, it assumes ownership
 * of the backend.  The caller may call "delete" on it whenever, but that is the
 * only valid operation that they may perform.
 * @return a new store object for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_frontend*
broker_store_clone_create_sqlite(const broker_endpoint* e,
                                 const broker_string* name,
                                 double resync_interval,
                                 broker_store_sqlite_backend* b);

/**
 * Create a new data store clone using the rocksdb storage backend (if it is
 * supported).
 * @param e the endpoint to attach the store to.
 * @param name the name of the master data store to clone.
 * @param resync_interval number of seconds at which to re-attempt
 * synchronizing with the master data store should the connection be lost.
 * @param b the backend to use.  On a successful call, it assumes ownership
 * of the backend.  The caller may call "delete" on it whenever, but that is the
 * only valid operation that they may perform.
 * @return a new store object for the caller to manage.
 * Or null if sufficient memory could not be allocated.
 */
broker_store_frontend*
broker_store_clone_create_rocksdb(const broker_endpoint* e,
                                  const broker_string* name,
                                  double resync_interval,
                                  broker_store_rocksdb_backend* b);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // BROKER_BROKER_H
