#ifndef BROKER_UTIL_MISC_HH
#define BROKER_UTIL_MISC_HH

#include "broker/data.hh"
#include "broker/store/expiration_time.hh"

namespace broker {
namespace util {

/**
 * A visitor for incrementing integral data.
 */
struct increment_visitor {
	using result_type = bool;

	int64_t by;

	result_type operator()(uint64_t& val) const
		{ val += by; return true; }

	result_type operator()(int64_t& val) const
		{ val += by; return true; }

	template <typename T>
	result_type operator()(const T&) const
		{ return false; }
};

/**
 * Update the last modification time of an expiration value.
 * @param et an existing expiration time to update
 * @param mod_time the new last-modified time.
 * @return the new expiration value (if it needed to be updated).
 */
optional<store::expiration_time>
update_last_modification(optional<store::expiration_time>& et, double mod_time);

/**
 * Increment integral data by a given amount.
 * @param d data to increment.  If disengaged, it is implicitly set to 0.
 * @param by the amount to integrate by.
 * @param error_msg an optional location to put an error message.
 * @return false on failure (i.e. the data type is not an integral type).
 */
bool increment_data(optional<data>& d, int64_t by, std::string* error_msg);

/**
 * @see increment_data
 */
bool increment_data(data& d, int64_t by, std::string* error_msg);

/**
 * Add data element to a set.
 * @param s Tte set to modify.
 * @param element The element to insert.
 * @param error_msg an optional location to put an error message.
 * @return false on failure (i.e. the data type is not a set).
 */
bool add_data_to_set(optional<data>& s, data element, std::string* error_msg);

/**
 * @see add_data_to_set
 */
bool add_data_to_set(data& s, data element, std::string* error_msg);

/**
 * Remove data element from a set.
 * @param s the set to modify.
 * @param element the element to remove.
 * @param error_msg an optional location to put an error message.
 * @return false on failure (i.e. the data type is not a set).
 */
bool remove_data_from_set(optional<data>& s, const data& element,
                          std::string* error_msg);

/**
 * @see remove_data_from_set
 */
bool remove_data_from_set(data& s, const data& element, std::string* error_msg);

/**
 * Push an item to the head of a vector.
 * @param v the vector to modify.
 * @param items the elements to add to the vector
 * @param error_msg an optional location to put an error message.
 * @return false on failure (i.e. the data type is not a vector).
 */
bool push_left(optional<data>& v, vector items, std::string* error_msg);

/**
 * @see push_left
 */
bool push_left(data& v, vector items, std::string* error_msg);

/**
 * Push an item to the tail of a vector.
 * @param v the vector to modify.
 * @param items the elements to add to the vector
 * @param error_msg an optional location to put an error message.
 * @return false on failure (i.e. the data type is not a vector).
 */
bool push_right(optional<data>& v, vector items, std::string* error_msg);

/**
 * @see push_right
 */
bool push_right(data& v, vector items, std::string* error_msg);

/**
 * Pop the first item in a vector.
 * @param v the vector to modify
 * @param error_msg an optional location to put an error message.
 * @param shrink whether to consider shrinking the capacity of \a v
 * @return nil on failure (i.e. the data type is not a vector), or first item
 * if one was available.
 */
optional<optional<data>> pop_left(optional<data>& v, std::string* error_msg,
                                  bool shrink = false);

/**
 * @see pop_left
 */
optional<optional<data>> pop_left(data& v, std::string* error_msg,
                                  bool shrink = false);

/**
 * Pop the last item in a vector.
 * @param v the vector to modify
 * @param error_msg an optional location to put an error message.
 * @param shrink whether to consider shrinking the capacity of \a v
 * @return nil on failure (i.e. the data type is not a vector), or last item
 * if one was available.
 */
optional<optional<data>> pop_right(optional<data>& v, std::string* error_msg,
                                   bool shrink = false);

/**
 * @see pop_right
 */
optional<optional<data>> pop_right(data& v, std::string* error_msg,
                                   bool shrink = false);

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_MISC_HH
