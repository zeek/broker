#ifndef BROKER_UTIL_MISC_HH
#define BROKER_UTIL_MISC_HH

#include "broker/data.hh"

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

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_MISC_HH
