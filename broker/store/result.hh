#ifndef BROKER_STORE_RESULT_HH
#define BROKER_STORE_RESULT_HH

#include <broker/data.hh>
#include <broker/store/snapshot.hh>
#include <cstdint>
#include <vector>

namespace broker { namespace store {

/**
 * The corresponding "answer" to a data store query.
 */
class result : util::equality_comparable<result> {
public:

	/**
	 * A tag indicating which value of the variant is currently valid.
	 * For status other than success, it's arbitrarily tagged with
	 * exists_result.
	 */
	enum class tag: uint8_t {
		// "Exists" is also used as lookup result when key doesn't exist or
		// when popping an empty list.
		exists_result,
		size_result,
		lookup_or_pop_result,
		keys_result,
		snapshot_result,
	};

	/**
	 * The status of the query result -- whether it is valid or not as
	 * well as some indication as to why.
	 */
	enum class status : uint8_t {
		success,
		failure,  // Query could not be fulfilled.
		timeout
	} stat;

	// NIT: maybe deque instead of vector for keys/snapshot results.
	typedef util::variant<tag,
	                      bool, uint64_t, data, std::vector<data>, snapshot>
	        type;

	type value;

	/**
	 * Default construct a result in a failed state.
	 */
	result()
		: stat(status::failure), value()
		{ }

	/**
	 * Construct a result in a given state.
	 */
	result(status s)
		: stat(s), value()
		{ }

	/**
	 * Construct a successful result from given result data.
	 */
	result(type rd)
		: stat(status::success), value(std::move(rd))
		{ }
};

inline bool operator==(const result& lhs, const result& rhs)
    { return lhs.stat == rhs.stat && lhs.value == rhs.value; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_RESULT_HH
