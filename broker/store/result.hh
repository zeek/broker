#ifndef BROKER_STORE_RESULT_HH
#define BROKER_STORE_RESULT_HH

#include <broker/data.hh>
#include <broker/store/snapshot.hh>
#include <cstdint>
#include <unordered_set>

namespace broker { namespace store {

class result : util::equality_comparable<result> {
public:

	// This tag indicates which value of the variant is currently valid.
	// For status other than success, it's arbitrarily tagged w/ exists_result.
	enum class type: uint8_t {
		exists_result,
		size_result,
		lookup_result,
		keys_result,
		snapshot_result,
	};

	enum class status : uint8_t {
		success,
		failure,  // Query could not be fulfilled.
		timeout
	} stat;

	using result_data = util::variant<type, bool, uint64_t, data,
	                                  std::unordered_set<data>, snapshot>;

	result_data value;

	result()
		: stat(status::failure), value()
		{ }

	result(status s)
		: stat(s), value()
		{ }

	result(result_data rd)
		: stat(status::success), value(std::move(rd))
		{ }
};

inline bool operator==(const result& lhs, const result& rhs)
    { return lhs.stat == rhs.stat && lhs.value == rhs.value; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_RESULT_HH
