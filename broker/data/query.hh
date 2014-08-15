#ifndef BROKER_DATA_QUERY_HH
#define BROKER_DATA_QUERY_HH

#include <broker/data/types.hh>
#include <broker/data/result.hh>
#include <broker/data/store.hh>
#include <cstdint>

namespace broker { namespace data {

class query {
public:

	enum class type : uint8_t {
		lookup,
		exists,
		keys,
		size,
		snapshot
	} tag;

	key k;

	query(type t = type::lookup, key arg_k = {})
		: tag(t), k(arg_k)
		{ }

	result process(const store& s) const
		{
		switch ( tag ) {
		case type::lookup:
			return result(s.lookup(k));
		case type::exists:
			return result(s.exists(k));
		case type::keys:
			return result(s.keys());
		case type::size:
			return result(s.size());
		case type::snapshot:
			return result(s.snap());
		}
		}
};

inline bool operator==(const query& lhs, const query& rhs)
    { return lhs.tag == rhs.tag && lhs.k == rhs.k; }

} // namespace data
} // namespace broker

#endif // BROKER_DATA_QUERY_HH
