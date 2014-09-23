#ifndef BROKER_STORE_QUERY_HH
#define BROKER_STORE_QUERY_HH

#include <broker/data.hh>
#include <broker/store/result.hh>
#include <broker/store/store.hh>
#include <cstdint>

namespace broker { namespace store {

class query {
public:

	enum class type : uint8_t {
		lookup,
		exists,
		keys,
		size,
		snapshot
	} tag;

	data k;

	query(type t = type::lookup, data arg_k = {})
		: tag(t), k(arg_k)
		{ }

	result process(const store& s) const
		{
		switch ( tag ) {
		case type::lookup:
			{
			if ( auto r = s.lookup(k) )
				return result(std::move(*r));
			return result(false);
			}
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

} // namespace store
} // namespace broker

#endif // BROKER_STORE_QUERY_HH
