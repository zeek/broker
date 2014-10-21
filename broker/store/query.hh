#ifndef BROKER_STORE_QUERY_HH
#define BROKER_STORE_QUERY_HH

#include <broker/data.hh>
#include <broker/store/result.hh>
#include <broker/store/store.hh>
#include <cstdint>

namespace broker { namespace store {

/**
 * A generic data store query.
 */
class query {
public:

	/**
	 * Distinguishes particular types of queries.
	 */
	enum class tag : uint8_t {
		lookup,
		exists,
		keys,
		size,
		snapshot
	} type;

	data k;

	/**
	 * Construct a query.
	 * @param t the type of query.
	 * @param arg_k additional data if needed by the query type.
	 */
	query(tag t = tag::lookup, data arg_k = {})
		: type(t), k(arg_k)
		{ }

	/**
	 * Obtain an answer to a query.
	 * @param s a storage backend to query against.
	 * @return the result of the query.
	 */
	result process(const store& s) const
		{
		switch ( type ) {
		case tag::lookup:
			{
			if ( auto r = s.lookup(k) )
				return result(std::move(*r));
			return result(false);
			}
		case tag::exists:
			return result(s.exists(k));
		case tag::keys:
			return result(s.keys());
		case tag::size:
			return result(s.size());
		case tag::snapshot:
			return result(s.snap());
		}
		}
};

inline bool operator==(const query& lhs, const query& rhs)
    { return lhs.type == rhs.type && lhs.k == rhs.k; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_QUERY_HH
