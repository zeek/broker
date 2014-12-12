#ifndef BROKER_STORE_QUERY_HH
#define BROKER_STORE_QUERY_HH

#include <broker/data.hh>
#include <broker/store/result.hh>
#include <broker/store/backend.hh>
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
		pop_left,
		pop_right,
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
	result process(backend& s) const
		{
		switch ( type ) {
		case tag::pop_left:
			{
			if ( auto r = s.pop_left(k) )
				{
				if ( *r )
					return result(std::move(**r));
				else
					return result(false);
				}
			return result(result::status::failure);
			}
		case tag::pop_right:
			{
			if ( auto r = s.pop_right(k) )
				{
				if ( *r )
					return result(std::move(**r));
				else
					return result(false);
				}
			return result(result::status::failure);
			}
		case tag::lookup:
			{
			if ( auto r = s.lookup(k) )
				{
				if ( *r )
					return result(std::move(**r));
				else
					// Key doesn't exist.
					return result(false);
				}
			return result(result::status::failure);
			}
		case tag::exists:
			{
			if ( auto r = s.exists(k) )
				return result(std::move(*r));
			return result(result::status::failure);
			}
		case tag::keys:
			{
			if ( auto r = s.keys() )
				return result(std::move(*r));
			return result(result::status::failure);
			}
		case tag::size:
			{
			if ( auto r = s.size() )
				return result(std::move(*r));
			return result(result::status::failure);
			}
		case tag::snapshot:
			{
			if ( auto r = s.snap() )
				return result(std::move(*r));
			return result(result::status::failure);
			}
		default:
			assert(false);
		}
		}
};

inline bool operator==(const query& lhs, const query& rhs)
    { return lhs.type == rhs.type && lhs.k == rhs.k; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_QUERY_HH
