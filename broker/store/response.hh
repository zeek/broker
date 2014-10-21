#ifndef BROKER_STORE_RESPONSE_HH
#define BROKER_STORE_RESPONSE_HH

#include <broker/store/query.hh>
#include <broker/store/result.hh>

namespace broker { namespace store {

/**
 * A full response to a data store query contains the original query, the
 * result of that query, and an associated cookie provided by the requester.
 */
struct response {
	query request;
	result reply;
	void* cookie;
};

inline bool operator==(const response& lhs, const response& rhs)
	{
	return lhs.request == rhs.request &&
	       lhs.reply == rhs.reply &&
	       lhs.cookie == rhs.cookie;
	}

} // namespace store
} // namespace broker

#endif // BROKER_STORE_RESPONSE_HH
