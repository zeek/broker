#ifndef BROKER_DATA_RESPONSE_HH
#define BROKER_DATA_RESPONSE_HH

#include <broker/data/query.hh>
#include <broker/data/result.hh>

namespace broker { namespace data {

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

} // namespace data
} // namespace broker

#endif // BROKER_DATA_RESPONSE_HH
