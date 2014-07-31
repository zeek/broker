#ifndef BROKER_DATA_REQUESTMSGS_HH
#define BROKER_DATA_REQUESTMSGS_HH

#include "broker/data/types.hh"
#include "../Subscription.hh"

#include <caf/actor.hpp>

namespace broker { namespace data {

struct SnapshotRequest {
	SubscriptionTopic st;
	caf::actor clone;
};

inline bool operator==(const SnapshotRequest& lhs, const SnapshotRequest& rhs)
    { return lhs.st == rhs.st && lhs.clone == rhs.clone; }

struct LookupRequest {
	SubscriptionTopic st;
	Key key;
};

inline bool operator==(const LookupRequest& lhs, const LookupRequest& rhs)
    { return lhs.st == rhs.st && lhs.key == rhs.key; }

struct HasKeyRequest {
	SubscriptionTopic st;
	Key key;
};

inline bool operator==(const HasKeyRequest& lhs, const HasKeyRequest& rhs)
    { return lhs.st == rhs.st && lhs.key == rhs.key; }

struct KeysRequest {
	SubscriptionTopic st;
};

inline bool operator==(const KeysRequest& lhs, const KeysRequest& rhs)
    { return lhs.st == rhs.st; }

struct SizeRequest {
	SubscriptionTopic st;
};

inline bool operator==(const SizeRequest& lhs, const SizeRequest& rhs)
    { return lhs.st == rhs.st; }

} // namespace data
} // namespace broker

#endif // BROKER_DATA_REQUESTMSGS_HH
