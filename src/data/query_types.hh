#ifndef BROKER_DATA_QUERY_TYPES_HH
#define BROKER_DATA_QUERY_TYPES_HH

#include "broker/data/types.hh"
#include "../subscription.hh"

#include <caf/actor.hpp>

namespace broker { namespace data {

struct snapshot_request {
	subscription st;
	caf::actor clone;
};

inline bool operator==(const snapshot_request& lhs, const snapshot_request& rhs)
    { return lhs.st == rhs.st && lhs.clone == rhs.clone; }

struct lookup_request {
	subscription st;
	key key;
};

inline bool operator==(const lookup_request& lhs, const lookup_request& rhs)
    { return lhs.st == rhs.st && lhs.key == rhs.key; }

struct has_key_request {
	subscription st;
	key key;
};

inline bool operator==(const has_key_request& lhs, const has_key_request& rhs)
    { return lhs.st == rhs.st && lhs.key == rhs.key; }

struct keys_request {
	subscription st;
};

inline bool operator==(const keys_request& lhs, const keys_request& rhs)
    { return lhs.st == rhs.st; }

struct size_request {
	subscription st;
};

inline bool operator==(const size_request& lhs, const size_request& rhs)
    { return lhs.st == rhs.st; }

} // namespace data
} // namespace broker

#endif // BROKER_DATA_QUERY_TYPES_HH
