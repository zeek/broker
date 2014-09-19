#ifndef BROKER_STORE_SNAPSHOT_HH
#define BROKER_STORE_SNAPSHOT_HH

#include <broker/store/types.hh>
#include <broker/store/sequence_num.hh>
#include <unordered_map>

namespace broker { namespace store {

struct snapshot {
	std::unordered_map<key, value> datastore;
	sequence_num sn;
};

inline bool operator==(const snapshot& lhs, const snapshot& rhs)
    { return lhs.sn == rhs.sn && lhs.datastore == rhs.datastore; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_SNAPSHOT_HH
