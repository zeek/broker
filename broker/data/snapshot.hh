#ifndef BROKER_DATA_SNAPSHOT_HH
#define BROKER_DATA_SNAPSHOT_HH

#include <broker/data/types.hh>
#include <broker/data/sequence_num.hh>
#include <unordered_map>

namespace broker { namespace data {

struct snapshot {
	std::unordered_map<key, value> datastore;
	sequence_num sn;
};

inline bool operator==(const snapshot& lhs, const snapshot& rhs)
    { return lhs.sn == rhs.sn && lhs.datastore == rhs.datastore; }

} // namespace data
} // namespace broker

#endif // BROKER_DATA_SNAPSHOT_HH
