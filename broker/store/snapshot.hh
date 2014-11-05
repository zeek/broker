#ifndef BROKER_STORE_SNAPSHOT_HH
#define BROKER_STORE_SNAPSHOT_HH

#include <broker/data.hh>
#include <broker/store/expiration_time.hh>
#include <broker/store/sequence_num.hh>
#include <unordered_map>

namespace broker { namespace store {

/**
 * A snapshot of a data store's contents along with the sequence number
 * that corresponds to it.
 */
struct snapshot {
	std::unordered_map<data, value> datastore;
	sequence_num sn;
};

inline bool operator==(const snapshot& lhs, const snapshot& rhs)
    { return lhs.sn == rhs.sn && lhs.datastore == rhs.datastore; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_SNAPSHOT_HH
