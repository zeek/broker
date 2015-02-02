#ifndef BROKER_STORE_SNAPSHOT_HH
#define BROKER_STORE_SNAPSHOT_HH

#include <broker/data.hh>
#include <broker/store/value.hh>
#include <broker/store/sequence_num.hh>
#include <vector>
#include <utility>

namespace broker { namespace store {

/**
 * A snapshot of a data store's contents along with the sequence number
 * that corresponds to it.
 */
struct snapshot {
	std::vector<std::pair<data, value>> entries;
	sequence_num sn;
};

inline bool operator==(const snapshot& lhs, const snapshot& rhs)
    { return lhs.sn == rhs.sn && lhs.entries == rhs.entries; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_SNAPSHOT_HH
