#ifndef BROKER_PEER_STATUS_HH
#define BROKER_PEER_STATUS_HH

#include <broker/peering.hh>
#include <string>

namespace broker {

/**
 * A notification regarding a peering relationship between two endpoints.
 */
struct peer_status {

	/**
	 * The type of status notification.
	 */
	enum class type : uint8_t {
		established,
		disconnected,
		incompatible,
	};

	/**
	 * The identity of a peering relationship between two endpoints.
	 */
	peering relation;

	/**
	 * A notification regarding the latest known status of the peering.
	 */
	type status;

	/**
	 * When status is established, contains a name the peer chose for itself.
	 */
	std::string peer_name;
};

inline bool operator==(const peer_status& lhs, const peer_status& rhs)
	{
	return lhs.status == rhs.status &&
	       lhs.relation == rhs.relation;
	}

} // namespace broker

#endif // BROKER_PEER_STATUS_HH
