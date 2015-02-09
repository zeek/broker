#ifndef BROKER_INCOMING_CONNECTION_STATUS_HH
#define BROKER_INCOMING_CONNECTION_STATUS_HH

#include <broker/peering.hh>
#include <string>

namespace broker {

/**
 * A notification regarding an incoming attempt to establish a peering
 * relationship between two endpoints.
 */
struct incoming_connection_status {

	/**
	 * The type of status notification.
	 */
	enum class tag : uint8_t {
		established,
		disconnected,
	};

	/**
	 * A notification regarding the latest known status of the peering.
	 */
	tag status;

	/**
	 * Contains a name the peer chose for itself.
	 */
	std::string peer_name;
};

inline bool operator==(const incoming_connection_status& lhs,
                       const incoming_connection_status& rhs)
	{ return lhs.status == rhs.status && lhs.peer_name == rhs.peer_name; }

} // namespace broker

#endif // BROKER_INCOMING_CONNECTION_STATUS_HH
