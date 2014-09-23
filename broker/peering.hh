#ifndef BROKER_PEERING_HH
#define BROKER_PEERING_HH

#include <memory>
#include <utility>
#include <cstdint>
#include <string>
#include <chrono>

namespace broker { class peering; }
namespace std { template<> struct std::hash<broker::peering>; }

namespace broker {

class endpoint;

/**
 * Contains information about a peering between two endpoints.
 */
class peering {
friend class endpoint;
friend struct std::hash<peering>;

public:

	/**
	 * Construct an uninitialized peering object.
	 */
	peering();

	/**
	  * Destruct a peering object (not the actual connection between endpoints).
	  */
	~peering();

	/**
	  * Copy a peering object.
	  */
	peering(const peering& other);

	/**
	 * Steal a peering object.
	 */
	peering(peering&& other);

	/**
	 * Replace a peering object with a copy of another.
	 */
	peering& operator=(const peering& other);

	/**
	 * Replace a peering object by stealing another.
	 */
	peering& operator=(peering&& other);

	/**
	 * @return whether the peering is between a local and remote endpoint.
	 */
	bool remote() const;

	/**
	 * @return the host and port of a remote endpoint.
	 */
	const std::pair<std::string, uint16_t>& remote_tuple() const;

	/**
	 * A possible return value from @see peering::handshake().
	 */
	enum class handshake_status: uint8_t {
		success,
		timeout,
		invalid, // incompatible peer
	};

	/**
	 * Blocks until a handshake between the two peer endpoints completes.
	 * The handshake involves the endpoints exchanging the topics to which
	 * they are currently subscribed.  If a handshake is not performed, a
	 * message sent to a local endpoint may not be forwarded to a peer that has
	 * just connected because the subscriptions have not yet been exchanged.
	 */
	handshake_status handshake() const;

	/**
	 * Waits until a handshake between the two peer endpoints complete or
	 * a given timeout duration has been reached.  @see peering::handshake().
	 */
	handshake_status handshake(std::chrono::duration<double> timeout) const;

	/**
	 * False if the peering is not yet initialized, else true.
	 */
	explicit operator bool() const;

	/**
	 * @return true if two peering objects are equal.
	 */
	bool operator==(const peering& rhs) const;

private:

	class impl;

	peering(std::unique_ptr<impl> p);

	std::unique_ptr<impl> pimpl;
};

} // namespace broker

namespace std {
template <> struct hash<broker::peering> {
	size_t operator()(const broker::peering& p) const;
};
}

#endif // BROKER_PEERING_HH
