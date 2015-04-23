#ifndef BROKER_ENDPOINT_HH
#define BROKER_ENDPOINT_HH

#include <broker/topic.hh>
#include <broker/peering.hh>
#include <broker/message.hh>
#include <broker/outgoing_connection_status.hh>
#include <broker/incoming_connection_status.hh>
#include <broker/queue.hh>
#include <memory>
#include <string>
#include <cstdint>
#include <chrono>

namespace broker {

typedef broker::queue<broker::outgoing_connection_status>
outgoing_connection_status_queue;

typedef broker::queue<broker::incoming_connection_status>
incoming_connection_status_queue;

// Endpoint options.

/**
 * Don't restrict message topics that the endpoint publishes to peers.
 */
constexpr int AUTO_PUBLISH = 0x01;

/**
 * Don't restrict what queue topics and store identifiers that the endpoint
 * advertises to peers.
 */
constexpr int AUTO_ADVERTISE = 0x02;

// Messaging modes.

/**
 * Send only to subscribers (e.g. message queue) attached directly to endpoint.
 */
constexpr int SELF = 0x01;

/**
 * Send only to peers of the endpoint that advertise interest in the topic.
 */
constexpr int PEERS = 0x02;

/**
 * Send to peers of the endpoint even if they don't advertise interest in the
 * topic.  This leaves it up to the peer to decide if it can handle the message.
 */
constexpr int UNSOLICITED = 0x04;

/**
 * A local broker endpoint, the main entry point for communicating with peer.
 */
class endpoint {
public:

	/**
	 * Create a local broker endpoint.
	 * @param name a descriptive name for this endpoint.
	 * @param flags tune the behavior of the endpoint.
	 */
	endpoint(std::string name, int flags = AUTO_PUBLISH | AUTO_ADVERTISE);

	/**
	 * Shutdown the local broker endpoint and disconnect from peers.
	 */
	~endpoint();

	/**
	 * Copying endpoint objects is disallowed.
	 */
	endpoint(const endpoint& other) = delete;

	/**
	 * Steal another endpoint.
	 */
	endpoint(endpoint&& other);

	/**
	 * Copying endpoint objects is disallowed.
	 */
	endpoint& operator=(const endpoint& other) = delete;

	/**
	 * Replace endpoint by stealing another.
	 */
	endpoint& operator=(endpoint&& other);

	/**
	 * @return the descriptive name for this endpoint (as given to ctor).
	 */
	const std::string& name() const;

	/**
	 * @return the current option flags used by the endpoint.
	 */
	int flags() const;

	/**
	 * Changes the option flags used by the endpoint.
	 */
	void set_flags(int flags);

	/**
	 * @return an error code associated with the last failed endpoint operation.
	 * If non-zero, it may be passed to broker::strerror() for a description.
	 */
	int last_errno() const;

	/**
	 * @return descriptive error text associated with the last failed endpoint
	 * operation.
	 */
	const std::string& last_error() const;

	/**
	 * Make this local broker endpoint available for remote peer connections.
	 * @param port the TCP port on which to accept connections.
	 * @param addr an address to accept on, e.g. "127.0.0.1".
	 *             A nullptr refers to @p INADDR_ANY.
	 * @param reuse_addr equivalent to behavior of SO_REUSEADDR.
	 * @return true if the endpoint is now listening, else false.  For the
	 *         latter case, last_error() contains descriptive error text and
	 *         last_errno(), if non-zero, is an error code set by @p bind(2).
	 */
	bool listen(uint16_t port, const char* addr = nullptr,
	            bool reuse_addr = true);

	/**
	 * Connect to a remote endpoint.
	 * @param addr an address to connect to, e.g. "localhost" or "127.0.0.1".
	 * @param port the TCP port on which the remote is listening.
	 * @param retry an interval at which to retry establishing the connection
	 *        with the remote peer.
	 * @return a peer object that this endpoint can use to identify the
	 *         particular peer established by this method.
	 */
	peering peer(std::string addr, uint16_t port,
	             std::chrono::duration<double> retry = std::chrono::seconds(5));

	/**
	 * Connect to a local endpoint.
	 * @param e another local endpoint.
	 * @return a peer object that this endpoint can use to identify the
	 *         particular peer established by this method.
	 */
	peering peer(const endpoint& e);

	/**
	 * Remove a connection to a peer endpoint.
	 * @param p a peer object previously returned by endpoint::peer.
	 * @return false if no such associated peer exists, else true (and the
	 *         peering is no more).
	 */
	bool unpeer(peering p);

	/**
	 * @return a queue that may be used to inspect the results of a peering
	 * connection attempt. e.g. established, disconnected, incompatible, etc.
	 * Until one checks the queue for a result that indicates the peering is
	 * established, messages sent using endpoint::send(), are not guaranteed
	 * to be delivered to the peer as it may still be in the process of
	 * registering its subscriptions.
	 */
	const outgoing_connection_status_queue& outgoing_connection_status() const;

	/**
	 * @return a queue that may be used to inspect the status of an incoming
	 * peer connection. e.g. established, disconnected.
	 */
	const incoming_connection_status_queue& incoming_connection_status() const;

	/**
	 * Sends a message to all message_queue's that are registered for a given
	 * topic and either connected to this endpoint directly or indirectly
	 * through peer endpoints.
	 * @param t the topic name associated with the message.
	 * @param msg a message to send all queues subscribed for the topic.
	 * @param flags tunes the messaging mode behavior.
	 */
	void send(topic t, message msg, int flags = SELF | PEERS) const;

	/**
	 * Allow the endpoint to publish messages with the given topic to peers.
	 * No effect while the endpoint uses the AUTO_PUBLISH flag.
	 */
	void publish(topic t);

	/**
	 * Stop allowing the endpoint to publish messages with the given topic to
	 * peers.  No effect while the endpoint uses the AUTO_PUBLISH flag.
	 */
	void unpublish(topic t);

	/**
	 * Allow advertising a given queue topic or store identifier to peers.
	 * No effect while the endpoint uses the AUTO_ADVERTISE flag.
	 */
	void advertise(std::string t);

	/**
	 * Don't allow advertising a given queue topic or store identifier to peers.
	 * No effect while the endpoint uses the AUTO_ADVERTISE flag.
	 */
	void unadvertise(std::string t);

	/**
	 * @return a unique handle for the endpoint.
	 */
	void* handle() const;

private:

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_ENDPOINT_HH
