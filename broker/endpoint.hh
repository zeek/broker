#ifndef BROKER_ENDPOINT_HH
#define BROKER_ENDPOINT_HH

#include <broker/peering.hh>
#include <broker/print_msg.hh>
#include <broker/log_msg.hh>
#include <broker/event_msg.hh>
#include <memory>
#include <string>
#include <cstdint>
#include <chrono>

namespace broker {

/**
 * A local broker endpoint, the main entry point for communicating with peer.
 */
class endpoint {
public:

	/**
	 * Create a local broker endpoint.
	 * @param name a descriptive name for this endpoint.
	 * @param flags tune the behavior of the endpoint.  No flags exist yet.
	 */
	endpoint(std::string name, int flags = 0);

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
	 * @return true if the endpoint is now listening, else false.  For the
	 *         later case, LastError() contains descriptive error text and
	 *         LastErrno(), if non-zero, is an error code set by @p bind(2).
	 */
	bool listen(uint16_t port, const char* addr = nullptr);

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
	 * @param peerno a peer object previously returned by endpoint::peer.
	 * @return false if no such peer associated exists, else true (and the
	 *         peering is no more).
	 */
	bool unpeer(peering p);

	/**
	 * Sends a message string to all print_queue's for a given topic that are
	 * connected to this endpoint directly or indirectly through peer endpoints.
	 * @param topic the topic associated with the message.
	 * @param msg a message to send all queues subscribed for the topic.
	 */
	void print(std::string topic, print_msg msg) const;

	/**
	 * Sends a log to all log_queue's for a given topic that are connected to
	 * this endpoint directly or indirectly through peer endpoints.
	 * @param topic the topic associated with the log.
	 * @param msg a logging message to send all queues subscribed for the topic.
	 */
	void log(std::string topic, log_msg msg) const;

	/**
	 * Sends an event to all event_queue's for a given topic that are connected
	 * to this endpoint directly or indirectly through peer endpoings.
	 * @param topic the topic associated with the event.
	 * @param msg an event to send all queues subscribed for the topic.
	 */
	void event(std::string topic, event_msg msg) const;

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
