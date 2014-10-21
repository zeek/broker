#ifndef BROKER_PEER_STATUS_QUEUE_HH
#define BROKER_PEER_STATUS_QUEUE_HH

#include <broker/peer_status.hh>
#include <memory>
#include <deque>

namespace broker {

class endpoint;

/**
 * Stores peer_status notifications awaiting retrieval/processing.
 */
class peer_status_queue {
friend class endpoint;

public:

	/**
	 * Construct queue.
	 */
	peer_status_queue();

	/**
	  * Destruct queue.
	  */
	~peer_status_queue();

	/**
	 * Copying a queue is disallowed.
	 */
	peer_status_queue(const peer_status_queue& other) = delete;

	/**
	 * Steal a queue.
	 */
	peer_status_queue(peer_status_queue&& other);

	/**
	 * Copying a queue is disallowed.
	 */
	peer_status_queue& operator=(const peer_status_queue& other) = delete;

	/**
	 * Replace queue by stealing another.
	 */
	peer_status_queue& operator=(peer_status_queue&& other);

	/**
	 * @return a file descriptor that is ready for reading when the queue is
	 *         non-empty, suitable for use with poll, select, etc.
	 */
	int fd() const;

	/**
	 * @return Any status messages that are available at the time of the call.
	 */
	std::deque<peer_status> want_pop() const;

	/**
	 * @return At least one message.  The call blocks if it must.
	 */
	std::deque<peer_status> need_pop() const;

private:

	void* handle() const;

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_PEER_STATUS_QUEUE_HH
