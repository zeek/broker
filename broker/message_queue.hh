#ifndef BROKER_MESSAGE_QUEUE_HH
#define BROKER_MESSAGE_QUEUE_HH

#include <broker/endpoint.hh>
#include <broker/topic.hh>
#include <broker/message.hh>
#include <string>
#include <memory>
#include <deque>

namespace broker {

/**
 * Stores message's awaiting retrieval/processing.
 */
class message_queue {
public:

	/**
	 * Create an uninitialized message queue.  It will never contain any messages.
	 */
	message_queue();

	/**
	  * Destruct message queue.
	  */
	~message_queue();

	/**
	 * Copying a message queue is disallowed.
	 */
	message_queue(const message_queue& other) = delete;

	/**
	 * Steal a message queue.
	 */
	message_queue(message_queue&& other);

	/**
	 * Copying a message queue is disallowed.
	 */
	message_queue& operator=(const message_queue& other) = delete;

	/**
	 * Replace message queue by stealing another.
	 */
	message_queue& operator=(message_queue&& other);

	/**
	 * Create a message queue that will receive message messages directly from an
	 * endpoint or via one if its peers.
	 * @param t only message messages that match this topic are received.
	 * @param e a local endpoint.
	 */
	message_queue(topic t, const endpoint& e);

	/**
	 * @return a file descriptor that is ready for reading when the queue is
	 *         non-empty, suitable for use with poll, select, etc.
	 */
	int fd() const;

	/**
	 * @return Any message messages that are available at the time of the call.
	 */
	std::deque<message> want_pop() const;

	/**
	 * @return At least one message message.  The call blocks if it must.
	 */
	std::deque<message> need_pop() const;

	/**
	 * @return the topic associated with the queue.
	 */
	const topic& get_topic() const;

private:

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_MESSAGE_QUEUE_HH
