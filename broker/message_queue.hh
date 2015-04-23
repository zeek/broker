#ifndef BROKER_MESSAGE_QUEUE_HH
#define BROKER_MESSAGE_QUEUE_HH

#include <broker/endpoint.hh>
#include <broker/message.hh>
#include <broker/queue.hh>
#include <broker/topic.hh>
#include <memory>

namespace broker {

/**
 * Requests messages from a broker::endpoint or its peers that match a topic
 * prefix.
 */
class message_queue : public queue<broker::message> {
public:

	/**
	 * Create an uninitialized message queue.
	 */
	message_queue();

	/**
	 * Destruct message_queue.
	 */
	~message_queue();

	/**
	 * Copying a message_queue is not allowed.
	 */
	message_queue(const message_queue&) = delete;

	/**
	 * Construct a message_queue by stealing another.
	 */
	message_queue(message_queue&&);

	/**
	 * Copying a message_queue is not allowed.
	 */
	message_queue& operator=(const message_queue&) = delete;

	/**
	 * Replace message_queue by stealing another.
	 */
	message_queue& operator=(message_queue&&);

	/**
	 * Attach a message_queue to an endpoint.
	 * @param prefix the subscription topic to use.  All messages sent via
	 * endpoint \a e or one of its peers that use a topic prefixed by \a prefix
	 * will be copied in to this queue.
	 * @param e the endpoint to attach the message_queue.
	 */
	message_queue(topic prefix, const endpoint& e);

	/**
	 * @return the subscription topic prefix of the queue.
	 */
	const topic& get_topic_prefix() const;

	/**
	 * True if the message_queue is initialized for use, else false.
	 */
	explicit operator bool() const;

private:

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_MESSAGE_QUEUE_HH
