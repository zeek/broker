#ifndef BROKER_EVENT_QUEUE_HH
#define BROKER_EVENT_QUEUE_HH

#include <broker/endpoint.hh>
#include <broker/event_msg.hh>
#include <string>
#include <memory>
#include <deque>

namespace broker {

class event_queue {
public:

	/**
	 * Create an uninitialized event queue.  It will never contain any messages.
	 */
	event_queue();

	/**
	  * Destruct event queue.
	  */
	~event_queue();

	/**
	 * Copying a event queue is disallowed.
	 */
	event_queue(const event_queue& other) = delete;

	/**
	 * Steal a event queue.
	 */
	event_queue(event_queue&& other);

	/**
	 * Copying a event queue is disallowed.
	 */
	event_queue& operator=(const event_queue& other) = delete;

	/**
	 * Replace event queue by stealing another.
	 */
	event_queue& operator=(event_queue&& other);

	/**
	 * Create a event queue that will receive event messages directly from an
	 * endpoint or via one if its peers.
	 * @param topic_name only event messages that match this string are
	 *                   received.
	 * @param e a local endpoint.
	 */
	event_queue(std::string topic_name, const endpoint& e);

	/**
	 * @return a file descriptor that is ready for reading when the queue is
	 *         non-empty, suitable for use with poll, select, etc.
	 */
	int fd() const;

	/**
	 * @return Any event messages that are available at the time of the call.
	 */
	std::deque<event_msg> want_pop() const;

	/**
	 * @return At least one event message.  The call blocks if it must.
	 */
	std::deque<event_msg> need_pop() const;

	/**
	 * @return the topic name associated with the queue.
	 */
	const std::string& topic_name() const;

private:

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_EVENT_QUEUE_HH
