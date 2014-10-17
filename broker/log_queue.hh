#ifndef BROKER_LOG_QUEUE_HH
#define BROKER_LOG_QUEUE_HH

#include <broker/endpoint.hh>
#include <broker/log_msg.hh>
#include <string>
#include <memory>
#include <deque>

namespace broker {

class log_queue {
public:

	/**
	 * Create an uninitialized log queue.  It will never contain any messages.
	 */
	log_queue();

	/**
	  * Destruct log queue.
	  */
	~log_queue();

	/**
	 * Copying a log queue is disallowed.
	 */
	log_queue(const log_queue& other) = delete;

	/**
	 * Steal a log queue.
	 */
	log_queue(log_queue&& other);

	/**
	 * Copying a log queue is disallowed.
	 */
	log_queue& operator=(const log_queue& other) = delete;

	/**
	 * Replace log queue by stealing another.
	 */
	log_queue& operator=(log_queue&& other);

	/**
	 * Create a log queue that will receive log messages directly from an
	 * endpoint or via one if its peers.
	 * @param topic_name only log messages that match this string are received.
	 * @param e a local endpoint.
	 */
	log_queue(std::string topic_name, const endpoint& e);

	/**
	 * @return a file descriptor that is ready for reading when the queue is
	 *         non-empty, suitable for use with poll, select, etc.
	 */
	int fd() const;

	/**
	 * @return Any log messages that are available at the time of the call.
	 */
	std::deque<log_msg> want_pop() const;

	/**
	 * @return At least one log message.  The call blocks if it must.
	 */
	std::deque<log_msg> need_pop() const;

	/**
	 * @return the topic name associated with the queue.
	 */
	const std::string& topic_name() const;

private:

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_LOG_QUEUE_HH
