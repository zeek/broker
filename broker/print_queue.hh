#ifndef BROKER_PRINT_QUEUE_HH
#define BROKER_PRINT_QUEUE_HH

#include <broker/endpoint.hh>
#include <string>
#include <memory>
#include <deque>

namespace broker {

class print_queue {
public:

	/**
	 * Create an uninitialized print queue.  It will never contain any messages.
	 */
	print_queue();

	/**
	 * Create a print queue that will receive print messages directly from an
	 * endpoint or via one if its peers.
	 * @param e a local endpoint.
	 * @param topic only print messages that match this string are received.
	 */
	print_queue(std::string topic, const endpoint& e);

	/**
	 * @return a file descriptor that is ready for reading when the queue is
	 *         non-empty, suitable for use with poll, select, etc.
	 */
	int fd() const;

	/**
	 * @return Any print messages that are available at the time of the call.
	 */
	std::deque<std::string> want_pop();

	/**
	 * @return At least one print message.  The call blocks if it must.
	 */
	std::deque<std::string> need_pop();

	/**
	 * @return the topic associated with the queue.
	 */
	const std::string& topic() const;

private:

	class impl;
	std::shared_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_PRINT_QUEUE_HH
