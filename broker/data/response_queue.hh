#ifndef BROKER_DATA_RESPONSE_QUEUE_HH
#define BROKER_DATA_RESPONSE_QUEUE_HH

#include <broker/data/response.hh>
#include <memory>
#include <deque>

namespace broker { namespace data {

class frontend;

class response_queue {
friend class frontend;

public:

	/**
	 * Construct response queue.
	 */
	response_queue();

	/**
	  * Destruct response queue.
	  */
	~response_queue();

	/**
	 * Copying a response queue disallowed.
	 */
	response_queue(const response_queue& other) = delete;

	/**
	 * Steal a response queue.
	 */
	response_queue(response_queue&& other);

	/**
	 * Copying a response queue disallowed.
	 */
	response_queue& operator=(const response_queue& other) = delete;

	/**
	 * Replace queue by stealing another.
	 */
	response_queue& operator=(response_queue&& other);

	/**
	 * @return a file descriptor that is ready for reading when the queue is
	 *         non-empty, suitable for use with poll, select, etc.
	 */
	int fd() const;

	/**
	 * @return Any print messages that are available at the time of the call.
	 */
	std::deque<response> want_pop() const;

	/**
	 * @return At least one print message.  The call blocks if it must.
	 */
	std::deque<response> need_pop() const;

private:

	void* handle() const;

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_RESPONSE_QUEUE_HH
