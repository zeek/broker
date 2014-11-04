#ifndef BROKER_QUEUE_HH
#define BROKER_QUEUE_HH

#include <memory>
#include <deque>

namespace broker {

/**
 * Stores items of type T that are awaiting retrieval/processing.
 */
template <class T>
class queue {
public:

	/**
	 * Create an uninitialized queue.
	 */
	queue();

	/**
	 * Destruct queue.
	 */
	~queue();

	/**
	 * Copying a queue is not allowed.
	 */
	queue(const queue<T>&) = delete;

	/**
	 * Construct a queue by stealing another.
	 */
	queue(queue<T>&&);

	/**
	 * Copying a queue is not allowed.
	 */
	queue<T>& operator=(const queue<T>&) = delete;

	/**
	 * Replace queue by stealing another.
	 */
	queue<T>& operator=(queue<T>&&);

	/**
	 * @return a file descriptor that is ready for reading when the queue is
	 *         non-empty, suitable for use with poll, select, etc.
	 */
	int fd() const;

	/**
	 * @return The contents of the queue at time of call, which may be empty.
	 */
	std::deque<T> want_pop() const;

	/**
	 * @return The contents of the queue, which must contain at least one item.
	 * If it must, the call blocks until at least one item is available.
	 */
	std::deque<T> need_pop() const;

	void* handle() const;

private:

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace broker

#endif // BROKER_QUEUE_HH
