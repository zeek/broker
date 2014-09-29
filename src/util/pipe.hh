#ifndef BROKER_UTIL_PIPE_HH
#define BROKER_UTIL_PIPE_HH

namespace broker {
namespace util {

class pipe {
public:

	/**
	 * Create a pair of file descriptors via pipe(), or aborts if it cannot.
	 * @param flags0 file descriptor flags to set on read end of pipe.
	 * @param flags1 file descriptor flags to set on write end of pipe.
	 * @param status_flags0 descriptor status flags to set on read end of pipe.
	 * @param status_flags1 descriptor status flags to set on write end of pipe.
	 */
	pipe(int flags0 = 0, int flags1 = 0, int status_flags0 = 0,
	     int status_flags1 = 0);

	/**
	  * Close the pair of file descriptors owned by the object.
	  */
	~pipe();

	/**
	 * Make a copy of another pipe object (file descriptors are dup'd).
	 */
	pipe(const pipe& other);

	/**
	 * Steals the file descriptors of another pipe object.
	 */
	pipe(pipe&& other);

	/**
	 * Assign a pipe object by closing file descriptors and duping those of
	 * the other.
	 */
	pipe& operator=(const pipe& other);

	/**
	 * Assign a pipe object by closing file descriptors and stealing those of
	 * the other.
	 */
	pipe& operator=(pipe&& other);

	/**
	 * @return the file descriptor associated with the read-end of the pipe.
	 */
	int read_fd() const
		{ return fds[0]; }

	/**
	 * @return the file descriptor associated with the write-end of the pipe.
	 */
	int write_fd() const
		{ return fds[1]; }

private:

	void close();
	void copy(const pipe& other);
	void steal(pipe&& other);

	int fds[2];
	int flags[2];
};

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_PIPE_HH
