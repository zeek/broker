#ifndef BROKER_UTIL_FLARE_HH
#define BROKER_UTIL_FLARE_HH

#include "pipe.hh"

namespace broker {
namespace util {

class flare {
public:

	/**
	 * Create a flare object that can be used to signal a "ready" status via
	 * a file descriptor that may be integrated with select(), poll(), etc.
	 * Though it may be used to signal availability of a resource across
	 * threads, both access to that resource and the use of the fire/extinguish
	 * functions must be performed in a thread-safe manner in order for that
	 * to work correctly.
	 */
	flare();

	/**
	 * @return a file descriptor that will become ready if the flare has been
	 *         fire()'d and not yet extinguished()'d.
	 */
	int fd() const
		{ return p.read_fd(); }

	/**
	 * Put the object in the "ready" state.
	 */
	void fire();

	/**
	 * Take the object out of the "ready" state.
	 */
	void extinguish();

private:

	pipe p;
};

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_FLARE_HH
