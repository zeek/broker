#ifndef BROKER_DETAIL_FLARE_HH
#define BROKER_DETAIL_FLARE_HH

#include "broker/detail/pipe.hh"

namespace broker {
namespace detail {

// An object that can be used to signal a "ready" status via a file descriptor
// that may be integrated with select(), poll(), etc. Though it may be used to
// signal availability of a resource across threads, both access to that
// resource and the use of the fire/extinguish functions must be performed in
// a thread-safe manner in order for that to work correctly.
class flare {
public:
  flare();

  // Retrieves a file descriptor that will become ready if the flare has been
  // fired and not yet extinguishedd.
  int fd() const {
    return p.read_fd();
  }

  // Put the object in the "ready" state by writing one byte into the
  // underlying pipe.
  void fire();

  // Take the object out of the "ready" state by consuming all bytes from the
  // underlying pipe.
  void extinguish();

  // Attempts to consume only one byte from the pipe, potentially leaving the
  // flare in "ready" state.
  // Returns `true` if one byte was read successfully from the pipe and false
  // if the pipe had no data to be read.
  bool extinguish_one();

private:
  pipe p;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_FLARE_HH
