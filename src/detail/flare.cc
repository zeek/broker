#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "broker/detail/flare.hh"

namespace broker {
namespace detail {

flare::flare()
  : p{FD_CLOEXEC, FD_CLOEXEC, O_NONBLOCK, O_NONBLOCK} {
}

void flare::fire() {
  char tmp = 0;
  for (;;) {
    auto n = ::write(p.write_fd(), &tmp, 1);
    if (n > 0)
      break; // Success -- wrote a byte to pipe.
    if (n < 0 && errno == EAGAIN)
      break; // Success -- pipe is full and just need at least one byte in it.
    // Loop because either the byte wasn't written or got EINTR error.
  }
}

void flare::extinguish() {
  char tmp[256];
  for (;;)
    if (::read(p.read_fd(), tmp, sizeof(tmp)) == -1 && errno == EAGAIN)
      break; // Pipe is now drained.
}

bool flare::extinguish_one() {
  char tmp = 0;
  auto n = 0;
  for (;;) {
    auto n = ::read(p.read_fd(), &tmp, 1);
    if (n == 1)
      return true; // Read one byte.
    if (n < 0 && errno == EAGAIN)
      return false; // No data available to read.
  }
}

} // namespace detail
} // namespace broker
