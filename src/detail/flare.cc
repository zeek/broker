#include "broker/detail/flare.hh"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <exception>
#include <algorithm>

#include "broker/logger.hh"

namespace broker {
namespace detail {

namespace {

constexpr size_t stack_buffer_size = 256;

} // namespace <anonymous>

flare::flare() {
  if (::pipe(fds_) == -1) {
    BROKER_ERROR("failed to create flare pipe");
    std::terminate();
  }

  int res;

  res = ::fcntl(fds_[0], F_SETFD, ::fcntl(fds_[0], F_GETFD) | FD_CLOEXEC);

  if (res == -1)
    BROKER_ERROR("failed to set flare fd 0 CLOEXEC");

  res = ::fcntl(fds_[1], F_SETFD, ::fcntl(fds_[1], F_GETFD) | FD_CLOEXEC);

  if (res == -1)
    BROKER_ERROR("failed to set flare fd 1 CLOEXEC");

  res = ::fcntl(fds_[0], F_SETFL, ::fcntl(fds_[0], F_GETFL) | O_NONBLOCK);

  if (res == -1) {
    BROKER_ERROR("failed to set flare fd 0 NONBLOCK");
    std::terminate();
  }

  // Do not set the write handle to nonblock, because we want the producer to
  // slow down in case the consumer cannot keep up emptying the pipe.
  //::fcntl(fds_[1], F_SETFL, ::fcntl(fds_[1], F_GETFL) | O_NONBLOCK);
}

int flare::fd() const {
  return fds_[0];
}

void flare::fire(size_t num) {
  char tmp[stack_buffer_size];
  size_t remaining = num;
  while (remaining > 0) {
    auto n = ::write(fds_[1], tmp,
                     static_cast<int>(std::min(remaining, stack_buffer_size)));
    if (n <= 0) {
      BROKER_ERROR("unable to write flare pipe!");
      std::terminate();
    }
    remaining -= static_cast<size_t>(n);
  }
}

size_t flare::extinguish() {
  char tmp[stack_buffer_size];
  size_t result = 0;
  for (;;) {
    auto n = ::read(fds_[0], tmp, stack_buffer_size);
    if (n > 0)
      result += static_cast<size_t>(n);
    else if (n == -1 && errno == EAGAIN)
      return result; // Pipe is now drained.
  }
}

bool flare::extinguish_one() {
  char tmp = 0;
  for (;;) {
    auto n = ::read(fds_[0], &tmp, 1);
    if (n == 1)
      return true; // Read one byte.
    if (n < 0 && errno == EAGAIN)
      return false; // No data available to read.
  }
}

void flare::await_one() {
  CAF_LOG_TRACE("");
  pollfd p = {fds_[0], POLLIN, 0};
  for (;;) {
    CAF_LOG_DEBUG("polling");
    auto n = ::poll(&p, 1, -1);
    if (n < 0 && errno != EAGAIN)
      std::terminate();
    if (n == 1) {
      CAF_ASSERT(p.revents & POLLIN);
      return;
    }
  }
}

bool flare::await_one_impl(int ms_timeout) {
  CAF_LOG_TRACE("");
  pollfd p = {fds_[0], POLLIN, 0};
  auto n = ::poll(&p, 1, ms_timeout);
  if (n < 0 && errno != EAGAIN)
    std::terminate();
  if (n == 1) {
    CAF_ASSERT(p.revents & POLLIN);
    return true;
  }
  return false;
}

} // namespace detail
} // namespace broker
