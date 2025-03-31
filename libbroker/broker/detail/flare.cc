#include "broker/detail/flare.hh"

#include <cerrno>

#include <algorithm>
#include <exception>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/net/pipe_socket.hpp>
#include <caf/net/socket.hpp>

#include "broker/config.hh"
#include "broker/detail/assert.hh"
#include "broker/logger.hh"

#ifdef BROKER_WINDOWS

#  include <Winsock2.h>

#  define PIPE_WRITE(fd, buf, num_bytes) ::send(fd, buf, num_bytes, 0)

#  define PIPE_READ(fd, buf, num_bytes) ::recv(fd, buf, num_bytes, 0)

namespace {

template <class... Ts>
auto poll(Ts... xs) {
  return WSAPoll(xs...);
}

bool try_again_later() {
  int code = WSAGetLastError();
  return code == WSAEWOULDBLOCK;
}

} // namespace

#else // BROKER_WINDOWS

#  include <cerrno>
#  include <fcntl.h>
#  include <poll.h>
#  include <unistd.h>

#  define PIPE_WRITE ::write

#  define PIPE_READ ::read

namespace {

bool try_again_later() {
  if constexpr (EAGAIN == EWOULDBLOCK)
    return errno == EAGAIN;
  else
    return errno == EAGAIN || errno == EWOULDBLOCK;
}

} // namespace

#endif // BROKER_WINDOWS

namespace broker::detail {

namespace {

constexpr size_t stack_buffer_size = 256;

struct stack_buffer {
  char data[stack_buffer_size];

  stack_buffer() {
    memset(data, 0, stack_buffer_size);
  }
};

} // namespace

flare::flare() {
  auto maybe_fds = caf::net::make_pipe();
  if (!maybe_fds) {
    log::core::critical("cannot-create-pipe", "failed to create pipe: {}",
                        maybe_fds.error());
    abort();
  }
  auto [first, second] = *maybe_fds;
  fds_[0] = first.id;
  fds_[1] = second.id;
  if (auto err = caf::net::child_process_inherit(first, false))
    log::core::error("cannot-set-cloexec",
                     "failed to set flare fd 0 CLOEXEC: {}", err);
  if (auto err = caf::net::child_process_inherit(second, false))
    log::core::error("cannot-set-cloexec",
                     "failed to set flare fd 1 CLOEXEC: {}", err);
  if (auto err = caf::net::nonblocking(first, true)) {
    log::core::critical("cannot-set-nonblock",
                        "failed to set flare fd 0 NONBLOCK: {}", err);
    std::terminate();
  }
  // Do not set the write handle to nonblock, because we want the producer to
  // slow down in case the consumer cannot keep up emptying the pipe.
  //::fcntl(fds_[1], F_SETFL, ::fcntl(fds_[1], F_GETFL) | O_NONBLOCK);
}

flare::~flare() {
  caf::net::close(caf::net::pipe_socket{fds_[0]});
  caf::net::close(caf::net::pipe_socket{fds_[1]});
}

native_socket flare::fd() const {
  return fds_[0];
}

void flare::fire(size_t num) {
  stack_buffer tmp;
  size_t remaining = num;
  while (remaining > 0) {
    int len = static_cast<int>(std::min(remaining, stack_buffer_size));
    auto n = PIPE_WRITE(fds_[1], tmp.data, len);
    if (n <= 0) {
      log::core::error("cannot-write-flare-pipe",
                       "failed to write to flare pipe: {}", n);
      std::terminate();
    }
    remaining -= static_cast<size_t>(n);
  }
}

size_t flare::extinguish() {
  stack_buffer tmp;
  size_t result = 0;
  for (;;) {
    auto n = PIPE_READ(fds_[0], tmp.data, stack_buffer_size);
    if (n > 0)
      result += static_cast<size_t>(n);
    else if (n == -1 && try_again_later())
      return result; // Pipe is now drained.
  }
}

bool flare::extinguish_one() {
  char tmp = 0;
  for (;;) {
    auto n = PIPE_READ(fds_[0], &tmp, 1);
    if (n == 1)
      return true; // Read one byte.
    if (n < 0 && try_again_later())
      return false; // No data available to read.
  }
}

void flare::await_one() {
  pollfd p = {fds_[0], POLLIN, 0};
  for (;;) {
    auto n = ::poll(&p, 1, -1);
    if (n < 0 && !try_again_later())
      std::terminate();
    if (n == 1) {
      BROKER_ASSERT(p.revents & POLLIN);
      return;
    }
  }
}

bool flare::await_one_impl(int ms_timeout) {
  pollfd p = {fds_[0], POLLIN, 0};
  auto n = ::poll(&p, 1, ms_timeout);
  if (n < 0 && !try_again_later())
    std::terminate();
  if (n == 1) {
    BROKER_ASSERT(p.revents & POLLIN);
    return true;
  }
  return false;
}

} // namespace broker::detail
