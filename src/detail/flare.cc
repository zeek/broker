#include "broker/detail/flare.hh"

#include <errno.h>

#include <algorithm>
#include <exception>

#include "broker/config.hh"
#include "broker/detail/assert.hh"
#include "broker/logger.hh"

#ifdef BROKER_WINDOWS

#include <Winsock2.h>

#define PIPE_WRITE(fd, buf, num_bytes) ::send(fd, buf, num_bytes, 0)

#define PIPE_READ(fd, buf, num_bytes) ::recv(fd, buf, num_bytes, 0)

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

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#define PIPE_WRITE ::write

#define PIPE_READ ::read

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

} // namespace

flare::flare() {
  using namespace caf::io::network;
  auto [first, second] = create_pipe();
  fds_[0] = first;
  fds_[1] = second;
  if (auto res = child_process_inherit(first, false); !res)
    BROKER_ERROR("failed to set flare fd 0 CLOEXEC: " << res.error());
  if (auto res = child_process_inherit(second, false); !res)
    BROKER_ERROR("failed to set flare fd 1 CLOEXEC: " << res.error());
  if (auto res = nonblocking(first, false); !res) {
    BROKER_ERROR("failed to set flare fd 0 NONBLOCK: " << res.error());
    std::terminate();
  }
  // Do not set the write handle to nonblock, because we want the producer to
  // slow down in case the consumer cannot keep up emptying the pipe.
  //::fcntl(fds_[1], F_SETFL, ::fcntl(fds_[1], F_GETFL) | O_NONBLOCK);
}

flare::~flare() {
  using caf::io::network::close_socket;
  close_socket(fds_[0]);
  close_socket(fds_[1]);
}

flare::native_socket flare::fd() const {
  return fds_[0];
}

void flare::fire(size_t num) {
  char tmp[stack_buffer_size];
  size_t remaining = num;
  while (remaining > 0) {
    int len = static_cast<int>(std::min(remaining, stack_buffer_size));
    auto n = PIPE_WRITE(fds_[1], tmp, len);
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
    auto n = PIPE_READ(fds_[0], tmp, stack_buffer_size);
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
  BROKER_TRACE("");
  pollfd p = {fds_[0], POLLIN, 0};
  for (;;) {
    BROKER_DEBUG("polling");
    auto n = ::poll(&p, 1, -1);
    if (n < 0 && !try_again_later())
      std::terminate();
    if (n == 1) {
      CAF_ASSERT(p.revents & POLLIN);
      return;
    }
  }
}

bool flare::await_one_impl(int ms_timeout) {
  BROKER_TRACE("");
  pollfd p = {fds_[0], POLLIN, 0};
  auto n = ::poll(&p, 1, ms_timeout);
  if (n < 0 && !try_again_later())
    std::terminate();
  if (n == 1) {
    CAF_ASSERT(p.revents & POLLIN);
    return true;
  }
  return false;
}

} // namespace broker::detail
