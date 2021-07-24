#include "broker/detail/connector.hh"

#include <cstdio>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

// clang-format off
#ifdef CAF_WINDOWS
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif // CAF_WINDOWS
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif // NOMINMAX
#  ifdef CAF_MINGW
#    undef _WIN32_WINNT
#    undef WINVER
#    define _WIN32_WINNT WindowsVista
#    define WINVER WindowsVista
#    include <w32api.h>
#  endif // CAF_MINGW
#  include <windows.h>
#  include <winsock2.h>
#else
#  include <sys/types.h>
#  include <sys/socket.h>
#  include <poll.h>
#endif // CAF_WINDOWS
// clang-format on

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/config.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/expected.hpp>
#include <caf/net/tcp_accept_socket.hpp>
#include <caf/net/tcp_stream_socket.hpp>

#include "broker/alm/lamport_timestamp.hh"
#include "broker/detail/overload.hh"
#include "broker/filter_type.hh"
#include "broker/message.hh"

namespace {

#ifndef POLLRDHUP
#  define POLLRDHUP POLLHUP
#endif

#ifndef POLLPRI
#  define POLLPRI POLLIN
#endif

#ifdef CAF_WINDOWS
// From the MSDN: If the POLLPRI flag is set on a socket for the Microsoft
//                Winsock provider, the WSAPoll function will fail.
constexpr short read_mask = POLLIN;
#else
constexpr short read_mask = POLLIN | POLLPRI;
#endif

constexpr short write_mask = POLLOUT;

constexpr short error_mask = POLLRDHUP | POLLERR | POLLHUP | POLLNVAL;

constexpr short rw_mask = read_mask | write_mask;

} // namespace

namespace broker::detail {

namespace {

enum class connector_msg : uint8_t {
  indeterminate, // Needs more data.
  shutdown,      // System is shutting down.
  connect,       // Try connect to a new Broker endpoint.
  listen,        // Allow other Broker endpoint to connect.
  drop,          // Drop an address from the list of known endpoints.
};

template <class... Ts>
caf::byte_buffer to_buf(connector_msg tag, Ts&&... xs) {
  caf::byte_buffer buf;
  caf::binary_serializer snk{nullptr, buf};
  if (!snk.apply(static_cast<uint8_t>(tag))
      || !snk.apply(uint32_t{0}) // Placeholder for the serialized size.
      || !snk.apply(std::forward_as_tuple(xs...))) {
    throw std::runtime_error("failed to serialize arguments");
  }
  auto payload_len = static_cast<uint32_t>(buf.size() - 5);
  snk.seek(1);
  std::ignore = snk.apply(payload_len);
  return buf;
}

template <class... Ts>
std::tuple<Ts...> from_source(caf::binary_deserializer& src) {
  std::tuple<Ts...> result;
  if (!src.apply(result) || src.remaining() != 0)
    throw std::runtime_error{"error while parsing pipe input"};
  return result;
}

class broken_pipe : public std::exception {
public:
  broken_pipe(int flags) noexcept : flags_(flags) {
    // nop
  }

  const char* what() const noexcept {
    if (flags_ & (POLLRDHUP | POLLHUP))
      return "POLLRDHUP: cannot read from closed pipe";
    else if (flags_ & POLLERR)
      return "POLLERR: cannot write to closed pipe";
    else if (flags_ & POLLNVAL)
      return "POLLNVAL: invalid pipe handle";
    else
      return "failed to read from pipe for an unknown reason";
  }

private:int flags_;
};

class pipe_reader {
public:
  pipe_reader(caf::net::pipe_socket sock, bool* done) noexcept
    : sock_(sock), done_(done) {
    // nop
  }

  ~pipe_reader() {
    caf::net::close(sock_);
  }

  template <class Manager>
  void read(Manager& mgr) {
    BROKER_TRACE("");
    for (;;) {
      auto rdres = caf::net::read(sock_, caf::make_span(rd_buf_));
      if (rdres > 0) {
        buf_.insert(buf_.end(), rd_buf_, rd_buf_ + rdres);
        invoke_from_buf(mgr);
      } else if (rdres == 0 || !caf::net::last_socket_error_is_temporary()) {
        throw broken_pipe{POLLHUP};
      }
      // EAGAIN or some other temporary error.
      return;
    }
  }

private:
  template <class Manager>
  void invoke_from_buf(Manager& mgr) {
    BROKER_TRACE("");
    while (!buf_.empty()) {
      caf::binary_deserializer src{nullptr, buf_};
      uint8_t tag = 0;
      if (!src.apply(tag))
        throw std::runtime_error{"error while parsing pipe input"};
      auto msg_type = static_cast<connector_msg>(tag);
      if (msg_type == connector_msg::shutdown) {
        BROKER_DEBUG("received shutdown event, stop the connector");
        *done_ = true;
        return;
      }
      if (buf_.size() < 5)
        return; // Try again later.
      uint32_t len = 0;
      if (!src.apply(len) || len == 0)
        throw std::runtime_error{"error while parsing pipe input"};
      if (len < src.remaining())
        return; // Try again later.
      src.reset({buf_.data() + 5, len});
      switch (msg_type) {
        case connector_msg::connect: {
          auto&& [eid, addr]
            = from_source<connector_event_id, network_info>(src);
          mgr.connect(eid, addr);
          break;
        }
        case connector_msg::drop: {
          auto&& [eid, addr]
            = from_source<connector_event_id, network_info>(src);
          mgr.drop(eid, addr);
          break;
        }
        case connector_msg::listen: {
          auto&& [eid, host, port]
            = from_source<connector_event_id, std::string, uint16_t>(src);
          mgr.listen(eid, host, port);
          break;
        }
        default:
          throw std::runtime_error{"error while parsing pipe input"};
      }
      buf_.erase(buf_.begin(), buf_.begin() + 5 + static_cast<ptrdiff_t>(len));
    }
  }

  caf::net::pipe_socket sock_;
  caf::byte_buffer buf_;
  caf::byte rd_buf_[512];
  bool* done_;
};

enum class rw_state {
  done,          // Finished operation, i.e., received or sent all bytes.
  indeterminate, // Must try again later.
  failed,        // Must abort any operations on the socket.
};

struct originator_tag_t {};

constexpr auto originator_tag = originator_tag_t{};

struct responder_tag_t {};

constexpr auto responder_tag = responder_tag_t{};

// TODO: make configurable
constexpr size_t max_connection_attempts = 10;

// Size of the 'constant' part of a handshake message without the leading
// 4-Bytes to encode the payload size.
static constexpr size_t handshake_prefix_size = 17;

struct connect_state {
  uint32_t payload_size = 0;
  uint32_t retry_count = 0;
  caf::byte_buffer wr_buf_data;
  caf::const_byte_span wr_buf;
  caf::byte_buffer rd_buf;
  size_t read_pos = 0;
  endpoint_id remote_id;
  alm::lamport_timestamp remote_ts;
  filter_type remote_filter;
  network_info addr;
  connector_event_id event_id = invalid_connector_event_id;
  bool is_originator;
  size_t connection_attempts = 0;

  template <class Tag>
  connect_state(Tag, caf::const_byte_span hs_prefix, shared_filter_type* filter)
    : wr_buf_data(hs_prefix.begin(), hs_prefix.end()),
      is_originator(std::is_same_v<Tag, originator_tag_t>) {
    rd_buf.resize(4); // Must provide space for the payload size.
    update_wr_buf(filter);
  }

  template <class Tag>
  connect_state(Tag tag, caf::const_byte_span hs_prefix, connector_event_id eid,
                network_info addr, shared_filter_type* filter)
    : connect_state(tag, hs_prefix, filter) {
    event_id = eid;
    this->addr = std::move(addr);
  }

  void update_wr_buf(shared_filter_type* filter) {
    BROKER_ASSERT(wr_buf_data.size() >= handshake_prefix_size);
    wr_buf_data.resize(handshake_prefix_size + 4);
    [[maybe_unused]] auto ok = filter->read([this](auto ts, auto& xs) {
      caf::binary_serializer sink{nullptr, wr_buf_data};
      if (!sink.apply(ts) || !sink.apply(xs))
        return false;
      sink.seek(0);
      return sink.apply(static_cast<uint32_t>(wr_buf_data.size() - 4));
    });
    BROKER_ASSERT(ok);
    wr_buf = wr_buf_data;
  }

  rw_state continue_writing(caf::net::stream_socket fd) {
    auto res = caf::net::write(fd, wr_buf);
    if (res < 0) {
      return caf::net::last_socket_error_is_temporary()
               ? rw_state::indeterminate
               : rw_state::failed;
    } else if (res > 0) {
      wr_buf = wr_buf.subspan(static_cast<size_t>(res));
      if (wr_buf.empty()) {
        BROKER_DEBUG("finished sending handshake to peer");
        return rw_state::done;
      } else {
        return rw_state::indeterminate;
      }
    } else {
      // Socket closed (treat as error).
      return rw_state::failed;
    }
  }

  rw_state continue_writing(caf::net::socket_id fd) {
    return continue_writing(caf::net::stream_socket{fd});
  }

  bool parse_handshake() {
    BROKER_TRACE("");
    BROKER_ASSERT(rd_buf.size() == payload_size + 4);
    caf::binary_deserializer src{nullptr, rd_buf};
    src.skip(4); // No need to read the payload size again.
    auto msg_type = alm_message_type{0};
    auto expected_msg_type = is_originator ? alm_message_type::responder_hello
                                           : alm_message_type::originator_hello;
    auto ok = src.apply(msg_type)     //
              && src.apply(remote_id) //
              && src.apply(remote_ts) //
              && src.apply(remote_filter);
    if (ok)
      BROKER_DEBUG(BROKER_ARG(msg_type)
                   << BROKER_ARG(remote_id) << BROKER_ARG(remote_ts)
                   << BROKER_ARG(remote_filter));
    else
      BROKER_DEBUG("failed to deserialize handshake:" << src.get_error());
    return ok && src.remaining() == 0 && msg_type == expected_msg_type;
  }

  rw_state continue_reading(caf::net::stream_socket fd) {
    BROKER_TRACE(BROKER_ARG(fd.id));
    for (;;) {
      auto read_size = payload_size + 4;
      auto res = caf::net::read(fd, caf::make_span(rd_buf.data() + read_pos,
                                                   read_size - read_pos));
      BROKER_DEBUG(BROKER_ARG(res));
      if (res < 0) {
        return caf::net::last_socket_error_is_temporary()
                 ? rw_state::indeterminate
                 : rw_state::failed;
      } else if (res == 0) {
        // Socket closed (treat as error).
        return rw_state::failed;
      } else {
        auto ures = static_cast<size_t>(res);
        read_pos += ures;
        if (read_pos == 4) {
          BROKER_ASSERT(payload_size == 0);
          caf::binary_deserializer src{nullptr, rd_buf};
          [[maybe_unused]] auto ok = src.apply(payload_size);
          BROKER_ASSERT(ok);
          if (payload_size == 0)
            return rw_state::failed;
          BROKER_DEBUG("wait for payload of size" << payload_size);
          rd_buf.resize(payload_size + 4);
          return continue_reading(fd);
        } else if (read_pos == read_size) {
          return parse_handshake() ? rw_state::done : rw_state::failed;
        } else {
          return rw_state::indeterminate;
        }
      }
    }
  }

  rw_state continue_reading(caf::net::socket_id fd) {
    return continue_reading(caf::net::stream_socket{fd});
  }
};

using connect_state_ptr = std::shared_ptr<connect_state>;

template <class... Ts>
connect_state_ptr make_connect_state(Ts&&... xs) {
  return std::make_shared<connect_state>(std::forward<Ts>(xs)...);
}

struct connect_manager {
  /// Our pollset.
  std::vector<pollfd> fdset;

  /// Stores state objects that wait for their next retry.
  std::multimap<caf::timestamp, connect_state_ptr> retry_schedule;

  /// Stores state objects that are currently performing handshakes.
  std::unordered_map<caf::net::socket_id, connect_state_ptr> pending;

  /// Tags socket IDs that belong to an acceptor.
  std::unordered_set<caf::net::socket_id> acceptors;

  /// We cannot add elements from the pollset while iterating it, so we push new
  /// entries to this container instead and insert after each loop iteration.
  std::vector<pollfd> pending_fdset;

  /// Wraps the callbacks for handshake completion.
  connector::listener* listener_;

  /// Grants access to the peer filter.
  shared_filter_type* filter_;

  /// Caches the prefix of originator_hello handshakes.
  uint8_t originator_buf[handshake_prefix_size + 4];

  /// Caches the prefix of responder_hello handshakes.
  uint8_t responder_buf[handshake_prefix_size + 4];

  connect_manager(endpoint_id this_peer, connector::listener* ls,
                  shared_filter_type* filter)
    : listener_(ls), filter_(filter) {
    BROKER_TRACE(BROKER_ARG(this_peer));
    auto as_u8 = [](auto x) { return static_cast<uint8_t>(x); };
    // Prepare the originator handshake.
    memset(originator_buf, 0, handshake_prefix_size);
    originator_buf[4] = as_u8(alm_message_type::originator_hello);
    memcpy(originator_buf + 5, this_peer.bytes().data(), 16);
    // Prepare the responder handshake.
    memset(responder_buf, 0, handshake_prefix_size);
    responder_buf[4] = as_u8(alm_message_type::responder_hello);
    memcpy(responder_buf + 5, this_peer.bytes().data(), 16);
  }

  connect_manager(const connect_manager&) = delete;

  connect_manager& operator=(const connect_manager&) = delete;

  ~connect_manager() {
    for (auto& entry : fdset)
      caf::net::close(caf::net::socket{entry.fd});
    for (auto& entry : pending_fdset)
      caf::net::close(caf::net::socket{entry.fd});
  }

  auto originator_handshake() {
    return caf::as_bytes(caf::make_span(originator_buf));
  }

  auto responder_handshake() {
    return caf::as_bytes(caf::make_span(responder_buf));
  }

  /// Returns the relative timeout for the next retry in milliseconds or -1.
  int next_timeout_ms() {
    if (retry_schedule.empty())
      return -1;
    auto now = caf::make_timestamp();
    auto timeout = retry_schedule.begin()->first;
    if (timeout > now) {
      namespace sc = std::chrono;
      auto count = sc::duration_cast<sc::milliseconds>(timeout - now).count();
      return static_cast<int>(count);
    }
    return 0;
  }

  void connect(connect_state_ptr state) {
    BROKER_ASSERT(state->is_originator);
    BROKER_ASSERT(!state->addr.address.empty());
    state->update_wr_buf(filter_);
    caf::uri::authority_type authority;
    authority.host = state->addr.address;
    authority.port = state->addr.port;
    auto event_id = state->event_id;
    if (auto sock = caf::net::make_connected_tcp_stream_socket(authority)) {
      pending.emplace(sock->id, state);
      pending_fdset.push_back({sock->id, rw_mask, 0});
    } else if (++state->connection_attempts < max_connection_attempts) {
      auto retry_interval = state->addr.retry;
      if (retry_interval.count() != 0) {
        retry_schedule.emplace(caf::make_timestamp() + retry_interval,
                               std::move(state));
      } else if (valid(event_id)) {
        listener_->on_error(event_id, make_error(ec::peer_unavailable));
      }
    } else if (valid(event_id)) {
      listener_->on_error(event_id, make_error(ec::peer_unavailable));
    }
    // BROKER_ASSERT(state->is_originator);
    // BROKER_ASSERT(state->addr != std::nullopt);
    // caf::uri::authority_type authority;
    // authority.host = state->addr->address;
    // authority.port = state->addr->port;
    // auto event_id = state->event_id;
    // if (auto sock = caf::net::make_connected_tcp_stream_socket(authority)) {
    //   if (valid(event_id))
    //     listener_->on_connection(state->event_id, state->remote_id, sock->id);
    //   else
    //     listener_->on_connection(state->remote_id, sock->id);
    // } else if (++state->connection_attempts < max_connection_attempts) {
    //   auto retry_interval = state->addr->retry;
    //   if (retry_interval.count() != 0) {
    //     retry_schedule.emplace(caf::make_timestamp() + retry_interval,
    //                            std::move(state));
    //   } else if (valid(event_id)) {
    //     listener_->on_error(event_id, make_error(ec::peer_unavailable));
    //   }
    // } else if (valid(event_id)) {
    //   listener_->on_error(event_id, make_error(ec::peer_unavailable));
    // }
  }

  /// Registers a new state object for connecting to given address.
  void connect(connector_event_id event_id, const network_info& addr) {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(addr));
    connect(make_connect_state(originator_tag, originator_handshake(), event_id,
                               addr, filter_));
  }

  void listen(connector_event_id event_id, std::string& addr, uint16_t port) {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(addr) << BROKER_ARG(port));
    caf::uri::authority_type authority;
    if (addr.empty())
      authority.host = std::string{"0.0.0.0"};
    else
      authority.host = addr;
    authority.port = port;
    if (auto sock = caf::net::make_tcp_accept_socket(authority)) {
      if (auto actual_port = caf::net::local_port(*sock)) {
        BROKER_DEBUG("started listening on port" << *actual_port);
        acceptors.emplace(sock->id);
        pending_fdset.push_back({sock->id, read_mask, 0});
        listener_->on_listen(event_id, *actual_port);
      } else {
        BROKER_DEBUG("local_port failed:" << actual_port.error());
        caf::net::close(*sock);
        listener_->on_error(event_id, std::move(actual_port.error()));
      }
    } else {
      BROKER_DEBUG("make_tcp_accept_socket failed:" << sock.error());
      listener_->on_error(event_id, std::move(sock.error()));
    }
  }

  /// Registers a new state object for connecting to given address.
  void drop(connector_event_id event_id, network_info& addr) {
    // nop
  }

  void finalize(caf::net::socket_id fd, connect_state& state) {
    listener_->on_connection(state.event_id, state.remote_id, state.addr,
                             state.remote_ts, state.remote_filter, fd);
  }

  void continue_reading(pollfd& entry) {
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      switch (i->second->continue_reading(entry.fd)) {
        case rw_state::done:
          entry.events &= ~read_mask;
          if (entry.events == 0)
            finalize(entry.fd, *i->second);
          break;
        case rw_state::indeterminate:
          break;
        case rw_state::failed:
          abort(entry);
      }
    } else if (acceptors.count(entry.fd) != 0) {
      auto sock = caf::net::tcp_accept_socket{entry.fd};
      if (auto new_sock = caf::net::accept(sock)) {
        auto st = make_connect_state(responder_tag, responder_handshake(),
                                     filter_);
        if (auto addr = caf::net::remote_addr(*new_sock))
          st->addr.address = std::move(*addr);
        if (auto port = caf::net::remote_port(*new_sock))
          st->addr.port = *port;
        pending_fdset.push_back({new_sock->id, rw_mask, 0});
        pending.emplace(new_sock->id, st);
      }
    } else {
      entry.events &= ~read_mask;
    }
  }

  void continue_writing(pollfd& entry) {
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      switch (i->second->continue_writing(entry.fd)) {
        case rw_state::done:
          entry.events &= ~write_mask;
          if (entry.events == 0)
            finalize(entry.fd, *i->second);
          break;
        case rw_state::indeterminate:
          break;
        case rw_state::failed:
          abort(entry);
      }
    } else {
      entry.events &= ~write_mask;
    }
  }

  void abort(pollfd& entry) {
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      auto state = std::move(i->second);
      pending.erase(i);
      if (state->is_originator) {
        auto retry_interval = state->addr.retry;
        if (retry_interval.count() > 0
            && ++state->connection_attempts < max_connection_attempts) {
          retry_schedule.emplace(caf::make_timestamp() + retry_interval,
                                 std::move(state));
          BROKER_DEBUG("failed to connect on socket"
                       << entry.fd << "-> try again in" << retry_interval);
        } else {
          BROKER_DEBUG("failed to connect on socket" << entry.fd
                                                     << "-> give up");
          if (valid(state->event_id))
            listener_->on_error(state->event_id,
                                make_error(ec::peer_unavailable));
        }
      } else {
        BROKER_DEBUG("incoming peering failed on socket" << entry.fd);
      }
    } else if (auto j = acceptors.find(entry.fd); j != acceptors.end()) {
      BROKER_ERROR("acceptor failed: socket" << entry.fd);
      acceptors.erase(j);
    }
    close(caf::net::socket{entry.fd});
    entry.events = 0;
  }

  void handle_timeouts() {
    auto now = caf::make_timestamp();
    while (!retry_schedule.empty() && retry_schedule.begin()->first <= now) {
      // TODO: implement me
    }
  }

  void prepare_next_cycle() {
    BROKER_TRACE("pending handles:" << pending_fdset.size());
    auto is_done = [](auto& x) { return x.events == 0; };
    auto new_end = std::remove_if(fdset.begin(), fdset.end(), is_done);
    if (new_end != fdset.end()) {
      BROKER_DEBUG("drop" << std::distance(new_end, fdset.end())
                          << "completed sockets");
      fdset.erase(new_end, fdset.end());
    }
    if (!pending_fdset.empty()) {
      fdset.insert(fdset.end(), pending_fdset.begin(), pending_fdset.end());
      pending_fdset.clear();
    }
  }
};

} // namespace

connector::listener::~listener() {
  // nop
}

connector::connector(endpoint_id this_peer) : this_peer_(this_peer) {
  auto fds = caf::net::make_pipe();
  if (!fds) {
    auto err_str = to_string(fds.error());
    fprintf(stderr, "failed to create pipe: %s\n", err_str.c_str());
    abort();
  }
  std::tie(pipe_rd_, pipe_wr_) = *fds;
  if (auto err = caf::net::nonblocking(pipe_rd_, true)) {
    auto err_str = to_string(err);
    fprintf(stderr, "failed to set pipe to nonblocking: %s\n", err_str.c_str());
    abort();
  }
}

connector::~connector() {
  caf::net::close(pipe_wr_);
  caf::net::close(pipe_rd_);
}

void connector::async_connect(connector_event_id event_id,
                              const network_info& addr) {
  auto buf = to_buf(connector_msg::connect, event_id, addr);
  write_to_pipe(buf);
}

void connector::async_drop(const connector_event_id event_id,
                           const network_info& addr) {
  auto buf = to_buf(connector_msg::drop, event_id, addr);
  write_to_pipe(buf);
}

void connector::async_listen(connector_event_id event_id,
                             const std::string& address, uint16_t port) {
  auto buf = to_buf(connector_msg::listen, event_id, address, port);
  write_to_pipe(buf);
}

void connector::async_shutdown() {
  auto tag = connector_msg::shutdown;
  write_to_pipe(caf::as_bytes(caf::make_span(&tag, 1)));
}

void connector::write_to_pipe(caf::span<const caf::byte> bytes) {
  std::unique_lock guard{mtx_};
  if (pipe_wr_.id == caf::net::invalid_socket_id)
    throw std::runtime_error("failed to write to the pipe");
  auto res = caf::net::write(pipe_wr_, bytes);
  if (res != static_cast<ptrdiff_t>(bytes.size())) {
    caf::net::close(pipe_wr_);
    pipe_wr_.id = caf::net::invalid_socket_id;
    throw std::runtime_error("failed to write to the pipe");
  }
}

void connector::init(std::unique_ptr<listener> sub, shared_filter_ptr filter) {
  BROKER_ASSERT(sub != nullptr);
  BROKER_ASSERT(filter != nullptr);
  std::unique_lock guard{mtx_};
  if (sub_ != nullptr)
    throw std::logic_error("connector::init called twice");
  BROKER_ASSERT(filter_ == nullptr);
  sub_ = std::move(sub);
  filter_ = std::move(filter);
  sub_cv_.notify_all();
}

void connector::run() {
  BROKER_TRACE("");
  // Wait for subscriber and filter before starting the loop.
  listener* sub = nullptr;
  shared_filter_type* filter = nullptr;
  {
    std::unique_lock guard{mtx_};
    while (sub_ == nullptr)
      sub_cv_.wait(guard);
    sub = sub_.get();
    filter = filter_.get();
  }
  try {
    run_impl(sub, filter);
  } catch ([[maybe_unused]] std::exception& ex) {
    BROKER_ERROR("exception:" << ex.what());
  }
  sub->on_shutdown();
}

void connector::run_impl(listener* sub, shared_filter_type* filter) {
  using std::find_if;
  // Poll isn't terribly efficient nor fast, but the connector is not a
  // performance-critical system component. It only establishes connections and
  // reads handshake messages, so poll() is 'good enough' and we chose it since
  // it's portable.
  connect_manager mgr{this_peer_, sub, filter};
  auto& fdset = mgr.fdset;
  fdset.push_back({pipe_rd_.id, read_mask, 0});
  bool done = false;
  pipe_reader prd{pipe_rd_, &done};
  // Loop until we receive a shutdown via the pipe.
  while (!done) {
    int presult =
#ifdef CAF_WINDOWS
      ::WSAPoll(fdset.data(), static_cast<ULONG>(fdset.size()), -1);
#else
      ::poll(fdset.data(), static_cast<nfds_t>(fdset.size()), -1);
#endif
    BROKER_DEBUG("poll on" << fdset.size() << "sockets returned" << presult);
    if (presult < 0) {
      switch (caf::net::last_socket_error()) {
        case std::errc::interrupted:
          // nop
          break;
        default:
          BROKER_ERROR("poll() failed");
          throw std::runtime_error("poll() failed");
      }
    } else if (presult > 0) {
      auto has_activity = [](auto& entry) { return entry.revents != 0; };
      auto i = std::find_if(fdset.begin(), fdset.end(), has_activity);
      BROKER_ASSERT(i != fdset.end());
      auto advance = [&i, &fdset, has_activity] {
        i = std::find_if(i + 1, fdset.end(), has_activity);
        return i != fdset.end();
      };
      do {
        if (i->fd == pipe_rd_.id) {
          if (i->revents & read_mask) {
            prd.read(mgr);
          } else if (i->revents & error_mask) {
            throw broken_pipe{i->revents};
          }
        } else {
          if (i->revents & read_mask) {
            mgr.continue_reading(*i);
          } else if (i->revents & write_mask) {
            mgr.continue_writing(*i);
          } else {
            mgr.abort(*i);
          }
        }
      } while (--presult > 0 && advance());
    }
    mgr.handle_timeouts();
    mgr.prepare_next_cycle();
  }
  // TODO: close sockets
}

} // namespace broker::detail
