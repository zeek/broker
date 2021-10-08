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
  auto ok = snk.apply(static_cast<uint8_t>(tag))
            && snk.apply(uint32_t{0}) // Placeholder for the serialized size.
            && (snk.apply(xs) && ...);
  if (!ok) {
    BROKER_ERROR("failed to serialize arguments");
    throw std::runtime_error("failed to serialize arguments");
  }
  if constexpr (sizeof...(Ts) > 0) {
    auto payload_len = static_cast<uint32_t>(buf.size() - 5);
    snk.seek(1);
    std::ignore = snk.apply(payload_len);
  }
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

private:
  int flags_;
};

class pipe_reader {
public:
  pipe_reader(caf::net::pipe_socket sock, bool* done) noexcept
    : sock_(sock), done_(done) {
    // nop
  }

  ~pipe_reader() {
    // nop
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
    BROKER_TRACE(BROKER_ARG2("buf.size", buf_.size()));
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
      if (src.remaining() < len) {
        BROKER_DEBUG("wait for payload of size"
                     << len << "with" << src.remaining() << "already received");
        return; // Try again later.
      }
      src.reset({buf_.data() + 5, len});
      switch (msg_type) {
        case connector_msg::connect: {
          BROKER_DEBUG("received connect event");
          auto&& [eid, addr]
            = from_source<connector_event_id, network_info>(src);
          mgr.connect(eid, addr);
          break;
        }
        case connector_msg::drop: {
          BROKER_DEBUG("received drop event");
          auto&& [eid, addr]
            = from_source<connector_event_id, network_info>(src);
          mgr.drop(eid, addr);
          break;
        }
        case connector_msg::listen: {
          BROKER_DEBUG("received listen event");
          auto&& [eid, host, port]
            = from_source<connector_event_id, std::string, uint16_t>(src);
          mgr.listen(eid, host, port);
          break;
        }
        default:
          BROKER_ERROR("received invalid event");
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

// TODO: make configurable
constexpr size_t max_connection_attempts = 10;

// Size of the 'constant' part of a handshake message without the leading
// 4-Bytes to encode the payload size.
static constexpr size_t handshake_prefix_size = 17;

struct connect_manager;

class connect_state : public std::enable_shared_from_this<connect_state> {
public:
  connect_manager* mgr;

  uint32_t payload_size = 0;
  uint32_t retry_count = 0;
  caf::byte_buffer wr_buf;
  caf::byte_buffer rd_buf;
  size_t read_pos = 0;
  endpoint_id remote_id;
  alm::lamport_timestamp remote_ts;
  filter_type remote_filter;
  network_info addr;
  connector_event_id event_id = invalid_connector_event_id;
  size_t connection_attempts = 0;
  // Stores whether we detected a redundant connection. Either locally or by
  // receiving a drop_con messages from the remote.
  bool redundant = false;
  // Stores whether we called `psm.insert(remote_id, ...)`. If true, we must
  // erase that state again when aborting.
  bool added_peer_status = false;
  // Stores pointers to connect states that tried to start a handshake process
  // while this state had already started it. We store these pointers to delay
  // the drop_conn message. Otherwise, the drop_conn message might arrive before
  // handshake messages and the remote side may get confused.
  std::vector<std::shared_ptr<connect_state>> redundant_connections;

  using fn_t = bool (connect_state::*)(alm_message_type);

  fn_t fn = nullptr;

  peer_status_map& peer_statuses();

  void transition(fn_t f) {
    fn = f;
    if (f == &connect_state::fin) {
      if (!redundant_connections.empty()) {
        for (auto& conn : redundant_connections) {
          conn->send_drop_conn();
          conn->transition(&connect_state::fin);
        }
        redundant_connections.clear();
      }
    } else if (f == &connect_state::err) {
      if (added_peer_status) {
        auto& psm = peer_statuses();
        BROKER_DEBUG(remote_id << "::" << psm.get(remote_id) << "-> ()");
        psm.remove(remote_id);
        added_peer_status = false;
      }
    }
  }

  connect_state(connect_manager* mgr) : mgr(mgr), fn(&connect_state::err) {
    wr_buf.reserve(128);
    rd_buf.reserve(128);
    reset();
    BROKER_DEBUG("created new connect_state object");
  }

  connect_state(connect_manager* mgr, connector_event_id eid, network_info addr)
    : mgr(mgr), fn(&connect_state::err) {
    wr_buf.reserve(128);
    rd_buf.reserve(128);
    reset();
    event_id = eid;
    BROKER_DEBUG("created new connect_state object" << BROKER_ARG(event_id)
                                                    << BROKER_ARG(addr));
    this->addr = std::move(addr);
  }

  /// Resets the state after a connection attempt has failed before trying to
  /// reconnect.
  void reset() {
    BROKER_DEBUG("resetting connect_state object" << BROKER_ARG(event_id)
                                                  << BROKER_ARG(addr));
    redundant = false;
    if (added_peer_status) {
      auto& psm = peer_statuses();
      BROKER_DEBUG(remote_id << "::" << psm.get(remote_id) << "-> ()");
      psm.remove(remote_id);
      added_peer_status = false;
    }
    wr_buf.clear();
    // The read buffer must provide space for the payload size. This is the
    // first thing we're going to read from the socket.
    rd_buf.resize(4);
    read_pos = 0;
    payload_size = 0;
  }

  ~connect_state() {
    BROKER_DEBUG("destroy connect_state object" << BROKER_ARG(event_id)
                                                << BROKER_ARG(addr));
  }

  shared_filter_type& local_filter();

  bool reached_fin_state() const noexcept {
    return fn == &connect_state::fin;
  }

  bool done() const noexcept {
    return reached_fin_state() && wr_buf.empty();
  }

  rw_state continue_writing(caf::net::stream_socket fd) {
    auto res = caf::net::write(fd, wr_buf);
    if (res < 0) {
      return caf::net::last_socket_error_is_temporary()
               ? rw_state::indeterminate
               : rw_state::failed;
    } else if (res > 0) {
      wr_buf.erase(wr_buf.begin(), wr_buf.begin() + res);
      if (wr_buf.empty()) {
        BROKER_DEBUG("finished sending message to peer");
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
    auto ok = src.apply(msg_type);
    if (ok) {
      switch (msg_type) {
        default:
          src.emplace_error(caf::sec::invalid_argument, "invalid message type");
          ok = false;
          break;
        case alm_message_type::hello:
        case alm_message_type::originator_ack:
        case alm_message_type::drop_conn:
          ok = src.apply(remote_id);
          if (ok)
            BROKER_DEBUG(BROKER_ARG(msg_type) << BROKER_ARG(remote_id));
          break;
        case alm_message_type::originator_syn:
        case alm_message_type::responder_syn_ack:
          ok = src.apply(remote_id)    //
               && src.apply(remote_ts) //
               && src.apply(remote_filter);
          if (ok)
            BROKER_DEBUG(BROKER_ARG(msg_type)
                         << BROKER_ARG(remote_id) << BROKER_ARG(remote_ts)
                         << BROKER_ARG(remote_filter));
          break;
      }
    }
    if (!ok)
      BROKER_ERROR("failed to deserialize handshake:" << src.get_error());
    return ok && src.remaining() == 0 && (*this.*fn)(msg_type);
  }

  rw_state continue_reading(caf::net::stream_socket fd) {
    BROKER_TRACE(BROKER_ARG(fd.id));
    for (;;) {
      auto read_size = payload_size + 4;
      BROKER_DEBUG("try reading more bytes"
                   << BROKER_ARG2("fd", fd.id)
                   << BROKER_ARG2("rd_buf.size", rd_buf.size())
                   << BROKER_ARG(read_pos) << BROKER_ARG(read_size));
      auto res = caf::net::read(fd, caf::make_span(rd_buf.data() + read_pos,
                                                   read_size - read_pos));
      BROKER_DEBUG(BROKER_ARG(res));
      if (res < 0) {
        if (caf::net::last_socket_error_is_temporary()) {
          return rw_state::indeterminate;
        } else {
          BROKER_DEBUG("reading from fd ="
                       << fd.id
                       << "failed:" << caf::net::last_socket_error_as_string());
          transition(&connect_state::err);
          return rw_state::failed;
        }
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
          if (!parse_handshake())
            return rw_state::failed;
          // Read next message.
          payload_size = 0;
          read_pos = 0;
          read_size = 4;
          return reached_fin_state() ? rw_state::done : rw_state::indeterminate;
        } else {
          return rw_state::indeterminate;
        }
      }
    }
  }

  rw_state continue_reading(caf::net::socket_id fd) {
    return continue_reading(caf::net::stream_socket{fd});
  }

  // -- FSM --------------------------------------------------------------------

  void send_msg(caf::const_byte_span bytes, bool add_filter);

  void send_hello();

  void send_orig_syn();

  void send_resp_syn_ack();

  void send_orig_ack();

  void send_drop_conn();

  bool handle_drop_conn();

  bool await_hello_or_orig_syn(alm_message_type msg);

  bool await_hello(alm_message_type msg);

  bool await_orig_syn(alm_message_type msg);

  bool await_resp_syn_ack(alm_message_type msg);

  bool await_orig_ack(alm_message_type msg);

  bool performing_handshake() const noexcept {
    fn_t handshake_states[] = {&connect_state::await_orig_syn,
                               &connect_state::await_resp_syn_ack,
                               &connect_state::await_orig_ack};
    return std::any_of(std::begin(handshake_states), std::end(handshake_states),
                       [this](auto ptr) { return ptr == fn; });
  }

  // Connections enter this state when detecting a redundant connection. From
  // here, they transition to FIN after sending drop_conn eventually.
  bool paused(alm_message_type) {
    BROKER_ERROR("tried processing a message after reaching state FIN");
    return false;
  }

  bool fin(alm_message_type) {
    BROKER_ERROR("tried processing a message after reaching state FIN");
    return false;
  }

  bool err(alm_message_type) {
    BROKER_ERROR("tried processing a message after reaching state ERR");
    return false;
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
  connector::listener* listener;

  /// Grants access to the peer filter.
  shared_filter_type* filter;

  /// Grants access to the thread-safe bookkeeping of peer statuses.
  peer_status_map* peer_statuses_;

  /// Stores the ID of the local peer.
  endpoint_id this_peer;

  /// Caches the hello message.
  uint8_t hello_buf[handshake_prefix_size + 4];

  /// Caches the prefix of originator_hello handshakes.
  uint8_t orig_syn_buf[handshake_prefix_size + 4];

  /// Caches the prefix of originator_hello handshakes.
  uint8_t orig_ack_buf[handshake_prefix_size + 4];

  /// Caches the prefix of responder_hello handshakes.
  uint8_t resp_syn_ack_buf[handshake_prefix_size + 4];

  /// Caches the drop-redundant-connection message.
  uint8_t drop_conn_buf[handshake_prefix_size + 4];

  connect_manager(endpoint_id this_peer, connector::listener* ls,
                  shared_filter_type* filter, peer_status_map* peer_statuses)
    : listener(ls),
      filter(filter),
      peer_statuses_(peer_statuses),
      this_peer(this_peer) {
    BROKER_TRACE(BROKER_ARG(this_peer));
    auto init_buf = [this](uint8_t* buf, alm_message_type type) {
      auto as_u8 = [](auto x) { return static_cast<uint8_t>(x); };
      memset(buf, 0, handshake_prefix_size);
      buf[3] = static_cast<uint8_t>(handshake_prefix_size);
      buf[4] = as_u8(type);
      memcpy(buf + 5, this->this_peer.bytes().data(), 16);
    };
    init_buf(hello_buf, alm_message_type::hello);
    init_buf(orig_syn_buf, alm_message_type::originator_syn);
    init_buf(orig_ack_buf, alm_message_type::originator_ack);
    init_buf(resp_syn_ack_buf, alm_message_type::responder_syn_ack);
    init_buf(drop_conn_buf, alm_message_type::drop_conn);
  }

  connect_manager(const connect_manager&) = delete;

  connect_manager& operator=(const connect_manager&) = delete;

  ~connect_manager() {
    for (auto& entry : fdset) {
      BROKER_DEBUG("close socket" << entry.fd);
      caf::net::close(caf::net::socket{entry.fd});
    }
    for (auto& entry : pending_fdset) {
      BROKER_DEBUG("close socket" << entry.fd);
      caf::net::close(caf::net::socket{entry.fd});
    }
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

  pollfd* find_pollfd(caf::net::socket_id fd) {
    for (auto& ref : fdset)
      if (ref.fd == fd)
        return std::addressof(ref);
    for (auto& ref : pending_fdset)
      if (ref.fd == fd)
        return std::addressof(ref);
    return nullptr;
  }

  void register_fd(connect_state* ptr, short event) {
    auto pred = [ptr](const auto& kvp) { return kvp.second.get() == ptr; };
    auto e = pending.end();
    if (auto i = std::find_if(pending.begin(), e, pred); i != e) {
      BROKER_DEBUG("register for"
                   << (event == read_mask ? "reading" : "writing")
                   << BROKER_ARG2("fd", i->first));
      if (auto fds_ptr = find_pollfd(i->first)) {
        fds_ptr->events |= event;
      } else {
        pending_fdset.emplace_back(pollfd{i->first, event, 0});
      }
    } else {
      BROKER_ERROR("called register_writing for an unknown connect state");
    }
  }

  connect_state* find_pending_handshake(endpoint_id peer) {
    for (auto& kvp : pending) {
      auto st = kvp.second.get();
      if (st->performing_handshake() && st->remote_id == peer)
        return st;
    }
    return nullptr;
  }

  void register_writing(connect_state* ptr) {
    register_fd(ptr, write_mask);
  }

  void register_reading(connect_state* ptr) {
    register_fd(ptr, read_mask);
  }

  void connect(connect_state_ptr state) {
    BROKER_TRACE("");
    BROKER_ASSERT(!state->addr.address.empty());
    caf::uri::authority_type authority;
    authority.host = state->addr.address;
    authority.port = state->addr.port;
    BROKER_DEBUG("try connecting to" << authority);
    auto event_id = state->event_id;
    if (auto sock = caf::net::make_connected_tcp_stream_socket(authority)) {
      BROKER_DEBUG("established connection to" << authority
                                               << "(initiate handshake)"
                                               << BROKER_ARG2("fd", sock->id));
      if (auto err = caf::net::nonblocking(*sock, true)) {
        auto err_str = to_string(err);
        fprintf(stderr, "failed to set pipe to nonblocking: %s\n",
                err_str.c_str());
        ::abort();
      }
      if (auto err = caf::net::allow_sigpipe(*sock, false)) {
        auto err_str = to_string(err);
        fprintf(stderr, "failed to disable sigpipe: %s\n",
                err_str.c_str());
        ::abort();
      }
      if (auto i = pending.find(sock->id); i != pending.end()) {
        BROKER_WARNING("socket" << sock->id
                                << "already associated to state object -> "
                                   "assume stale state and drop it!");
        pending.erase(i);
      }
      state->reset();
      pending.emplace(sock->id, state);
      pending_fdset.push_back({sock->id, read_mask, 0});
      state->transition(&connect_state::await_hello_or_orig_syn);
      state->send_hello();
    } else if (++state->connection_attempts < max_connection_attempts) {
      auto retry_interval = state->addr.retry;
      if (retry_interval.count() != 0) {
        BROKER_DEBUG("failed to connect to" << authority << "-> retry in"
                                            << retry_interval);
        listener->on_peer_unavailable(state->addr);
        retry_schedule.emplace(caf::make_timestamp() + retry_interval,
                               std::move(state));
      } else if (valid(event_id)) {
        BROKER_DEBUG("failed to connect to" << authority
                                            << "-> fail (retry disabled)");
        listener->on_error(event_id, make_error(ec::peer_unavailable));
      } else {
        listener->on_peer_unavailable(state->addr);
      }
    } else {
      BROKER_DEBUG("failed to connect to"
                   << authority << "-> fail (reached max connection attempts)");
      if (valid(event_id))
        listener->on_error(event_id, make_error(ec::peer_unavailable));
      else
        listener->on_peer_unavailable(state->addr);
    }
  }

  /// Registers a new state object for connecting to given address.
  void connect(connector_event_id event_id, const network_info& addr) {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(addr));
    connect(make_connect_state(this, event_id, addr));
  }

  void listen(connector_event_id event_id, std::string& addr, uint16_t port) {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(addr) << BROKER_ARG(port));
    caf::uri::authority_type authority;
    if (addr.empty())
      authority.host = std::string{"0.0.0.0"};
    else
      authority.host = addr;
    authority.port = port;
    if (auto sock = caf::net::make_tcp_accept_socket(authority, true)) {
      if (auto actual_port = caf::net::local_port(*sock)) {
        BROKER_DEBUG("started listening on port" << *actual_port << "socket"
                                                 << sock->id);
        acceptors.emplace(sock->id);
        pending_fdset.push_back({sock->id, read_mask, 0});
        listener->on_listen(event_id, *actual_port);
      } else {
        BROKER_ERROR("local_port failed:" << actual_port.error());
        caf::net::close(*sock);
        listener->on_error(event_id, std::move(actual_port.error()));
      }
    } else {
      BROKER_DEBUG("make_tcp_accept_socket failed:" << sock.error());
      listener->on_error(event_id, std::move(sock.error()));
    }
  }

  /// Registers a new state object for connecting to given address.
  void drop(connector_event_id event_id, network_info& addr) {
    // nop
  }

  void finalize(pollfd& entry, connect_state& state) {
    BROKER_TRACE(BROKER_ARG2("fd", entry.fd));
    BROKER_ASSERT(entry.events == 0);
    if (!state.redundant)
      listener->on_connection(state.event_id, state.remote_id, state.addr,
                              state.remote_ts, state.remote_filter, entry.fd);
    else
      listener->on_redundant_connection(state.event_id, state.remote_id,
                                        state.addr);
  }

  void continue_reading(pollfd& entry) {
    BROKER_TRACE(BROKER_ARG2("fd", entry.fd));
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      switch (i->second->continue_reading(entry.fd)) {
        case rw_state::done:
          entry.events &= ~read_mask;
          if (i->second->done()) {
            finalize(entry, *i->second);
            pending.erase(i);
          }
          break;
        case rw_state::indeterminate:
          break;
        case rw_state::failed:
          abort(entry);
      }
    } else if (acceptors.count(entry.fd) != 0) {
      using namespace std::literals;
      auto sock = caf::net::tcp_accept_socket{entry.fd};
      if (auto new_sock = caf::net::accept(sock)) {
        BROKER_ASSERT(pending.count(new_sock->id) == 0);
        if (auto err = caf::net::nonblocking(*new_sock, true)) {
          auto err_str = to_string(err);
          fprintf(stderr, "failed to set pipe to nonblocking: %s\n",
                  err_str.c_str());
          ::abort();
        }
        if (auto err = caf::net::allow_sigpipe(*new_sock, false)) {
          auto err_str = to_string(err);
          fprintf(stderr, "failed to disable sigpipe: %s\n", err_str.c_str());
          ::abort();
        }
        auto st = make_connect_state(this);
        st->addr.retry = 0s;
        if (auto addr = caf::net::remote_addr(*new_sock))
          st->addr.address = std::move(*addr);
        if (auto port = caf::net::remote_port(*new_sock))
          st->addr.port = *port;
        BROKER_DEBUG("accepted new connection from socket"
                     << entry.fd << "-> register for reading"
                     << BROKER_ARG2("fd", new_sock->id));
        pending_fdset.push_back({new_sock->id, read_mask, 0});
        pending.emplace(new_sock->id, st);
        st->transition(&connect_state::await_hello);
      }
    } else {
      entry.events &= ~read_mask;
    }
  }

  void continue_writing(pollfd& entry) {
    BROKER_TRACE(BROKER_ARG2("fd", entry.fd));
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      switch (i->second->continue_writing(entry.fd)) {
        case rw_state::done:
          entry.events &= ~write_mask;
          if (i->second->done()) {
            BROKER_DEBUG("peer state reports done fd =" << entry.fd);
            finalize(entry, *i->second);
            pending.erase(i);
          }
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
    BROKER_TRACE(BROKER_ARG2("fd", entry.fd));
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      auto state = std::move(i->second);
      pending.erase(i);
      if (state->redundant) {
        BROKER_DEBUG("drop redundant connection on socket" << entry.fd);
        if (state->event_id != invalid_connector_event_id)
          listener->on_redundant_connection(state->event_id, state->remote_id,
                                            state->addr);
      } else if (state->event_id != invalid_connector_event_id) {
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
            listener->on_error(state->event_id,
                               make_error(ec::peer_unavailable));
        }
      } else {
        BROKER_DEBUG("incoming peering failed on socket" << entry.fd);
      }
    } else if (auto j = acceptors.find(entry.fd); j != acceptors.end()) {
      BROKER_ERROR("acceptor failed: socket" << entry.fd);
      acceptors.erase(j);
    }
    BROKER_DEBUG("close socket" << entry.fd);
    pending.erase(entry.fd);
    close(caf::net::socket{entry.fd});
    entry.events = 0;
  }

  int next_timeout() {
    if (retry_schedule.empty()) {
      return -1;
    } else {
      auto now = caf::make_timestamp();
      auto ts = retry_schedule.begin()->first;
      if (ts <= now) {
        return 0;
      } else {
        namespace sc = std::chrono;
        auto ms = sc::duration_cast<sc::milliseconds>(ts - now);
        return static_cast<int>(ms.count());
      }
    }
  }

  void handle_timeouts() {
    BROKER_TRACE("");
    auto now = caf::make_timestamp();
    while (!retry_schedule.empty() && retry_schedule.begin()->first <= now) {
      auto i = retry_schedule.begin();
      auto state = std::move(i->second);
      retry_schedule.erase(i);
      connect(std::move(state));
    }
  }

  void prepare_next_cycle() {
    BROKER_TRACE("pending handles:" << pending_fdset.size());
    auto is_done = [](auto& x) { return x.events == 0; };
    auto new_end = std::remove_if(fdset.begin(), fdset.end(), is_done);
    if (new_end != fdset.end()) {
#if CAF_LOG_LEVEL >= CAF_LOG_LEVEL_DEBUG
      std::for_each(new_end, fdset.end(), [](auto& x) {
        BROKER_DEBUG("drop completed socket from pollset"
                     << BROKER_ARG2("fd", x.fd));
      });
#endif
      fdset.erase(new_end, fdset.end());
    }
    if (!pending_fdset.empty()) {
      fdset.insert(fdset.end(), pending_fdset.begin(), pending_fdset.end());
      pending_fdset.clear();
    }
  }
};

peer_status_map& connect_state::peer_statuses() {
  return *mgr->peer_statuses_;
}

void connect_state::send_msg(caf::const_byte_span bytes, bool add_filter) {
  BROKER_ASSERT(bytes.size() >= handshake_prefix_size);
  auto old_size = wr_buf.size();
  wr_buf.insert(wr_buf.end(), bytes.begin(), bytes.end());
  if (add_filter) {
    [[maybe_unused]] auto ok
      = local_filter().read([this, old_size](auto ts, auto& xs) {
          caf::binary_serializer sink{nullptr, wr_buf};
          if (!sink.apply(ts) || !sink.apply(xs))
            return false;
          sink.seek(old_size);
          return sink.apply(static_cast<uint32_t>(wr_buf.size() - 4));
        });
    BROKER_ASSERT(ok);
  }
  BROKER_DEBUG("start writing a message of size" << wr_buf.size() - old_size);
  mgr->register_writing(this);
}

void connect_state::send_hello() {
  BROKER_TRACE("");
  send_msg(caf::as_bytes(caf::make_span(mgr->hello_buf)), false);
}

void connect_state::send_orig_syn() {
  BROKER_TRACE("");
  send_msg(caf::as_bytes(caf::make_span(mgr->orig_syn_buf)), true);
}

void connect_state::send_resp_syn_ack() {
  BROKER_TRACE("");
  send_msg(caf::as_bytes(caf::make_span(mgr->resp_syn_ack_buf)), true);
}

void connect_state::send_orig_ack() {
  BROKER_TRACE("");
  send_msg(caf::as_bytes(caf::make_span(mgr->orig_ack_buf)), false);
}

void connect_state::send_drop_conn() {
  BROKER_TRACE("");
  send_msg(caf::as_bytes(caf::make_span(mgr->drop_conn_buf)), false);
}

bool connect_state::handle_drop_conn() {
  BROKER_TRACE("");
  // The remote peer sends this message if we already have a connection
  // established. However, re-connects after an unpeering or other events may
  // produce conflicting views where the remote did not realize yet that this
  // connection no longer exists. Hence, we need to double-check whether we have
  // a connection or peering relation and otherwise we raise an error to trigger
  // retries.
  auto stat = peer_statuses().get(remote_id);
  BROKER_DEBUG("received drop_from from" << remote_id << "with peer status"
                                         << stat);
  switch (stat) {
    case peer_status::connected:
    case peer_status::peered:
      redundant = true;
      transition(&connect_state::fin);
      return true;
    default:
      transition(&connect_state::err);
      return false;
  }
}

bool connect_state::await_hello_or_orig_syn(alm_message_type msg) {
  BROKER_TRACE(msg);
  switch (msg) {
    default:
      transition(&connect_state::err);
      return false;
    case alm_message_type::hello:
      return await_hello(msg);
    case alm_message_type::originator_syn:
      return await_orig_syn(msg);
    case alm_message_type::drop_conn:
      return handle_drop_conn();
  }
}

bool connect_state::await_hello(alm_message_type msg) {
  BROKER_TRACE(msg);
  if (msg == alm_message_type::drop_conn) {
    return handle_drop_conn();
  } else if (msg != alm_message_type::hello) {
    transition(&connect_state::err);
    return false;
  }
  if (mgr->this_peer < remote_id) {
    auto proceed = [this] {
      send_orig_syn();
      transition(&connect_state::await_resp_syn_ack);
    };
    auto& psm = peer_statuses();
    auto status = peer_status::connecting;
    if (psm.insert(remote_id, status)) {
      added_peer_status = true;
      BROKER_ASSERT(status == peer_status::connecting);
      BROKER_DEBUG(remote_id << ":: () -> connecting");
      proceed();
    } else {
      for (;;) { // Repeat until we succeed or fail.
        switch (status) {
          case peer_status::initialized:
            if (psm.update(remote_id, status, peer_status::connecting)) {
              BROKER_DEBUG(remote_id << ":: initialized -> connecting");
              proceed();
              return true;
            }
            break;
          case peer_status::connecting:
          case peer_status::reconnecting:
            if (auto other = mgr->find_pending_handshake(remote_id)) {
              BROKER_DEBUG("detected redundant connection, enter paused state");
              BROKER_ASSERT(other != this);
              redundant = true;
              other->redundant_connections.emplace_back(shared_from_this());
              transition(&connect_state::paused);
              return true;
            } else {
              BROKER_DEBUG("detected redundant connection but "
                           "find_pending_handshake failed");
              transition(&connect_state::err);
              return false;
            }
            break;
          case peer_status::connected:
          case peer_status::peered:
            BROKER_DEBUG("detected redundant connection for connected peer");
            send_drop_conn();
            redundant = true;
            transition(&connect_state::fin);
            return true;
          case peer_status::disconnected:
            if (psm.update(remote_id, status, peer_status::reconnecting)) {
              BROKER_DEBUG(remote_id << ":: disconnected -> reconnecting");
              proceed();
              return true;
            }
            break;
          default:
            BROKER_ERROR("invalid peer status while handling 'hello'");
            transition(&connect_state::err);
            return false;
        }
      }
    }
  } else {
    send_hello();
    transition(&connect_state::await_orig_syn);
  }
  return true;
}

bool connect_state::await_orig_syn(alm_message_type msg) {
  BROKER_TRACE(msg);
  if (msg == alm_message_type::drop_conn) {
    return handle_drop_conn();
  } else if (msg != alm_message_type::originator_syn) {
    transition(&connect_state::err);
    return false;
  }
  auto proceed = [this] {
    send_resp_syn_ack();
    transition(&connect_state::await_orig_ack);
  };
  auto& psm = peer_statuses();
  auto status = peer_status::connecting;
  if (psm.insert(remote_id, status)) {
    added_peer_status = true;
    BROKER_ASSERT(status == peer_status::connecting);
    proceed();
    return true;
  } else {
    for (;;) { // Repeat until we succeed or fail.
      switch (status) {
        case peer_status::initialized:
          if (psm.update(remote_id, status, peer_status::connecting)) {
            BROKER_DEBUG(remote_id << ":: initialized -> connecting");
            proceed();
            return true;
          }
          break;
        case peer_status::disconnected:
          if (psm.update(remote_id, status, peer_status::reconnecting)) {
            BROKER_DEBUG(remote_id << ":: disconnected -> reconnecting");
            proceed();
            return true;
          }
          break;
        case peer_status::connecting:
        case peer_status::reconnecting:
        case peer_status::connected:
        case peer_status::peered:
          BROKER_DEBUG("detected redundant connection to"
                       << remote_id << "at the responder"
                       << BROKER_ARG(status));
          transition(&connect_state::err);
          return false;
        default:
          BROKER_ERROR("invalid peer status while handling 'orig_syn'");
          transition(&connect_state::err);
          return false;
      }
    }
  }
}

bool connect_state::await_resp_syn_ack(alm_message_type msg) {
  BROKER_TRACE(msg);
  if (msg != alm_message_type::responder_syn_ack) {
    transition(&connect_state::err);
    return false;
  }
  auto& psm = peer_statuses();
  auto status = peer_status::connecting;
  if (psm.update(remote_id, status, peer_status::connected)) {
    BROKER_DEBUG(remote_id << ":: connecting -> connected");
  } else if (status == peer_status::reconnecting
             && psm.update(remote_id, status, peer_status::connected)) {
    BROKER_DEBUG(remote_id << ":: reconnecting -> connected");
  } else {
    BROKER_ERROR("got a resp_syn_ack message but peer status does not match");
    return false;
  }
  send_orig_ack();
  transition(&connect_state::fin);
  return true;
}

bool connect_state::await_orig_ack(alm_message_type msg) {
  BROKER_TRACE(msg);
  if (msg != alm_message_type::originator_ack) {
    transition(&connect_state::err);
    return false;
  }
  auto& psm = peer_statuses();
  auto status = peer_status::connecting;
  if (psm.update(remote_id, status, peer_status::connected)) {
    BROKER_DEBUG(remote_id << ":: connecting -> connected");
  } else if (status == peer_status::reconnecting
             && psm.update(remote_id, status, peer_status::connected)) {
    BROKER_DEBUG(remote_id << ":: reconnecting -> connected");
  } else {
    BROKER_ERROR("got a resp_syn_ack message but peer status does not match");
    return false;
  }
  transition(&connect_state::fin);
  return true;
}

shared_filter_type& connect_state::local_filter() {
  return *mgr->filter;
}

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
    ::abort();
  }
}

connector::~connector() {
  caf::net::close(pipe_rd_);
  caf::net::close(pipe_wr_);
}

void connector::async_connect(connector_event_id event_id,
                              const network_info& addr) {
  BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(addr));
  auto buf = to_buf(connector_msg::connect, event_id, addr);
  write_to_pipe(buf);
}

void connector::async_drop(const connector_event_id event_id,
                           const network_info& addr) {
  BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(addr));
  auto buf = to_buf(connector_msg::drop, event_id, addr);
  write_to_pipe(buf);
}

void connector::async_listen(connector_event_id event_id,
                             const std::string& address, uint16_t port) {
  BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(address) << BROKER_ARG(port));
  auto buf = to_buf(connector_msg::listen, event_id, address, port);
  write_to_pipe(buf);
}

void connector::async_shutdown() {
  BROKER_TRACE("");
  auto buf = to_buf(connector_msg::shutdown);
  write_to_pipe(buf, true);
}

void connector::write_to_pipe(caf::span<const caf::byte> bytes,
                              bool shutdown_after_write) {
  BROKER_TRACE(bytes.size() << "bytes");
  std::unique_lock guard{mtx_};
  if (shutting_down_) {
    const char* errmsg = "failed to write to the pipe: shutting down";
    BROKER_ERROR(errmsg);
    throw std::runtime_error(errmsg);
  }
  auto res = caf::net::write(pipe_wr_, bytes);
  if (res != static_cast<ptrdiff_t>(bytes.size())) {
    const char* errmsg = "wrong number of bytes written to the pipe";
    BROKER_ERROR(errmsg);
    throw std::runtime_error(errmsg);
  }
  if (shutdown_after_write)
    shutting_down_ = true;
}

void connector::init(std::unique_ptr<listener> sub, shared_filter_ptr filter,
                     shared_peer_status_map_ptr peer_statuses) {
  BROKER_ASSERT(sub != nullptr);
  BROKER_ASSERT(filter != nullptr);
  std::unique_lock guard{mtx_};
  if (sub_ != nullptr)
    throw std::logic_error("connector::init called twice");
  BROKER_ASSERT(filter_ == nullptr);
  sub_ = std::move(sub);
  filter_ = std::move(filter);
  peer_statuses_ = std::move(peer_statuses);
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
  connect_manager mgr{this_peer_, sub, filter, peer_statuses_.get()};
  auto& fdset = mgr.fdset;
  fdset.push_back({pipe_rd_.id, read_mask, 0});
  bool done = false;
  pipe_reader prd{pipe_rd_, &done};
  // Loop until we receive a shutdown via the pipe.
  while (!done) {
    int presult =
#ifdef CAF_WINDOWS
      ::WSAPoll(fdset.data(), static_cast<ULONG>(fdset.size()),
                mgr.next_timeout());
#else
      ::poll(fdset.data(), static_cast<nfds_t>(fdset.size()),
             mgr.next_timeout());
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
        BROKER_DEBUG(BROKER_ARG2("fd", i->fd)
                     << BROKER_ARG2("event-mask", i->revents));
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
  for (auto& entry : fdset) {
    BROKER_DEBUG("close socket" << entry.fd);
    caf::net::close(caf::net::socket{entry.fd});
  }
}

} // namespace broker::detail
