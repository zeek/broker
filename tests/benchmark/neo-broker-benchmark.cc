#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <caf/deep_to_string.hpp>
#include <caf/downstream.hpp>
#include <caf/uuid.hpp>

#include "caf/binary_deserializer.hpp"
#include "caf/binary_serializer.hpp"
#include "caf/flow/async/publisher.hpp"
#include "caf/flow/async/publishing_queue.hpp"
#include "caf/flow/merge.hpp"
#include "caf/flow/publisher.hpp"
#include "caf/flow/subscriber.hpp"
#include "caf/init_global_meta_objects.hpp"
#include "caf/io/middleman.hpp"
#include "caf/net/actor_shell.hpp"
#include "caf/net/length_prefix_framing.hpp"
#include "caf/net/middleman.hpp"
#include "caf/net/socket_manager.hpp"
#include "caf/net/subscriber_adapter.hpp"
#include "caf/net/tcp_accept_socket.hpp"
#include "caf/openssl/all.hpp"
#include "caf/scheduled_actor/flow.hpp"
#include "caf/tag/message_oriented.hpp"

#include "broker/address.hh"
#include "broker/alm/lamport_timestamp.hh"
#include "broker/alm/multipath.hh"
#include "broker/config.hh"
#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/core_actor.hh"
#include "broker/data.hh"
#include "broker/detail/retry_state.hh"
#include "broker/endpoint.hh"
#include "broker/internal_command.hh"
#include "broker/port.hh"
#include "broker/publisher.hh"
#include "broker/snapshot.hh"
#include "broker/status.hh"
#include "broker/status_subscriber.hh"
#include "broker/store.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"
#include "broker/topic.hh"
#include "broker/version.hh"
#include "broker/zeek.hh"

// -- custom types -------------------------------------------------------------

using uuid = caf::uuid;

struct uuid_multipath_tree;

class uuid_multipath;
class uuid_multipath_group;
class uuid_multipath_node;

namespace broker::detail {

class connection_flow_handshake;
class publisher_flow_handshake;

using connection_flow_handshake_ptr
  = std::shared_ptr<connection_flow_handshake>;

using publisher_flow_handshake_ptr = std::shared_ptr<publisher_flow_handshake>;

} // namespace broker::detail

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::connection_flow_handshake_ptr)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::publisher_flow_handshake_ptr)

#define ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(neo_broker_bench, type)

CAF_BEGIN_TYPE_ID_BLOCK(neo_broker_bench, caf::id_block::broker::end)

  ADD_TYPE_ID((broker::detail::connection_flow_handshake_ptr))
  ADD_TYPE_ID((broker::detail::publisher_flow_handshake_ptr))
  ADD_TYPE_ID((uuid))
  ADD_TYPE_ID((uuid_multipath))

CAF_END_TYPE_ID_BLOCK(neo_broker_bench)

#undef ADD_TYPE_ID

struct uuid_multipath_tree {
  uuid_multipath_tree(uuid id, bool is_receiver);
  ~uuid_multipath_tree();
  uuid_multipath_node* root;
  broker::detail::monotonic_buffer_resource mem;
};

class uuid_multipath_group {
public:
  friend class uuid_multipath;
  friend class uuid_multipath_node;

  using iterator = broker::alm::node_iterator<uuid_multipath_node>;

  using const_iterator = broker::alm::node_iterator<const uuid_multipath_node>;

  uuid_multipath_group() noexcept = default;

  uuid_multipath_group(const uuid_multipath_group&) = delete;

  uuid_multipath_group& operator=(const uuid_multipath_group&) = delete;

  ~uuid_multipath_group();

  size_t size() const noexcept {
    return size_;
  }

  bool empty() const noexcept {
    return size_ == 0;
  }

  iterator begin() noexcept {
    return iterator{first_};
  }

  const_iterator begin() const noexcept {
    return const_iterator{first_};
  }

  iterator end() noexcept {
    return iterator{nullptr};
  }

  const_iterator end() const noexcept {
    return const_iterator{nullptr};
  }

  bool equals(const uuid_multipath_group& other) const noexcept;

  bool contains(uuid what) const noexcept;

  std::pair<uuid_multipath_node*, bool>
  emplace(broker::detail::monotonic_buffer_resource& mem, uuid id,
          bool is_receiver);

  bool emplace(uuid_multipath_node* node);

private:
  template <class MakeNewNode>
  std::pair<uuid_multipath_node*, bool> emplace_impl(uuid id,
                                                     MakeNewNode make_new_node);

  size_t size_ = 0;
  uuid_multipath_node* first_ = nullptr;
};


class uuid_multipath_node {
public:
  friend class uuid_multipath;
  friend class uuid_multipath_group;
  friend class broker::alm::node_iterator<const uuid_multipath_node>;
  friend class broker::alm::node_iterator<uuid_multipath_node>;
  friend struct uuid_multipath_tree;

  uuid_multipath_node(uuid id, bool is_receiver) noexcept
    : id_(id), is_receiver_(is_receiver) {
    // nop
  }

  uuid_multipath_node() = delete;

  uuid_multipath_node(const uuid_multipath_node&) = delete;

  uuid_multipath_node& operator=(const uuid_multipath_node&) = delete;

  ~uuid_multipath_node();

  const uuid& id() const noexcept {
    return id_;
  }

  bool is_receiver() const noexcept {
    return is_receiver_;
  }

  auto& nodes() noexcept {
    return down_;
  }

  const auto& nodes() const noexcept {
    return down_;
  }

  bool equals(const uuid_multipath_node& other) const noexcept;

  bool contains(uuid what) const noexcept;

private:
  template <class Inspector>
  bool save_children(Inspector& f) {
    if (f.begin_sequence(down_.size()))
      for (auto& child : down_)
        if (!child.save(f))
          return false;
    return f.end_sequence();
  }

  template <class Inspector>
  bool save(Inspector& f) {
    // We are lying to the inspector about the type, because multipath_node and
    // multipath_group are internal implementation details.
    return f.begin_object(caf::type_id_v<uuid_multipath>,
                          caf::type_name_v<uuid_multipath>)
           && f.begin_field("id")          // <id>
           && f.apply(id_)                 //   [...]
           && f.end_field()                // </id>
           && f.begin_field("is_receiver") // <is_receiver>
           && f.apply(is_receiver_)        //   [...]
           && f.end_field()                // </is_receiver>
           && f.begin_field("nodes")       // <nodes>
           && save_children(f)             //   [...]
           && f.end_field()                // </nodes>
           && f.end_object();
  }

  template <class Inspector>
  bool load_children(broker::detail::monotonic_buffer_resource& mem,
                     Inspector& f) {
    size_t n = 0;
    if (f.begin_sequence(n)) {
      for (size_t i = 0; i < n; ++i) {
        auto child = broker::detail::new_instance<uuid_multipath_node>(mem, uuid{}, false);
        if (!child->load(mem, f)) {
          return false;
        } else if (!down_.emplace(child)) {
          f.emplace_error(caf::sec::field_invariant_check_failed,
                          "a multipath may not contain duplicates");
          return false;
        }
      }
    }
    return f.end_sequence();
  }

  template <class Inspector>
  bool load(broker::detail::monotonic_buffer_resource& mem, Inspector& f) {
    return f.begin_object(caf::type_id_v<uuid_multipath>,
                          caf::type_name_v<uuid_multipath>)
           && f.begin_field("id")          // <id>
           && f.apply(id_)                 //   [...]
           && f.end_field()                // </id>
           && f.begin_field("is_receiver") // <is_receiver>
           && f.apply(is_receiver_)        //   [...]
           && f.end_field()                // </is_receiver>
           && f.begin_field("nodes")       // <nodes>
           && load_children(mem, f)        //   [...]
           && f.end_field()                // </nodes>
           && f.end_object();
  }

  uuid id_;
  bool is_receiver_;
  uuid_multipath_node* right_ = nullptr;
  uuid_multipath_group down_;
};

class uuid_multipath {
public:
  using tree_ptr = std::shared_ptr<uuid_multipath_tree>;

  uuid_multipath();

  uuid_multipath(uuid id, bool is_receiver);

  uuid_multipath(const tree_ptr& t, uuid_multipath_node* h);

  explicit uuid_multipath(const tree_ptr& tptr)
    : uuid_multipath(tptr, tptr->root) {
    // nop
  }

  uuid_multipath(uuid_multipath&& other) noexcept = default;

  uuid_multipath(const uuid_multipath& other) = default;

  uuid_multipath& operator=(uuid_multipath&& other) noexcept = default;

  uuid_multipath& operator=(const uuid_multipath& other) = default;

  auto id() const noexcept {
    return head_->id_;
  }

  auto is_receiver() const noexcept {
    return head_->is_receiver_;
  }

  bool equals(const uuid_multipath& other) const noexcept;

  bool contains(uuid what) const noexcept;

  size_t num_nodes() const noexcept {
    return head_->down_.size();
  }

  template <class Inspector>
  friend bool inspect(Inspector& f, uuid_multipath& x) {
    if constexpr (Inspector::is_loading)
      return x.head_->load(x.tree_->mem, f);
    else
      return x.head_->save(f);
  }

private:
  auto emplace(uuid id, bool is_receiver) {
    return head_->down_.emplace(tree_->mem, id, is_receiver);
  }

  std::shared_ptr<uuid_multipath_tree> tree_;

  uuid_multipath_node* head_;
};

/// @relates multipath
inline bool operator==(const uuid_multipath& x, const uuid_multipath& y) {
  return x.equals(y);
}

/// @relates multipath
inline bool operator!=(const uuid_multipath& x, const uuid_multipath& y) {
  return !(x == y);
}

uuid_multipath_tree::uuid_multipath_tree(uuid id, bool is_receiver) {
  root = broker::detail::new_instance<uuid_multipath_node>(mem, id, is_receiver);
}

uuid_multipath_tree::~uuid_multipath_tree() {
  // nop; we can simply "wink out" the tree structure.
}

uuid_multipath_group::~uuid_multipath_group() {
  delete first_;
}

bool
uuid_multipath_group::equals(const uuid_multipath_group& other) const noexcept {
  auto eq = [](const auto& lhs, const auto& rhs) { return lhs.equals(rhs); };
  return std::equal(begin(), end(), other.begin(), other.end(), eq);
}

bool uuid_multipath_group::contains(uuid id) const noexcept {
  auto pred = [&id](const uuid_multipath_node& node) {
    return node.contains(id);
  };
  return std::any_of(begin(), end(), pred);
}

template <class MakeNewNode>
std::pair<uuid_multipath_node*, bool>
uuid_multipath_group::emplace_impl(uuid id, MakeNewNode make_new_node) {
  if (size_ == 0) {
    first_ = make_new_node();
    size_ = 1;
    return {first_, true};
  } else {
    // Insertion sorts by ID.
    BROKER_ASSERT(first_ != nullptr);
    if (first_->id_ == id) {
      return {first_, false};
    } else if (first_->id_ > id) {
      ++size_;
      auto new_node = make_new_node();
      new_node->right_ = first_;
      first_ = new_node;
      return {new_node, true};
    }
    auto pos = first_;
    auto next = pos->right_;
    while (next != nullptr) {
      if (next->id_ == id) {
        return {next, false};
      } else if (next->id_ > id) {
        ++size_;
        auto new_node = make_new_node();
        pos->right_ = new_node;
        new_node->right_ = next;
        return {new_node, true};
      } else {
        pos = next;
        next = next->right_;
      }
    }
    ++size_;
    auto new_node = make_new_node();
    BROKER_ASSERT(pos->right_ == nullptr);
    pos->right_ = new_node;
    return {new_node, true};
  }
}

std::pair<uuid_multipath_node*, bool>
uuid_multipath_group::emplace(broker::detail::monotonic_buffer_resource& mem,
                              uuid id, bool is_receiver) {
  auto make_new_node = [&mem, id, is_receiver] {
    return broker::detail::new_instance<uuid_multipath_node>(mem, id,
                                                             is_receiver);
  };
  return emplace_impl(id, make_new_node);
}

bool uuid_multipath_group::emplace(uuid_multipath_node* new_node) {
  auto make_new_node = [new_node] { return new_node; };
  return emplace_impl(new_node->id_, make_new_node).second;
}

uuid_multipath_node::~uuid_multipath_node() {
  delete right_;
}

bool
uuid_multipath_node::equals(const uuid_multipath_node& other) const noexcept {
  return id_ == other.id_ && down_.equals(other.down_);
}

bool uuid_multipath_node::contains(uuid what) const noexcept {
  return id_ == what || down_.contains(what);
}

uuid_multipath::uuid_multipath() {
  tree_ = std::make_shared<uuid_multipath_tree>(uuid{}, false);
  head_ = tree_->root;
}

uuid_multipath::uuid_multipath(uuid id, bool is_receiver) {
  tree_ = std::make_shared<uuid_multipath_tree>(id, is_receiver);
  head_ = tree_->root;
}

uuid_multipath::uuid_multipath(const tree_ptr& t, uuid_multipath_node* h)
  : tree_(t), head_(h) {
  // nop
}

bool uuid_multipath::equals(const uuid_multipath& other) const noexcept {
  return head_->equals(*other.head_);
}

bool uuid_multipath::contains(uuid what) const noexcept {
  return head_->contains(what);
}


namespace {

using namespace broker;

int event_type = 1;
double batch_rate = 1;
int batch_size = 1;
double rate_increase_interval = 0;
double rate_increase_amount = 0;
uint64_t max_received = 0;
uint64_t max_in_flight = 0;
bool server = false;
bool verbose = false;

// Global state
size_t total_recv;
size_t total_sent;
size_t last_sent;
double last_t;

std::atomic<size_t> num_events;

size_t reset_num_events() {
  auto result = num_events.load();
  if (result == 0)
    return 0;
  for (;;)
    if (num_events.compare_exchange_strong(result, 0))
      return result;
}

double current_time() {
  using namespace std::chrono;
  auto t = system_clock::now();
  auto usec = duration_cast<microseconds>(t.time_since_epoch()).count();
  return usec / 1e6;
}

static std::string random_string(int n) {
    static unsigned int i = 0;
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

     const size_t max_index = (sizeof(charset) - 1);
     char buffer[11];
     for ( unsigned int j = 0; j < sizeof(buffer) - 1; j++ )
    buffer[j] = charset[++i % max_index];
     buffer[sizeof(buffer) - 1] = '\0';

     return buffer;
}

static uint64_t random_count() {
    static uint64_t i = 0;
    return ++i;
}

vector createEventArgs() {
    switch ( event_type ) {
     case 1: {
         return std::vector<data>{42, "test"};
     }

     case 2: {
         // This resembles a line in conn.log.
         address a1;
         address a2;
         convert("1.2.3.4", a1);
         convert("3.4.5.6", a2);

         return vector{
             now(),
             random_string(10),
             vector{
                 a1,
                 port(4567, port::protocol::tcp),
                 a2,
                 port(80, port::protocol::tcp)
             },
             enum_value("tcp"),
             random_string(10),
             std::chrono::duration_cast<timespan>(std::chrono::duration<double>(3.14)),
             random_count(),
             random_count(),
             random_string(5),
             true,
             false,
             random_count(),
             random_string(10),
             random_count(),
             random_count(),
             random_count(),
             random_count(),
             set({random_string(10), random_string(10)})
        };
     }

     case 3: {
         table m;

         for ( int i = 0; i < 100; i++ ) {
             set s;
             for ( int j = 0; j < 10; j++ )
                 s.insert(random_string(5));
             m[random_string(15)] = s;
         }

         return vector{now(), m};
     }

     default:
       std::cerr << "invalid event type" << std::endl;
       abort();
    }
}

void send_batch(endpoint& ep, publisher& p) {
  auto name = "event_" + std::to_string(event_type);
  vector batch;
  for (int i = 0; i < batch_size; i++) {
    auto ev = zeek::Event(std::string(name), createEventArgs());
    batch.emplace_back(std::move(ev));
  }
  total_sent += batch.size();
  p.publish(std::move(batch));
}

void receivedStats(endpoint& ep, data x) {
  // Example for an x: '[1, 1, [stats_update, [1ns, 1ns, 0]]]'.
  // We are only interested in the '[1ns, 1ns, 0]' part.
  auto xvec = caf::get<vector>(x);
  auto yvec = caf::get<vector>(xvec[2]);
  auto rec = caf::get<vector>(yvec[1]);

  double t;
  convert(caf::get<timestamp>(rec[0]), t);

  double dt_recv;
  convert(caf::get<timespan>(rec[1]), dt_recv);

  auto ev1 = caf::get<count>(rec[2]);
  auto all_recv = ev1;
  total_recv += ev1;

  auto all_sent = (total_sent - last_sent);

  double now;
  convert(broker::now(), now);
  double dt_sent = (now - last_t);

  auto recv_rate = (double(all_recv) / dt_recv);
  auto send_rate = double(total_sent - last_sent) / dt_sent;
  auto in_flight = (total_sent - total_recv);

  std::cerr << to_string(t) << " "
            << "[batch_size=" << batch_size << "] "
            << "in_flight=" << in_flight << " "
            << "d_t=" << dt_recv << " "
            << "d_recv=" << all_recv << " "
            << "d_sent=" << all_sent << " "
            << "total_recv=" << total_recv << " "
            << "total_sent=" << total_sent << " "
            << "[sending at " << send_rate << " ev/s, receiving at "
            << recv_rate << " ev/s " << std::endl;

  last_t = now;
  last_sent = total_sent;

  if (max_received && total_recv > max_received) {
    zeek::Event ev("quit_benchmark", std::vector<data>{});
    ep.publish("/benchmark/terminate", ev);
    std::this_thread::sleep_for(std::chrono::seconds(2)); // Give clients a bit.
    exit(0);
  }

  static int max_exceeded_counter = 0;
  if (max_in_flight && in_flight > max_in_flight) {

    if (++max_exceeded_counter >= 5) {
      std::cerr << "max-in-flight exceeded for 5 subsequent batches"
                << std::endl;
      exit(1);
    }
  } else
    max_exceeded_counter = 0;
}

struct source_state {
  static inline const char* name = "broker.benchmark.source";
};

struct sink_state {
  static inline const char* name = "broker.benchmark.sink";
};

} // namespace

namespace broker::detail {

struct peer_message {
  uuid_multipath path;
  packed_message content;
};

struct connection_flow_handshake_sync {
  std::mutex mtx;
  std::condition_variable cv;
  caf::flow::async::publisher_ptr<peer_message> pub;
  error err;

  void ack(caf::flow::async::publisher_ptr<peer_message> ptr) {
    BROKER_ASSERT(ptr != nullptr);
    std::unique_lock guard{mtx};
    pub = std::move(ptr);
    cv.notify_all();
  }

  void reject(caf::error reason) {
    BROKER_ASSERT(!reason.empty());
    std::unique_lock guard{mtx};
    err = std::move(reason);
    cv.notify_all();
  }
};

class connection_flow_handshake {
  public:
    connection_flow_handshake_sync* sync;
    uuid peer_id;
    caf::flow::async::publisher_ptr<peer_message> peer_input;

    connection_flow_handshake(
      connection_flow_handshake_sync* sync, uuid peer_id,
      caf::flow::async::publisher_ptr<peer_message> peer_input)
      : sync(sync), peer_id(peer_id), peer_input(std::move(peer_input)) {
      }

    auto handshake_data() {
      return std::forward_as_tuple(peer_id, peer_input);
    }

    ~connection_flow_handshake() {
      if (sync)
        sync->reject(caf::sec::broken_promise);
    }

    void ack(caf::flow::async::publisher_ptr<peer_message> ptr) {
      if (sync) {
        sync->ack(std::move(ptr));
        sync = nullptr;
      }
    }

    void reject(caf::error reason) {
      if (sync) {
        sync->reject(std::move(reason));
        sync = nullptr;
      }
    }
};

using connection_flow_handshake_ptr
= std::shared_ptr<connection_flow_handshake>;

class publisher_flow_handshake {
  public:
    using data_msg_publisher_ptr = caf::flow::async::publisher_ptr<data_message>;

    using command_msg_publisher_ptr = caf::flow::async::publisher_ptr<command_message>;

    using variant_type
      = std::variant<data_msg_publisher_ptr, command_msg_publisher_ptr>;

    std::unique_ptr<std::promise<bool>> promise;

    variant_type src;

    template <class PublisherPtr>
      publisher_flow_handshake(std::unique_ptr<std::promise<bool>> promise,
                               PublisherPtr&& src)
      : promise(std::move(promise)), src(std::forward<PublisherPtr>(src)) {
        // nop
      }

    ~publisher_flow_handshake() {
      if (promise)
        promise->set_value(false);
    }

    void ack() {
      if (promise) {
        promise->set_value(true);
        promise.reset();
      }
    }

    void reject() {
      if (promise) {
        promise->set_value(false);
        promise.reset();
      }
    }
};

using publisher_flow_handshake_ptr = std::shared_ptr<publisher_flow_handshake>;

class serializer_state {
public:
  caf::event_based_actor* self;

  caf::flow::merger_ptr<packed_message> merger;

  using data_msg_publisher_ptr = caf::flow::async::publisher_ptr<data_message>;

  using command_msg_publisher_ptr
    = caf::flow::async::publisher_ptr<command_message>;

  explicit serializer_state(caf::event_based_actor* self) : self(self) {
    merger.emplace(self);
    merger->delay_error(true);
    merger->shutdown_on_last_complete(false);
  }

  template <class MsgType>
  static packed_message pack(const MsgType& msg) {
    packed_message_type msg_type;
    caf::byte_buffer buf;
    buf.reserve(256);
    caf::binary_serializer sink{nullptr, buf};
    bool ok;
    if constexpr (std::is_same_v<MsgType, data_message>) {
      msg_type = packed_message_type::data;
      ok = sink.apply(get_data(msg));
    } else {
      static_assert(std::is_same_v<MsgType, command_message>);
      msg_type = packed_message_type::command;
      ok = sink.apply(get_command(msg));
    }
    if (!ok) {
      BROKER_ERROR("failed to serialize message:" << sink.get_error());
      throw std::runtime_error("failed to serialize message");
    }
    return packed_message{msg_type, get_topic(msg), std::move(buf)};
  }

  caf::behavior make_behavior() {
    return {
      [this](publisher_flow_handshake_ptr& ptr) {
//         if (!merger->has_subscribers()) {
//           ptr->reject();
//           return;
//         }
        auto add_src = [this](auto& src) {
          merger->add(
            self->observe(src)->map([](const auto& msg) { return pack(msg); }));
        };
        std::visit(add_src, ptr->src);
        ptr->ack();
      },
    };
  }
};

using serializer_actor = caf::stateful_actor<serializer_state>;

class deserializer_state {
public:
  using data_msg_publisher_ptr = caf::flow::publisher_ptr<data_message>;

  using command_msg_publisher_ptr = caf::flow::publisher_ptr<command_message>;

  caf::event_based_actor* self;

  deserializer_state(caf::event_based_actor* self) : self(self) {
    // nop
  }

  template <class MsgType>
  static std::optional<MsgType> unpack(const packed_message& msg) {
    const auto& [msg_type, msg_topic, msg_data] = msg.data();
    if constexpr (std::is_same_v<MsgType, data_message>) {
      if (msg_type != packed_message_type::data)
        return std::nullopt;
    } else {
      static_assert(std::is_same_v<MsgType, command_message>);
      if (msg_type != packed_message_type::command)
        return std::nullopt;
    }
    caf::binary_deserializer source{nullptr, msg_data};
    using value_type = std::conditional_t<std::is_same_v<MsgType, data_message>,
                                          data, internal_command>;
    value_type result;
    if (!source.apply(result)) {
      BROKER_ERROR("failed to deserialize message:" << source.get_error());
      return std::nullopt;
    }
    return MsgType{get<1>(msg), std::move(result)};
  }

  struct data_msg_step {
    using input_type = packed_message;
    using output_type = data_message;
    template <class Next>
    void apply(const input_type& item, Next* next) {
      if (auto val = unpack<output_type>(item))
        next->append_to_buf(std::move(*val));
    }
  };

  struct command_msg_step {
    using input_type = packed_message;
    using output_type = command_message;
    template <class Next>
    void apply(const input_type& item, Next* next) {
      if (auto val = unpack<output_type>(item))
        next->append_to_buf(std::move(*val));
    }
  };

  auto init(const caf::flow::publisher_ptr<packed_message>& in) {
    return std::make_tuple(in->transform(data_msg_step{}),
                           in->transform(command_msg_step{}));
  }
};

using deserializer_actor = caf::stateful_actor<deserializer_state>;

class controller_state;

class controller_forwarder : public caf::flow::merger<peer_message>::forwarder {
  public:
    using super = caf::flow::merger<peer_message>::forwarder;

    controller_forwarder(caf::flow::merger_ptr<peer_message> parent,
                         controller_state* state, uuid this_peer)
      : super(std::move(parent)), state_(state) {
        // nop
      }

    void on_complete() override;

    void on_error(const error& what) override;

  private:
    controller_state* state_;
    uuid this_peer_;
};

class controller_state {
public:
  explicit controller_state(caf::event_based_actor* self, uuid this_endpoint)
    : self(self), this_endpoint(this_endpoint) {
    // nop
  }

  struct route_step {
    using input_type = packed_message;
    using output_type = peer_message;
    controller_state* state;
    template <class Next>
    void apply(const input_type& item, Next* next) {
      // TODO: add actual routing logic.
      for (auto& kvp : state->peers)
        next->append_to_buf(peer_message{uuid_multipath{kvp.first, true}, item});
    }
  };

  struct select_local_step {
    using input_type = peer_message;
    using output_type = packed_message;
    controller_state* state;
    template <class Next>
    void apply(const input_type& item, Next* next) {
      // TODO: filter by locally subscribed topics?
      if (item.path.is_receiver() && item.path.id() == state->this_endpoint)
        next->append_to_buf(item.content);
    }
  };

  auto init(const caf::flow::publisher_ptr<packed_message>& in) {
    merger.emplace(self);
    merger->add(in->transform(route_step{this}));
    return merger->transform(select_local_step{this});
  }

  caf::behavior make_behavior() {
    return {
      [this](connection_flow_handshake_ptr& req) {
        BROKER_ASSERT(req != nullptr);
        auto&& [new_peer, peer_input] = req->handshake_data();
        if (peers.emplace(new_peer, peer_input).second) {
          merger->add(
            self->observe(peer_input),
            caf::make_counted<controller_forwarder>(merger, this, new_peer));
          auto predicate = [id{new_peer}](const peer_message& msg) {
            return msg.path.id() == id;
          };
          req->ack(self->to_async_publisher(merger->filter(predicate)));
        } else {
          req->reject(ec::repeated_peering_handshake_request);
        }
      },
    };
  }

  void peer_added(uuid) {
    // TODO: implement me
  }

  void peer_completed(uuid) {
    // TODO: implement me
  }

  void peer_failed(uuid, const caf::error&) {
    // TODO: implement me
  }

  caf::event_based_actor* self;
  uuid this_endpoint;
  caf::flow::merger_ptr<peer_message> merger;
  std::unordered_map<uuid, caf::flow::async::publisher_ptr<peer_message>> peers;
};

using controller_actor = caf::stateful_actor<controller_state>;

void controller_forwarder::on_complete() {
  state_->peer_completed(this_peer_);
  super::on_complete();
}

void controller_forwarder::on_error(const error& what) {
  // Handle error here and hide it from the merger.
  state_->peer_failed(this_peer_, what);
  super::on_complete();
}

///
class publisher_connector : public caf::ref_counted {
  public:
    ///
    virtual bool connect(caf::flow::async::publisher_ptr<data_message> input) = 0;

    ///
    virtual bool connect(caf::flow::async::publisher_ptr<command_message> input)
      = 0;
};

using publisher_connector_ptr = caf::intrusive_ptr<publisher_connector>;

class publisher_connector_impl : public publisher_connector {
  public:
    explicit publisher_connector_impl(caf::actor hdl) : hdl_(std::move(hdl)) {
      // nop
    }

    bool connect(caf::flow::async::publisher_ptr<data_message> input) override {
      return connect_impl(std::move(input));
    }

    bool
      connect(caf::flow::async::publisher_ptr<command_message> input) override {
        return connect_impl(std::move(input));
      }

  private:
    template <class PublisherPtr>
      bool connect_impl(PublisherPtr input) {
        auto promise = std::make_unique<std::promise<bool>>();
        auto res = promise->get_future();
        auto ptr = std::make_shared<publisher_flow_handshake>(std::move(promise),
                                                              std::move(input));
        anon_send(hdl_, std::move(ptr));
        return res.get();
      }

    caf::actor hdl_;
};

///
class controller_connector : public caf::ref_counted {
  public:
    ///
    virtual caf::expected<caf::flow::async::publisher_ptr<peer_message>>
      connect(uuid peer, caf::flow::async::publisher_ptr<peer_message> input) = 0;
};

using controller_connector_ptr = caf::intrusive_ptr<controller_connector>;

class controller_connector_impl : public controller_connector {
  public:
    explicit controller_connector_impl(caf::actor hdl) : hdl_(std::move(hdl)) {
      // nop
    }

    caf::expected<caf::flow::async::publisher_ptr<peer_message>>
      connect(uuid peer,
              caf::flow::async::publisher_ptr<peer_message> input) override {
        connection_flow_handshake_sync sync;
        auto ptr = std::make_shared<connection_flow_handshake>(
          &sync, peer, std::move(input));
        anon_send(hdl_, std::move(ptr));
        std::unique_lock guard{sync.mtx};
        while (sync.pub == nullptr && sync.err.empty())
          sync.cv.wait(guard);
        if (sync.pub)
          return {std::move(sync.pub)};
        else
          return {std::move(sync.err)};
      }

  private:
    caf::actor hdl_;
};

enum class broker_network_message_type : uint8_t {
  syn,
  syn_ack,
  ack,
  data_msg,
  command_msg,
};

template <class Inspector>
bool inspect(Inspector& f, broker_network_message_type& x) {
  auto get = [&x] { return static_cast<uint8_t>(x); };
  auto set = [&x](uint8_t val) {
    // TODO: check whether val is a legal value.
    x = static_cast<broker_network_message_type>(val);
    return true;
  };
  return f.apply(get, set);
}

struct originator_tag{};
struct responder_tag{};

class app {
public:
  using input_tag = caf::tag::message_oriented;

  enum state_t {
    await_syn,
    await_syn_ack,
    await_ack,
    await_data,
  };

  app(uuid this_peer, controller_connector_ptr connector, originator_tag)
    : state_(await_syn_ack),
      this_peer_(this_peer),
      connector_(std::move(connector)) {
    // nop
  }

  app(uuid this_peer, controller_connector_ptr connector, responder_tag)
    : state_(await_syn),
      this_peer_(this_peer),
      connector_(std::move(connector)) {
    // nop
  }

  template <class LowerLayerPtr>
  caf::error init(caf::net::socket_manager* mgr, LowerLayerPtr down,
                  const caf::settings&) {
    if (state_ == await_syn_ack) {
      down->begin_message();
      caf::binary_serializer sink{nullptr, down->message_buffer()};
      std::ignore = sink.apply(broker_network_message_type::syn);
      std::ignore = sink.apply(this_peer_);
      if (!down->end_message())
        return make_error(ec::shutting_down);
    }
    mgr_ = mgr;
    controller_messages_.emplace(mgr);
    return caf::none;
  }

  template <class LowerLayerPtr>
  bool prepare_send(LowerLayerPtr down) {
    while (!done_ && down->can_send_more()) {
      auto [val, done, err] = controller_messages_->poll();
      if (val) {
        if (!write(down, *val)) {
          down->abort_reason(make_error(ec::invalid_message));
          return false;
        }
      } else if (done) {
        done_ = true;
        if (err) {
          down->abort_reason(*err);
          return false;
        }
      } else {
        break;
      }
    }
    return true;
  }

  template <class LowerLayerPtr>
  bool done_sending(LowerLayerPtr) {
    return !controller_messages_->has_data();
  }

  template <class LowerLayerPtr>
  void abort(LowerLayerPtr, const caf::error&) {
    // nop
  }

  template <class LowerLayerPtr>
  ptrdiff_t consume(LowerLayerPtr down, caf::byte_span buf) {
    using bnm = broker_network_message_type;
    caf::binary_deserializer src{nullptr, buf};
    bnm tag;
    if (!src.apply(tag))
      return -1;
    switch (tag) {
      case bnm::syn:
        if (!handle_syn(down, src))
          return -1;
        break;
      case bnm::syn_ack:
        if (!handle_syn_ack(down, src))
          return -1;
        break;
      case bnm::ack:
        if (!handle_ack(down, src))
          return -1;
        break;
      case bnm::data_msg:
        if (!handle_data_msg(down, src))
          return -1;
        break;
      case bnm::command_msg:
        if (!handle_command_msg(down, src))
          return -1;
        break;
      default:
        // Illegal message type.
        return -1;
    }
    return static_cast<ptrdiff_t>(buf.size());
  }

private:
  template <class LowerLayerPtr>
  bool handle_syn(LowerLayerPtr down, caf::binary_deserializer& src) {
    if (state_ != await_syn) {
      BROKER_ERROR("got unexpected SYN message");
      down->abort_reason(make_error(ec::invalid_message));
      return false;
    }
    if (!src.apply(peer_)) {
      BROKER_ERROR("failed to read peer ID from handshake");
      down->abort_reason(make_error(ec::unexpected_handshake_message));
      return false;
    }
    if (peer_ == this_peer_) {
      BROKER_ERROR("tried to connect to self");
      down->abort_reason(make_error(ec::unexpected_handshake_message));
      return false;
    }
    if (!src.remainder().empty()) {
      BROKER_ERROR("handshake message contains unexpected data");
      down->abort_reason(make_error(ec::unexpected_handshake_message));
      return false;
    }
    state_ = await_ack;
    down->begin_message();
    caf::binary_serializer sink{nullptr, down->message_buffer()};
    std::ignore = sink.apply(broker_network_message_type::syn_ack);
    std::ignore = sink.apply(this_peer_);
    return down->end_message();
  }

  template <class LowerLayerPtr>
  bool handle_syn_ack(LowerLayerPtr down, caf::binary_deserializer& src) {
    if (state_ != await_syn_ack) {
      BROKER_ERROR("got unexpected SYN-ACK message");
      down->abort_reason(make_error(ec::invalid_message));
      return false;
    }
    if (!src.apply(peer_)) {
      BROKER_ERROR("failed to read peer ID from handshake");
      down->abort_reason(make_error(ec::unexpected_handshake_message));
      return false;
    }
    if (peer_ == this_peer_) {
      BROKER_ERROR("tried to connect to self");
      down->abort_reason(make_error(ec::unexpected_handshake_message));
      return false;
    }
    if (!src.remainder().empty()) {
      BROKER_ERROR("handshake message contains unexpected data");
      down->abort_reason(make_error(ec::unexpected_handshake_message));
      return false;
    }
    state_ = await_data;
    if (initialize_flows(down)) {
      down->begin_message();
      caf::binary_serializer sink{nullptr, down->message_buffer()};
      std::ignore = sink.apply(broker_network_message_type::ack);
      return down->end_message();
    } else {
      return false;
    }
  }

  template <class LowerLayerPtr>
  bool handle_ack(LowerLayerPtr down, caf::binary_deserializer& src) {
    if (state_ != await_ack) {
      BROKER_ERROR("got unexpected ACK message");
      down->abort_reason(make_error(ec::invalid_message));
      return false;
    }
    state_ = await_data;
    return initialize_flows(down);
  }

  template <class LowerLayerPtr>
  bool handle_data_msg(LowerLayerPtr down, caf::binary_deserializer& src) {
    if (state_ != await_data) {
      BROKER_ERROR("got unexpected data message");
      down->abort_reason(make_error(ec::invalid_message));
      return false;
    }
    auto fail = [&down] {
      BROKER_ERROR("got malformed data message");
      down->abort_reason(make_error(ec::invalid_message));
      return false;
    };
    // Extract path.
    uuid_multipath path;
    if (!src.apply(path))
      return fail();
    // Extract topic.
    topic msg_topic;
    uint16_t topic_len = 0;
    if (!src.apply(topic_len) || topic_len == 0) {
      return fail();
    } else {
      auto remainder = src.remainder();
      if (remainder.size() <= topic_len)
        return fail();
      auto topic_str = std::string{
        reinterpret_cast<const char*>(remainder.data()), topic_len};
      msg_topic = topic{std::move(topic_str)};
      src.skip(topic_len);
    }
    // Copy payload to a new byte buffer.
    BROKER_ASSERT(src.remaining() > 0);
    auto bytes = src.remainder();
    caf::byte_buffer payload{bytes.begin(), bytes.end()};
    // Push data down the pipeline.
    static constexpr auto msg_type = packed_message_type::data;
    auto tup = make_cow_tuple(msg_type, msg_topic, std::move(payload));
    auto msg = peer_message{std::move(path), std::move(tup)};
    // peer_messages_->try_push(std::move(msg));
    auto ok = peer_messages_->try_push(std::move(msg));
    if (!ok) {
      ++dropped_;
    }
      auto tn = clock_type::now();
      if (tn - t_ > std::chrono::seconds(1)) {
        std::cout << dropped_ << " drops/s\n";
        t_ = tn;
        dropped_ = 0;
      }
    return true;
  }

  using clock_type=std::chrono::steady_clock;
  size_t dropped_=0;
  clock_type::time_point t_=clock_type::now();

  template <class LowerLayerPtr>
  bool handle_command_msg(LowerLayerPtr down, caf::binary_deserializer& src) {
    if (state_ != await_data) {
      BROKER_ERROR("got unexpected command message");
      down->abort_reason(make_error(ec::invalid_message));
      return false;
    }
    down->abort_reason(make_error(ec::unspecified, "not implemented yet"));
    return false;
  }

  template <class LowerLayerPtr>
  bool write(LowerLayerPtr down, none) {
    return true;
  }

  template <class LowerLayerPtr>
  bool write(LowerLayerPtr down, const peer_message& msg) {
    auto&& [msg_type, msg_topic, payload] = msg.content.data();
    auto bnmt = msg_type == packed_message_type::data ?
                  broker_network_message_type::data_msg :
                  broker_network_message_type::command_msg;
    down->begin_message();
    caf::binary_serializer sink{nullptr, down->message_buffer()};
    auto write_bytes = [&sink](auto&& bytes) {
      sink.buf().insert(sink.buf().end(), bytes.begin(), bytes.end());
      return true;
    };
    auto write_topic
      = [&sink, &write_bytes](const auto& x) {
          const auto& str = x.string();
          if (str.size() > 0xFFFF) {
            BROKER_ERROR("topic exceeds maximum size of 65535 characters");
            return false;
          }
          return sink.apply(static_cast<uint16_t>(str.size()))
                 && write_bytes(caf::as_bytes(caf::make_span(str)));
        };
    return sink.apply(bnmt)          //
           && sink.apply(msg.path)   //
           && write_topic(msg_topic) //
           && write_bytes(payload)   //
           && down->end_message();   // Flush.
  }

  template <class LowerLayerPtr>
  bool initialize_flows(LowerLayerPtr down) {
    if (connector_ == nullptr) {
      BROKER_ERROR("received repeated handshake");
      down->abort_reason(make_error(ec::repeated_peering_handshake_request));
      return false;
    }
    BROKER_ASSERT(peer_messages_ == nullptr);
    using caf::flow::async::make_publishing_queue;
    auto& sys = mgr_->system();
    auto [queue_ptr, pub_ptr] = make_publishing_queue<peer_message>(sys, 512);
    auto conn_res = connector_->connect(peer_, std::move(pub_ptr));
    if (!conn_res) {
      BROKER_ERROR("peer refused by connector:" << conn_res.error());
      down->abort_reason(make_error(ec::invalid_handshake_state));
      return false;
    }
    peer_messages_ = std::move(queue_ptr);
    (*conn_res)->async_subscribe(controller_messages_);
    connector_ = nullptr;
    return true;
  }

  /// Configures which handlers are currently active and what inputs are allowed
  /// at this point.
  state_t state_;

  /// Stores the ID of the local Broker endpoint.
  uuid this_peer_;

  /// Stores the ID of the connected Broker endpoint.
  uuid peer_;

  /// Manages the handshake
  controller_connector_ptr connector_;

  /// Stores whether the controller signaled shutdown.
  bool done_ = false;

  /// Forwards outgoing messages to the peer. We write whatever we receive from
  /// this channel to the socket.
  caf::net::subscriber_adapter_ptr<peer_message> controller_messages_;

  /// After receiving messages from the socket, we publish peer messages to this
  /// queue for internal processing.
  caf::flow::async::publishing_queue_ptr<peer_message> peer_messages_;

  /// Points to the manager for this application.
  caf::net::socket_manager* mgr_ = nullptr;
};

} // namespace broker::detail

namespace broker {

class neo_endpoint {
public:
  explicit neo_endpoint(caf::actor_system_config& cfg)
    : this_endpoint_(uuid::random()), sys_(cfg) {
    // nop
  }

  void init() {
    using namespace detail;
    using caf::flow::async::publisher_from;
    auto serializer_out
      = publisher_from<detail::serializer_actor>(sys_, [&](auto* self) {
          pub_connector_ = caf::make_counted<publisher_connector_impl>(self);
          return self->state.merger->as_publisher();
        });
    auto ctrl_out = serializer_out->subscribe_with<controller_actor>(
      sys_,
      [&](auto* self, auto&& in) {
        ctrl_connector_ = caf::make_counted<controller_connector_impl>(self);
        return self->state.init(in);
      },
      this_endpoint_);
    ctrl_out->subscribe_with<deserializer_actor>(
      sys_, [&](auto* self, auto&& in) {
        auto&& [data_pub, cmd_pub] = self->state.init(in);
        data_pub->subscribe([](const data_message&) {
          // TODO: add instrumentation (metrics)
        });
        cmd_pub->subscribe([](const command_message&) {
          // TODO: add instrumentation (metrics)
        });
        data_messages_ = self->to_async_publisher(std::move(data_pub));
        command_messages_ = self->to_async_publisher(std::move(cmd_pub));
      });
  }

  auto id() {
    return this_endpoint_;
  }

  auto& system() {
    return sys_;
  }

  auto& config() {
    return sys_.config();
  }

  const auto& pub_connector() {
    return pub_connector_;
  }

  const auto& ctrl_connector() {
    return ctrl_connector_;
  }

  const auto& data_messages() {
    return data_messages_;
  }

  const auto& command_messages() {
    return command_messages_;
  }

private:
  uuid this_endpoint_;
  caf::actor_system sys_;
  detail::publisher_connector_ptr pub_connector_;
  detail::controller_connector_ptr ctrl_connector_;
  caf::flow::async::publisher_ptr<data_message> data_messages_;
  caf::flow::async::publisher_ptr<command_message> command_messages_;
};

} // namespace broker

int server_mode(neo_endpoint& ep, const std::string& iface, int port) {
  // Open port for incoming peerings.
  caf::net::tcp_accept_socket sock;
  auto auth = caf::uri::authority_type{};
  auth.host = iface;
  auth.port = static_cast<uint16_t>(port);
  if (auto sres = caf::net::make_tcp_accept_socket(std::move(auth)); !sres) {
    std::cerr << "*** unable to open port " << port << ": "
              << to_string(sres.error()) << '\n';
    return EXIT_FAILURE;
  } else {
    sock = *sres;
  }
  std::cout << "*** started listening for incoming connections on port "
            << caf::net::local_port(sock) << '\n';
  // Configure streaming pipeline.
  auto this_peer = ep.id();
  auto connector = ep.ctrl_connector();
  auto add_conn = [this_peer, connector](caf::net::tcp_stream_socket sock,
                                         caf::net::multiplexer* mpx) {
    std::cout << "*** got a new peer connection\n";
    using namespace caf::net;
    return make_socket_manager<broker::detail::app,
                               caf::net::length_prefix_framing,
                               caf::net::stream_transport>(
      sock, mpx, this_peer, connector, broker::detail::responder_tag{});
  };
  ep.system().network_manager().make_acceptor(sock, add_conn);
  using clock = std::chrono::steady_clock;
  ep.data_messages()->for_each(
    [n{0}, t{clock::now()}](const data_message& msg) mutable {
//puts("GOT ONE");
      ++n;
      auto tn = clock::now();
      if (tn - t > std::chrono::seconds(1)) {
        std::cout << n << " events/s\n";
        t = tn;
        n = 0;
      }
    });
  return EXIT_SUCCESS;
}

class bench_src : public caf::flow::buffered_publisher<broker::data_message> {
public:
  using super = caf::flow::buffered_publisher<broker::data_message>;

  using super::super;

  void pull(size_t n) override {
    for (size_t i = 0; i < n; ++i)
      append_to_buf(make_data_message("/foo/bar", createEventArgs()));
  }
};

int client_mode(neo_endpoint& ep, const std::string& host, int port) {
  caf::net::tcp_stream_socket sock;
  auto auth = caf::uri::authority_type{};
  auth.host = host;
  auth.port = static_cast<uint16_t>(port);
  if (auto sres = caf::net::make_connected_tcp_stream_socket(auth); !sres) {
    std::cerr << "*** unable to connect to " << host << " on port " << port
              << '\n';
    return EXIT_FAILURE;
  } else {
    sock = *sres;
  }
  auto mpx = ep.system().network_manager().mpx_ptr();
  auto this_peer = ep.id();
  auto connector = ep.ctrl_connector();
  auto mgr = caf::net::make_socket_manager<broker::detail::app,
                                           caf::net::length_prefix_framing,
                                           caf::net::stream_transport>(
    sock, mpx, this_peer, connector, broker::detail::originator_tag{});
  if (auto err = mgr->init(content(ep.config()))) {
    std::cerr << "*** failed to initialize the client: " << to_string(err)
              << '\n';
    return EXIT_FAILURE;
  }
  auto src = caf::flow::async::publisher_from(
    ep.system(), [](auto* self) { return caf::make_counted<bench_src>(self); });
  ep.pub_connector()->connect(src);
  return EXIT_SUCCESS;
}

int test_mode(caf::actor_system_config& cfg) {
  using namespace detail;
  using caf::flow::async::publisher_from;
  caf::actor_system sys_{cfg};
  detail::publisher_connector_ptr pub_connector_;
  caf::flow::async::publisher_ptr<data_message> data_messages_;
  caf::flow::async::publisher_ptr<command_message> command_messages_;
  auto serializer_out
    = publisher_from<detail::serializer_actor>(sys_, [&](auto* self) {
        pub_connector_ = caf::make_counted<publisher_connector_impl>(self);
        return self->state.merger->as_publisher();
      });
  auto src = caf::flow::async::publisher_from(
    sys_, [](auto* self) { return caf::make_counted<bench_src>(self); });
  pub_connector_->connect(src);
   auto stk = serializer_out->subscribe_with(
     sys_, [](auto* self, auto in) { return caf::flow::merge(std::move(in)); });
  //auto stk = src->subscribe_with(
  //  sys_, [](auto* self, auto in) { return std::move(in); });
  // serializer_out->for_each(
  //   [n{0}, t{clock::now()}](const packed_message& msg) mutable {
  stk->for_each(
    [n{0}, t{clock::now()}](const packed_message& msg) mutable {
      //printf("GOT ONE\n");
      ++n;
      auto tn = clock::now();
      if (tn - t > std::chrono::seconds(1)) {
        std::cout << n << " events/s\n";
        t = tn;
        n = 0;
      }
    });

  // serializer_out->subscribe_with<deserializer_actor>(
  //   sys_, [&](auto* self, auto&& in) {
  //     auto&& [data_pub, cmd_pub] = self->state.init(in);
  //     data_messages_ = self->to_async_publisher(std::move(data_pub));
  //     command_messages_ = self->to_async_publisher(std::move(cmd_pub));
  //   });
  // auto src = caf::flow::async::publisher_from(
  //   sys_, [](auto* self) { return caf::make_counted<bench_src>(self); });
  // pub_connector_->connect(src);
  // data_messages_->for_each(
  //   [n{0}, t{clock::now()}](const data_message& msg) mutable {
  //     printf("GOT ONE: %s\n", to_string(msg).c_str());
  //     ++n;
  //     auto tn = clock::now();
  //     if (tn - t > std::chrono::seconds(1)) {
  //       std::cout << n << " events/s\n";
  //       t = tn;
  //       n = 0;
  //     }
  //   });
  return EXIT_SUCCESS;
}

struct config : configuration {
  using super = configuration;

  config() : configuration(skip_init) {
    opt_group{custom_options_, "global"}
      .add(event_type, "event-type,t",
           "1 (vector, default) | 2 (conn log entry) | 3 (table)")
      .add(batch_rate, "batch-rate,r",
           "batches/sec (default: 1, set to 0 for infinite)")
      .add(batch_size, "batch-size,s", "events per batch (default: 1)")
      .add(rate_increase_interval, "batch-size-increase-interval,i",
           "interval for increasing the batch size (in seconds)")
      .add(rate_increase_amount, "batch-size-increase-amount,a",
           "additional batch size per interval")
      .add(max_received, "max-received,m", "stop benchmark after given count")
      .add(max_in_flight, "max-in-flight,f", "report when exceeding this count")
      .add(server, "server", "run in server mode")
      .add(verbose, "verbose", "enable status output");
  }

  using super::init;

  std::string help_text() const {
    return custom_options_.help_text();
  }
};

void usage(const config& cfg, const char* cmd_name) {
  std::cerr << "Usage: " << cmd_name
            << " [<options>] <zeek-host>[:<port>] | [--disable-ssl] --server "
               "<interface>:port\n\n"
            << cfg.help_text();
}

int main(int argc, char** argv) {
  caf::init_global_meta_objects<caf::id_block::neo_broker_bench>();
  caf::init_global_meta_objects<caf::id_block::broker>();
  caf::net::middleman::init_global_meta_objects();
  caf::io::middleman::init_global_meta_objects();
  caf::core::init_global_meta_objects();
  config cfg;
  cfg.load<caf::net::middleman>();
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << ex.what() << "\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  if (cfg.remainder.size() != 1) {
    std::cerr << "*** too many arguments\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  // Local variables configurable via CLI.
  auto arg = cfg.remainder[0];
  auto separator = arg.find(':');
  if (separator == std::string::npos) {
    std::cerr << "*** invalid argument\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  std::string host = arg.substr(0, separator);
  uint16_t port = 9999;
  try {
    auto str_port = arg.substr(separator + 1);
    if (!str_port.empty()) {
      auto int_port = std::stoi(str_port);
      if (int_port < 0 || int_port > std::numeric_limits<uint16_t>::max())
        throw std::out_of_range("not an uint16_t");
      port = static_cast<uint16_t>(int_port);
    }
  } catch (std::exception& e) {
    std::cerr << "*** invalid port: " << e.what() << "\n\n";
    usage(cfg, argv[0]);
    return EXIT_FAILURE;
  }
  //test_mode(cfg);
  // Run benchmark.
  neo_endpoint ep{cfg};
  ep.init();
  if (server)
    return server_mode(ep, host, port);
  else
    return client_mode(ep, host, port);
}
