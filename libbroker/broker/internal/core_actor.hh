#pragma once

#include "broker/detail/prefix_matcher.hh"
#include "broker/endpoint.hh"
#include "broker/endpoint_id.hh"
#include "broker/fwd.hh"
#include "broker/internal/connector.hh"
#include "broker/internal/connector_adapter.hh"
#include "broker/internal/fwd.hh"
#include "broker/internal/subscription_multimap.hh"
#include "broker/internal/wire_format.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/overflow_policy.hh"

#include <caf/async/spsc_buffer.hpp>
#include <caf/callback.hpp>
#include <caf/chunk.hpp>
#include <caf/disposable.hpp>
#include <caf/flow/observable.hpp>
#include <caf/make_counted.hpp>
#include <caf/uuid.hpp>

#include <array>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace broker::internal {

/// Denotes the subtype of a handler.
enum class handler_type {
  store,
  master, // Subtype of store.
  clone,  // Subtype of store.
  client,
  peering,
  hub,
  subscriber, // Subtype of hub.
  publisher,  // Subtype of hub.
};

/// Trait for getting the value and ID type of a handler.
template <handler_type>
struct handler_type_trait;

template <>
struct handler_type_trait<handler_type::store> {
  using value_type = command_message;
  using id_type = std::string;
};

template <>
struct handler_type_trait<handler_type::client> {
  using value_type = data_message;
  using id_type = endpoint_id;
};

template <>
struct handler_type_trait<handler_type::peering> {
  using value_type = caf::chunk;
  using id_type = endpoint_id;
};

template <>
struct handler_type_trait<handler_type::hub> {
  using value_type = data_message;
  using id_type = hub_id;
};

/// Convenience alias for the value type of a handler.
template <handler_type T>
using handler_value_type = typename handler_type_trait<T>::value_type;

/// Convenience alias for the ID type of a handler.
template <handler_type T>
using handler_id_type = typename handler_type_trait<T>::id_type;

/// The state of the core actor.
class core_actor_state {
public:
  // -- member types -----------------------------------------------------------

  /// Bundles message-related metrics that have a label dimension for the type.
  struct message_metrics_t {
    /// Counts how many messages were processed since starting the core.
    prometheus::Counter* processed = nullptr;
  };

  /// Bundles metrics for the core.
  struct metrics_t {
    explicit metrics_t(prometheus::Registry& reg);

    /// Keeps track of how many native peers are currently connected.
    prometheus::Gauge* native_connections = nullptr;

    /// Keeps track of how many WebSocket clients are currently connected.
    prometheus::Gauge* web_socket_connections = nullptr;

    /// Stores the metrics for all message types.
    std::array<message_metrics_t, 5> message_metric_sets;

    const message_metrics_t& metrics_for(packed_message_type msg_type) const {
      // Note: the enum starts at 1. Hence, we subtract 1 to get a 0-based index
      // into our array.
      return message_metric_sets[static_cast<size_t>(msg_type) - 1];
    }
  };

  /// The result of invoking `offer` on a handler.
  enum class offer_result {
    /// The message was accepted by the handler.
    ok,
    /// The message was skipped (ignored) by the handler.
    skip,
    /// The handler hit the buffer limit and disconnected.
    overflow_disconnect,
    /// The handler shuts down and should not be called again.
    term,
  };

  /// The result of invoking a callback on a handler.
  enum class handler_result {
    /// The callback was invoked successfully.
    ok,
    /// The callback was invoked but resulted in a terminal state, i.e., the
    /// handler should not be called again.
    term,
  };

  /// Wraps access to a node message and also allows converting it to a chunk.
  /// This conversion is done lazily, i.e., only when the message is actually
  /// needed. Furthermore, if multiple handlers require the conversion, it is
  /// done only once and the result is cached.
  class message_provider {
  public:
    void set(node_message what) {
      msg_ = std::move(what);
      binary_ = caf::chunk{};
    }

    const node_message& get() const {
      return msg_;
    }

    const caf::chunk& as_binary();

    data_message as_data() {
      if (msg_->type() == envelope_type::data) {
        return msg_->as_data();
      }
      return nullptr;
    }

    command_message as_command() {
      if (msg_->type() == envelope_type::command) {
        return msg_->as_command();
      }
      return nullptr;
    }

  private:
    /// The current message.
    node_message msg_;

    /// Caches the serialized message.
    caf::chunk binary_;

    /// Buffer for serializing the message to a chunk. Re-used for multiple
    /// conversions to avoid allocating a new buffer for each conversion.
    caf::byte_buffer buffer_;
  };

  /// Bundles state for sending and receiving messages to/from a peer, hub,
  /// store, or client.
  class handler {
  public:
    explicit handler(core_actor_state* parent) : parent(parent) {}

    virtual ~handler();

    /// Called whenever dispatching a message that matches the handler's filter.
    [[nodiscard]] virtual offer_result offer(message_provider& msg) = 0;

    /// Callback for a downstream component demanding more messages.
    virtual void add_demand(size_t demand) = 0;

    /// Tries to pull more messages from the handler.
    [[nodiscard]] virtual handler_result
    pull(std::vector<node_message>& buf) = 0;

    /// Disposes of the handler. Must be called after the handler has neither
    /// input nor output buffers and has been removed from its container.
    virtual void dispose() = 0;

    virtual bool input_closed() const noexcept = 0;

    virtual bool output_closed() const noexcept = 0;

    /// The parent object.
    core_actor_state* parent;

    /// The type of the handler.
    virtual handler_type type() const noexcept = 0;

    /// The name of this handler in log output.
    std::string pretty_name;
  };

  template <class T>
  struct pull_observer {
    explicit pull_observer(std::vector<T>& storage) : buf(&storage) {
      // nop
    }

    void on_next(const T& item) {
      buf->emplace_back(item);
    }

    void on_complete() {
      completed = true;
    }

    void on_error(const caf::error&) {
      failed = true;
    }

    std::vector<T>* buf;

    bool completed = false;

    bool failed = false;
  };

  /// Base class for all handler types, implementing the common functionality.
  template <handler_type Subtype>
  class handler_base : public handler {
  public:
    using super = handler;

    using value_type = handler_value_type<Subtype>;

    using id_type = handler_id_type<Subtype>;

    using buffer_producer_ptr =
      caf::async::spsc_buffer_producer_ptr<value_type>;

    using buffer_consumer_ptr =
      caf::async::spsc_buffer_consumer_ptr<value_type>;

    handler_base(core_actor_state* parent, size_t max_buffer_size,
                 overflow_policy policy, id_type id)
      : super(parent),
        max_buffer_size(max_buffer_size),
        policy(policy),
        id(std::move(id)) {
      // nop
    }

    /// The consumer for reading messages from the shared buffer.
    buffer_consumer_ptr in;

    /// The producer for writing messages to the shared buffer.
    buffer_producer_ptr out;

    /// A buffer for messages that are not yet sent to the peer.
    std::deque<value_type> queue;

    /// The maximum number of messages that can be buffered.
    size_t max_buffer_size;

    overflow_policy policy;

    /// The number of messages that can be sent to the peer immediately.
    size_t demand = 0;

    /// The ID of this handler. Can be a peer ID or a randomly generated UUID.
    /// Only used for clients and peerings.
    id_type id;

    /// A callback to be called when the handler is removed.
    caf::unique_callback_ptr<void()> on_dispose;

    handler_type type() const noexcept override {
      return Subtype;
    }

    [[nodiscard]] offer_result offer(message_provider& msg) override {
      if constexpr (std::is_same_v<value_type, caf::chunk>) {
        if (auto& serialized = msg.as_binary()) {
          return do_offer(serialized);
        }
        // Note: the provider will log an error if the conversion fails.
      } else if constexpr (std::is_same_v<value_type, node_message>) {
        return do_offer(msg);
      } else if constexpr (std::is_same_v<value_type, data_message>) {
        if (auto dmsg = msg.as_data()) {
          return do_offer(std::move(dmsg));
        }
      } else {
        static_assert(std::is_same_v<value_type, command_message>);
        if (auto cmsg = msg.as_command()) {
          return do_offer(std::move(cmsg));
        }
      }
      // If we reach this point, the message is not of the expected type and we
      // simply ignore it.
      return offer_result::skip;
    }

    void add_demand(size_t new_demand) override {
      if (new_demand == 0) {
        return;
      }
      demand += new_demand;
      while (demand > 0 && !queue.empty()) {
        auto n = std::min(demand, queue.size());
        auto i = queue.begin();
        auto e = i + static_cast<std::ptrdiff_t>(n);
        std::vector<value_type> items{i, e};
        queue.erase(i, e);
        if constexpr (Subtype == handler_type::peering) {
          if (auto* lptr = logger()) {
            lptr->on_peer_buffer_pull(id, items.size());
          }
        } else if constexpr (Subtype == handler_type::client) {
          if (auto* lptr = logger()) {
            lptr->on_client_buffer_pull(id, items.size());
          }
        }
        out->push(std::move(items));
        demand -= n;
      }
    }

    void dispose() override {
      if (in) {
        in->dispose();
      }
      if (out) {
        out->dispose();
      }
      if (on_dispose) {
        (*on_dispose)();
        on_dispose = nullptr;
      }
    }

    bool input_closed() const noexcept override {
      return !in;
    }

    bool output_closed() const noexcept override {
      return !out;
    }

    [[nodiscard]] handler_result pull(std::vector<node_message>& buf) override {
      if (!in) {
        return handler_result::ok;
      }
      auto do_pull = [this](auto& observer) {
        auto [again, pulled] = in->pull(100, observer);
        while (again && pulled == 100) {
          std::tie(again, pulled) = in->pull(100, observer);
        }
        if (!again) {
          in = nullptr;
          return handler_result::term;
        }
        return handler_result::ok;
      };
      if constexpr (std::is_same_v<value_type, caf::chunk>) {
        std::vector<caf::chunk> chunks;
        chunks.reserve(128);
        pull_observer<caf::chunk> observer{chunks};
        auto result = do_pull(observer);
        for (auto& item : chunks) {
          wire_format::v1::trait trait;
          node_message converted;
          if (!trait.convert(item.bytes(), converted)) {
            log::core::error("pull",
                             "{} failed to convert chunk to node message",
                             pretty_name);
            in->dispose();
            in = nullptr;
            return handler_result::term;
          }
          buf.emplace_back(std::move(converted));
        }
        return result;
      } else {
        pull_observer observer{buf};
        return do_pull(observer);
      }
    }

  private:
    [[nodiscard]] offer_result do_offer(value_type msg) {
      // If output is closed, we can't send messages anymore
      if (!out) {
        return offer_result::term;
      }
      // If we have demand, we can send the message immediately.
      if (demand > 0) {
        out->push(std::move(msg));
        --demand;
        return offer_result::ok;
      }
      // As long as our queue is not full, we can store the message in our queue
      // until we have demand.
      if (queue.size() < max_buffer_size) {
        queue.push_back(std::move(msg));
        if constexpr (Subtype == handler_type::peering) {
          if (auto* lptr = logger()) {
            lptr->on_peer_buffer_push(id, 1);
          }
        } else if constexpr (Subtype == handler_type::client) {
          if (auto* lptr = logger()) {
            lptr->on_client_buffer_push(id, 1);
          }
        }
        return offer_result::ok;
      }
      // If the queue is full, we need to decide what to do with the message.
      switch (policy) {
        default: // overflow_policy::disconnect
          log::core::info("offer",
                          "{} hit the buffer limit with policy disconnect",
                          pretty_name);
          out->abort(caf::make_error(caf::sec::backpressure_overflow));
          out = nullptr;
          return offer_result::overflow_disconnect;
        case overflow_policy::drop_newest:
          queue.pop_back();
          queue.push_back(std::move(msg));
          return offer_result::ok;
        case overflow_policy::drop_oldest:
          queue.pop_front();
          queue.push_back(std::move(msg));
          return offer_result::ok;
      }
    }
  };

  using handler_ptr = std::shared_ptr<handler>;

  class store_handler : public handler_base<handler_type::store> {
  public:
    using super = handler_base<handler_type::store>;

    store_handler(core_actor_state* parent, size_t max_buffer_size,
                  overflow_policy policy, std::string id, caf::actor hdl,
                  handler_type subtype)
      : super(parent, max_buffer_size, policy, std::move(id)),
        hdl(std::move(hdl)),
        type_(subtype) {
      BROKER_ASSERT(subtype == handler_type::master
                    || subtype == handler_type::clone);
    }

    handler_type type() const noexcept override {
      return type_;
    }

    caf::actor hdl;

  private:
    handler_type type_;
  };

  using store_handler_ptr = std::shared_ptr<store_handler>;

  class client_handler : public handler_base<handler_type::client> {
  public:
    using super = handler_base<handler_type::client>;

    using super::super;
  };

  using client_handler_ptr = std::shared_ptr<client_handler>;

  class hub_handler : public handler_base<handler_type::hub> {
  public:
    using super = handler_base<handler_type::hub>;

    hub_handler(core_actor_state* parent, size_t max_buffer_size,
                overflow_policy policy, hub_id hid)
      : super(parent, max_buffer_size, policy, hid) {
      // nop
    }

    handler_type type() const noexcept override {
      return type_;
    }

    void type(handler_type subtype) noexcept {
      BROKER_ASSERT(subtype == handler_type::subscriber
                    || subtype == handler_type::publisher);
      type_ = subtype;
    }

  private:
    handler_type type_ = handler_type::hub;
  };

  using hub_handler_ptr = std::shared_ptr<hub_handler>;

  class peering_handler : public handler_base<handler_type::peering> {
  public:
    // ASCII sequence 'BYE' followed by our 64-bit bye ID.
    static constexpr size_t bye_token_size = 11;

    using super = handler_base<handler_type::peering>;

    using bye_token = std::array<std::byte, bye_token_size>;

    peering_handler(core_actor_state* parent, endpoint_id id,
                    size_t max_buffer_size, overflow_policy policy,
                    network_info addr, filter_type filter)
      : super(parent, max_buffer_size, policy, id),
        addr(std::move(addr)),
        filter(std::move(filter)) {
      // nop
    }

    [[nodiscard]] offer_result offer(message_provider& msg) override;

    /// Creates a BYE message.
    node_message make_bye_message();

    /// Assigns a BYE token to a buffer.
    void assign_bye_token(bye_token& buf);

    template <class Info, sc S>
    node_message make_status_msg(Info&& ep, sc_constant<S> code,
                                 const char* msg) {
      auto val = status::make(code, std::forward<Info>(ep), msg);
      auto content = get_as<data>(val);
      return make_data_message(parent->id, parent->id,
                               topic{std::string{topic::statuses_str}},
                               content);
    }

    bool is_subscribed_to(const topic& what) const {
      detail::prefix_matcher f;
      return f(filter, what);
    }

    /// Indicates whether we are unpeering from this node and have sent a BYE
    /// message to the peer. Once the BYE handshake completes, we will call
    /// `on_peer_removed`.
    bool unpeering = false;

    /// Network address as reported from the transport (usually TCP).
    network_info addr;

    /// Stores the subscriptions of the remote peer.
    filter_type filter;

    /// A 64-bit token that we use as ping payload when unpeering. The ping is
    /// the last message we send. When receiving a pong message with that token,
    /// we know all messages arrived and can shut down the connection.
    uint64_t bye_id = 0;
  };

  using peering_handler_ptr = std::shared_ptr<peering_handler>;

  /// Calls the given function with the handler cast to the appropriate type.
  template <class Fn>
  auto with_subtype(const handler_ptr& ptr, Fn&& fn) {
    switch (ptr->type()) {
      case handler_type::store:
        return fn(std::static_pointer_cast<store_handler>(ptr));
      case handler_type::client:
        return fn(std::static_pointer_cast<client_handler>(ptr));
      case handler_type::peering:
        return fn(std::static_pointer_cast<peering_handler>(ptr));
      case handler_type::hub:
      case handler_type::subscriber:
      case handler_type::publisher:
        return fn(std::static_pointer_cast<hub_handler>(ptr));
      default:
        throw std::logic_error("invalid handler type");
    }
  }

  // -- constants --------------------------------------------------------------

  static inline const char* name = "broker.core";

  // --- constructors and destructors ------------------------------------------

  core_actor_state(caf::event_based_actor* self,
                   prometheus_registry_ptr registry, endpoint_id this_peer,
                   filter_type initial_filter, endpoint::clock* clock = nullptr,
                   const domain_options* adaptation = nullptr,
                   connector_ptr conn = nullptr);

  ~core_actor_state();

  // -- initialization and tear down -------------------------------------------

  /// Creates the initial set of message handlers for `self`.
  caf::behavior make_behavior();

  /// Initiates an orderly shutdown.
  void shutdown(shutdown_options options);

  /// Cleans up all state.
  void finalize_shutdown();

  // -- convenience functions --------------------------------------------------

  /// Emits a status or error code.
  /// @private
  template <class Info, class EnumConstant>
  void emit(Info&& ep, EnumConstant code, const char* msg);

  /// Returns whether `x` has at least one remote subscriber.
  bool has_remote_subscriber(const topic& x) const noexcept;

  /// Checks whether the peer with the given `id` is subscribed to the topic
  /// `what`.
  bool is_subscribed_to(endpoint_id id, const topic& what);

  /// Returns the @ref network_info associated with the given `id` if available.
  std::optional<network_info> addr_of(endpoint_id id) const;

  /// Creates a snapshot for the current values of the message metrics.
  table message_metrics_snapshot() const;

  /// Creates a snapshot for the peering statistics.
  table peer_stats_snapshot() const;

  /// Creates a snapshot that summarizes the current status of the core.
  table status_snapshot() const;

  /// Sets up a handler by connecting its input and output buffers as well as
  /// adding its subscriptions to the handler subscriptions map.
  template <class T>
  void setup(const std::shared_ptr<T>& ptr,
             caf::async::consumer_resource<typename T::value_type> in_res,
             caf::async::producer_resource<typename T::value_type> out_res,
             const filter_type& filter) {
    // Call `on_data` whenever there is activity on the input buffer.
    if (in_res) {
      ptr->in = in_res.consume_on(self, [this, ptr](auto&) { on_data(ptr); });
    }
    // Call `on_demand` if the peer requests more messages and
    // `on_output_closed` when the peer closes the output buffer.
    if (out_res) {
      ptr->out = out_res.produce_on(
        self, [this, ptr](auto&, size_t demand) { on_demand(ptr, demand); },
        [this, ptr](auto&) {
          ptr->out = nullptr;
          on_output_closed(ptr);
        });
    }
    // Extend our filter with the new handler.
    for (auto& sub : filter) {
      handler_subscriptions.insert(sub.string(), ptr);
    }
  }

  /// Calls `offer` on the given handler with the message provider and handles
  /// the result, removing the handler if it disconnects or terminates.
  /// @returns `true` if the message was accepted by the handler, `false`
  /// otherwise.
  bool offer_msg(const handler_ptr& ptr);

  // -- callbacks --------------------------------------------------------------

  /// Called whenever the user tries to unpeer from an unknown peer.
  /// @param x Either a peer ID, an actor handle or a network info.
  void cannot_remove_peer(endpoint_id x);

  /// Called whenever the user tries to unpeer from an unknown peer.
  /// @param x Either a peer ID, an actor handle or a network info.
  void cannot_remove_peer(const network_info& x);

  /// Called whenever establishing a connection to a remote peer failed.
  /// @param x Either a peer ID or a network info.
  void peer_unavailable(const network_info& x);

  /// Called whenever a new client connects.
  void client_added(endpoint_id client_id, const network_info& addr,
                    const std::string& type);

  /// Called whenever a client disconnects.
  void client_removed(endpoint_id client_id, const network_info& addr,
                      const std::string& type, const caf::error& reason,
                      bool removed);

  /// Called whenever we remove a peer by demand of the user.
  void on_peer_removed(const peering_handler_ptr& ptr);

  /// Called whenever we lose connection to a peer.
  void on_peer_lost(const peering_handler_ptr& ptr);

  /// Called whenever a peer has demand for more messages.
  void on_demand(const handler_ptr& peer, size_t demand);

  /// Called whenever a peer signals that it enqueues new messages.
  void on_data(const handler_ptr& ptr);

  /// Called whenever a peer signals that it enqueues new messages.
  void on_data(const peering_handler_ptr& peer);

  /// Called when triggering an overflow and the handler is configured to
  /// disconnect.
  void on_overflow_disconnect(const handler_ptr& ptr);

  /// Called whenever a handler closes its input buffer.
  void on_input_closed(const handler_ptr& ptr);

  /// Called whenever a handler closes its output buffer.
  void on_output_closed(const handler_ptr& ptr);

  /// Called from `on_overflow_disconnect`, `on_input_closed`, or
  /// `on_output_closed` when a handler was disposed.
  void on_handler_disposed(const handler_ptr& ptr);

  /// Called to remove a handler from its container.
  void erase(const handler_ptr& peer);

  // -- connection management --------------------------------------------------

  /// Tries to asynchronously connect to addr via the connector.
  void try_connect(const network_info& addr, caf::response_promise rp);

  // -- flow management --------------------------------------------------------

  caf::error init_new_peer(endpoint_id peer, const network_info& addr,
                           const filter_type& filter, chunk_consumer_res in_res,
                           chunk_producer_res out_res);

  /// Spin up a new background worker managing the socket and then dispatch to
  /// `init_new_peer` with the buffers that connect to the worker.
  caf::error init_new_peer(endpoint_id peer, const network_info& addr,
                           const filter_type& filter,
                           const pending_connection_ptr& conn);

  /// Connects the input and output buffers for a new client to our central
  /// merge point.
  caf::error init_new_client(const network_info& addr, std::string& type,
                             const filter_type& filter,
                             data_consumer_res in_res,
                             data_producer_res out_res);

  // -- topic management -------------------------------------------------------

  /// Adds `what` to the local filter and also forwards the subscription to
  /// connected peers.
  void subscribe(const filter_type& what);

  // -- data store management --------------------------------------------------

  /// Returns whether a master for `name` probably already exists on one of our
  /// peers.
  bool has_remote_master(const std::string& name) const;

  /// Attaches a master for the given store to this peer.
  caf::result<caf::actor> attach_master(const std::string& name,
                                        backend backend_type,
                                        backend_options opts);

  /// Attaches a clone for the given store to this peer.
  caf::result<caf::actor> attach_clone(const std::string& name,
                                       double resync_interval,
                                       double stale_interval,
                                       double mutation_buffer_interval);

  /// Terminates all masters and clones by sending exit messages to the
  /// corresponding actors.
  void shutdown_stores();

  // -- dispatching of messages ------------------------------------------------

  /// Dispatches `msg` to all subscribers based on the message topic.
  void dispatch(const node_message& msg);

  /// Dispatches `msg` to all subscribers based on the message topic, except for
  /// `from`.
  void dispatch_from(const node_message& msg, const handler_ptr& from);

  /// Broadcasts the local subscriptions to all peers.
  void broadcast_subscriptions();

  // -- unpeering --------------------------------------------------------------

  /// Disconnects a peer by demand of the user.
  void unpeer(endpoint_id peer_id);

  /// Disconnects a peer by demand of the user.
  void unpeer(const network_info& peer_addr);

  // -- properties -------------------------------------------------------------

  size_t store_buffer_size();

  overflow_policy store_overflow_policy();

  size_t peer_buffer_size();

  overflow_policy peer_overflow_policy();

  size_t web_socket_buffer_size();

  overflow_policy web_socket_overflow_policy();

  size_t hub_buffer_size();

  overflow_policy hub_overflow_policy();

  /// Returns the handlers for all peerings.
  auto peering_handlers() const {
    std::vector<peering_handler_ptr> result;
    result.reserve(peerings.size());
    for (auto& kvp : peerings) {
      result.push_back(kvp.second);
    }
    return result;
  }

  bool send_bye_message(const peering_handler_ptr& ptr);

  // -- member variables -------------------------------------------------------

  /// Points to the actor itself.
  caf::event_based_actor* self;

  /// Identifies this peer in the network.
  endpoint_id id;

  /// Stores the subscriptions from our non-peer handlers.
  subscription_multimap<handler_ptr> handler_subscriptions;

  /// Reusable message provider.
  message_provider msg_provider;

  /// Stores all master actors created by this endpoint.
  std::unordered_map<std::string, store_handler_ptr> masters;

  /// Stores all clone actors created by this endpoint.
  std::unordered_map<std::string, store_handler_ptr> clones;

  /// Stores the state for all peerings.
  std::unordered_map<endpoint_id, peering_handler_ptr> peerings;

  /// Stores the state for all clients.
  std::unordered_map<endpoint_id, client_handler_ptr> clients;

  /// Stores the state for all hubs.
  std::unordered_map<hub_id, hub_handler_ptr> hubs;

  /// Reusable buffer for pulling messages from handlers.
  std::vector<node_message> pull_buffer;

  /// Reusable buffer for passing to `handler_subscriptions.select()`.
  std::vector<handler_ptr> selection_buffer;

  /// Stores whether this peer has disabled forwarding, i.e., only appears as a
  /// leaf node to other peers.
  bool disable_forwarding = false;

  /// Stores prefixes that have subscribers on this endpoint. This is shared
  /// with the connector, which needs access to the filter during handshake.
  shared_filter_ptr filter;

  /// Stores IDs of peers that we have no path to yet but some local actor is
  /// already waiting for. Usually for testing purposes.
  std::multimap<endpoint_id, caf::response_promise> awaited_peers;

  /// Enables manual time management by the user.
  endpoint::clock* clock;

  /// Stores a reference to the metrics registry.
  prometheus_registry_ptr registry;

  /// Caches pointers to the Broker metrics.
  metrics_t metrics;

  /// Handle to the background worker for establishing peering relations.
  std::unique_ptr<connector_adapter> adapter;

  /// Synchronizes information about the current status of a peering with the
  /// connector.
  detail::shared_peer_status_map_ptr peer_statuses =
    std::make_shared<detail::peer_status_map>();

  /// Time-to-live when sending messages.
  uint16_t ttl;

  /// When shutting down, this scheduled action forces disconnects on all peers
  /// after the timeout.
  caf::disposable shutting_down_timeout;

  /// Counts messages that were published directly via message publishing, i.e.,
  /// without using the back-pressure of flows.
  int64_t published_via_async_msg = 0;

  /// Stores whether `shutdown` was called.
  bool shutting_down_ = false;
};

using core_actor = caf::stateful_actor<core_actor_state>;

} // namespace broker::internal
