#include "broker/alm/stream_transport.hh"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/net/length_prefix_framing.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/socket_manager.hpp>
#include <caf/net/stream_transport.hpp>
#include <caf/scheduled_actor/flow.hpp>

#include "broker/detail/native_socket.hh"
#include "broker/detail/overload.hh"
#include "broker/detail/protocol.hh"
#include "broker/logger.hh"

namespace broker::alm {

template <class T>
packed_message stream_transport::pack(const T& msg) {
  const auto& dst = get_topic(msg);
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  std::ignore = sink.apply(get<1>(msg));
  auto first = reinterpret_cast<std::byte*>(buf_.data());
  auto last = first + buf_.size();
  auto payload = std::vector<std::byte>(first, last);
  return packed_message{packed_message_type_v<T>, dst, std::move(payload)};
}

// -- constructors, destructors, and assignment operators ----------------------

namespace {

auto random_seed() {
  std::random_device dev;
  return dev();
}

// Minimum delay before flooding a path discovery.
constexpr int min_path_discovery_delay_ms = 7;

// Maximum delay before flooding a path discovery.
constexpr int max_path_discovery_delay_ms = 17;

// Extra delay for triggering filter requests. Ideally, path discovery always
// triggers first in order to avoid 'dead' messages.
constexpr int filter_request_extray_delay_ms = 10;

} // namespace

stream_transport::stream_transport(caf::event_based_actor* self)
  : super(self),
    reserved_(std::string{topic::reserved}),
    rng_(random_seed()),
    dis_(min_path_discovery_delay_ms, max_path_discovery_delay_ms) {
  // nop
}

stream_transport::stream_transport(caf::event_based_actor* self,
                                   detail::connector_ptr conn)
  : stream_transport(self) {
  if (conn) {
    auto on_peering = [this](endpoint_id remote_id, const network_info& addr,
                             alm::lamport_timestamp ts,
                             const filter_type& filter,
                             detail::native_socket fd) {
      std::ignore = init_new_peer(remote_id, addr, ts, filter, fd);
    };
    auto on_peer_unavailable = [this](const network_info& addr) {
      peer_unavailable(addr);
    };
    connector_adapter_.reset(new detail::connector_adapter(
      self, std::move(conn), on_peering, on_peer_unavailable, filter_,
      peer_statuses_));
  }
}

// -- properties -------------------------------------------------------------

const network_info* stream_transport::addr_of(endpoint_id id) const noexcept {
  if (auto i = peers_.find(id); i != peers_.end())
    return std::addressof(i->second.addr);
  return nullptr;
}

// -- overrides for peer::publish ----------------------------------------------

namespace {

template <class BufferedObservable, class Msg>
void push_impl(caf::scheduled_actor*, BufferedObservable& impl,
               const Msg& what) {
  impl.append_to_buf(what);
  impl.try_push();
}

} // namespace

void stream_transport::publish_filter_update_impl(multipath* routes,
                                                  size_t num_routes,
                                                  lamport_timestamp ts,
                                                  const filter_type& ft) {
  BROKER_TRACE(BROKER_ARG(ts) << BROKER_ARG(ft));
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(id_)   //
                             && sink.apply(ts) //
                             && sink.apply(ft);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::filter_update, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  for (size_t index = 0; index < num_routes; ++index)
    push_impl(self(), *central_merge_,
              node_message{std::move(routes[index]), pm});
}

void stream_transport::publish_filter_update(lamport_timestamp ts,
                                             const filter_type& new_filter) {
  if (tbl_.empty())
    return;
  std::vector<endpoint_id> all;
  all.reserve(tbl_.size());
  for (auto& kvp : tbl_)
    all.emplace_back(kvp.first);
  std::vector<multipath> routes;
  std::vector<endpoint_id> unreachables;
  multipath::generate(all, tbl_, routes, unreachables);
  if (!routes.empty()) {
    publish_filter_update_impl(routes.data(), routes.size(), ts, new_filter);
  } else {
    BROKER_ERROR("failed to generate any routes!");
  }
  if (!unreachables.empty())
    BROKER_ERROR("found unreachable entries in the routing table!");
}

void stream_transport::publish_filter_update(endpoint_id dst,
                                             lamport_timestamp ts,
                                             const filter_type& new_filter) {
  BROKER_TRACE(BROKER_ARG(dst) << BROKER_ARG(ts) << BROKER_ARG(new_filter));
  if (auto sp = shortest_path(tbl_, dst)) {
    auto route = alm::multipath{sp->begin(), sp->end()};
    publish_filter_update_impl(&route, 1, ts, new_filter);
  }
}

void stream_transport::trigger_path_discovery_cb() {
  BROKER_TRACE("");
  // TODO: we don't actually need to construct these. It would suffice to
  // serialize id_ and timestamp_ to appear as lists of one element.
  endpoint_id_list path{id_};
  vector_timestamp ts{timestamp_};
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(path) && sink.apply(ts);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::path_discovery, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  for_each_direct(tbl_, [&](auto receiver) {
    if (receiver)
      push_impl(self(), *central_merge_,
                node_message{multipath{receiver, true}, pm});
    else
      BROKER_ERROR("BROKEN ROUTING TABLE: NIL ENTRY FOUND");
  });
  scheduled_path_discovery_ = caf::disposable{};
}

void stream_transport::trigger_path_discovery() {
  BROKER_TRACE("");
  // Instead of firing the path discovery immediately, we delay it by some
  // random amount of time. This minimizes the number of path discovery messages
  // on the wire by sending fewer messages overall while also spreading them out
  // in time in order to avoid 'path discovery storms'.
  if (scheduled_path_discovery_) {
    // We have already scheduled a path discovery, do nothing.
    return;
  }
  auto f = [this] { trigger_path_discovery_cb(); };
  auto delay = std::chrono::milliseconds{dis_(rng_)};
  scheduled_path_discovery_ = self()->run_delayed(delay, f);
}

void stream_transport::forward_path_discovery(endpoint_id next_hop,
                                              const endpoint_id_list& path,
                                              const vector_timestamp& ts) {
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(path) && sink.apply(ts);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::path_discovery, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  push_impl(self(), *central_merge_,
            node_message{multipath{next_hop, true}, std::move(pm)});
}

void stream_transport::trigger_path_revocation(const endpoint_id& lost_peer) {
  BROKER_TRACE(BROKER_ARG(lost_peer));
  // TODO: we don't actually need to construct these. It would suffice to
  // serialize id_ and timestamp_ to appear as lists of one element.
  endpoint_id_list path{id_};
  vector_timestamp ts{timestamp_};
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(path)  //
                             && sink.apply(ts) //
                             && sink.apply(lost_peer);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::path_revocation, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  for_each_direct(tbl_, [&](auto receiver) {
    push_impl(self(), *central_merge_,
              node_message{multipath{receiver, true}, pm});
  });
}

void stream_transport::forward_path_revocation(endpoint_id next_hop,
                                               const endpoint_id_list& path,
                                               const vector_timestamp& ts,
                                               const endpoint_id& lost_peer) {
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(path) && sink.apply(ts);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::path_discovery, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  push_impl(self(), *central_merge_,
            node_message{multipath{next_hop, true}, std::move(pm)});
}

void stream_transport::trigger_filter_requests_cb() {
  BROKER_TRACE(BROKER_ARG(hidden_peers_));
  if (hidden_peers_.empty()) {
    // Nothing left to do.
    scheduled_filter_requests_ = caf::disposable{};
    return;
  }
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(id_);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::filter_request, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  std::vector<multipath> routes;
  std::vector<endpoint_id> unreachables;
  multipath::generate(hidden_peers_, tbl_, routes, unreachables);
  for (auto& route : routes)
    push_impl(self(), *central_merge_, node_message{std::move(route), pm});
  for (auto& unreachable : unreachables) {
    auto& v = hidden_peers_;
    v.erase(std::remove(v.begin(), v.end(), unreachable), v.end());
  }
  scheduled_filter_requests_ = caf::disposable{};
  trigger_filter_requests();
}

void stream_transport::trigger_filter_requests() {
  BROKER_TRACE(BROKER_ARG(hidden_peers_));
  // Same approach we use in trigger_path_discovery() to minimize load on the
  // network.
  if (scheduled_filter_requests_ || hidden_peers_.empty()) {
    // We have already scheduled filter requests or there is nothing to do.
    return;
  }
  auto f = [this] { trigger_filter_requests_cb(); };
  using std::chrono::milliseconds;
  auto delay = milliseconds{dis_(rng_) + filter_request_extray_delay_ms};
  scheduled_path_discovery_ = self()->run_delayed(delay, f);
}

#ifdef NDEBUG
#  define OBSERVABLE_CAST(var) dynamic_cast<buffered_t&>(var)
#else
#  define OBSERVABLE_CAST(var) static_cast<buffered_t&>(var)
#endif

void stream_transport::publish_locally(const data_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  using buffered_t = caf::flow::buffered_observable_impl<data_message>;
  if (data_outputs_)
    push_impl(self(), OBSERVABLE_CAST(*data_outputs_.ptr()), msg);
}

void stream_transport::publish_locally(const command_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  using buffered_t = caf::flow::buffered_observable_impl<command_message>;
  if (command_outputs_)
    push_impl(self(), OBSERVABLE_CAST(*command_outputs_.ptr()), msg);
}

void stream_transport::dispatch(const data_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  push_impl(self(), *data_inputs_, msg);
}

void stream_transport::dispatch(const command_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  push_impl(self(), *command_inputs_, msg);
}

void stream_transport::dispatch(alm::multipath path, const data_message& msg) {
  auto packed = pack(msg);
  push_impl(self(), *central_merge_,
            node_message{std::move(path), std::move(packed)});
}

void stream_transport::dispatch(alm::multipath path,
                                const command_message& msg) {
  auto packed = pack(msg);
  push_impl(self(), *central_merge_,
            node_message{std::move(path), std::move(packed)});
}

// -- overrides for alm::peer --------------------------------------------------

void stream_transport::shutdown(shutdown_options options) {
  BROKER_TRACE(BROKER_ARG(options));
  super::shutdown(options);
  // Allow mergers to shut down.
  auto shutdown_on_last_complete = [](auto&... ptr) {
    (ptr->shutdown_on_last_complete(true), ...);
  };
  shutdown_on_last_complete(data_inputs_, command_inputs_, central_merge_);
  // Cancel all incoming flows.
  BROKER_DEBUG("cancel" << subscriptions_.size() << "subscriptions");
  for (auto& sub : subscriptions_)
    sub.dispose();
  subscriptions_.clear();
}

// -- utility ------------------------------------------------------------------

void stream_transport::init_data_outputs() {
  if (!data_outputs_) {
    BROKER_DEBUG("create data outputs");
    data_outputs_
      = central_merge_->as_observable()
          .filter([this_node{id_}](const node_message& msg) {
            const auto& hop = get_path(msg).head();
            return hop.id() == this_node //
                   && hop.is_receiver()  //
                   && get_type(msg) == packed_message_type::data;
          })
          .map([](const node_message& msg) {
            caf::binary_deserializer src{nullptr, get_payload(msg)};
            data content;
            std::ignore = src.apply(content);
            auto result = data_message{get_topic(msg), std::move(content)};
            BROKER_DEBUG("dispatch data message locally:" << result);
            return result;
          })
          .as_observable();
  }
}

void stream_transport::init_command_outputs() {
  if (!command_outputs_) {
    BROKER_DEBUG("create command outputs");
    command_outputs_ //
      = central_merge_->as_observable()
          .filter([this_node{id_}](const node_message& msg) {
            const auto& hop = get_path(msg).head();
            return hop.id() == this_node //
                   && hop.is_receiver()  //
                   && get_type(msg) == packed_message_type::command;
          })
          .map([](const node_message& msg) {
            caf::binary_deserializer src{nullptr, get_payload(msg)};
            internal_command content;
            std::ignore = src.apply(content);
            return command_message{get_topic(msg), std::move(content)};
          })
          .as_observable();
  }
}

// void stream_transport::add_source(caf::flow::observable<data_message> source) {
//   data_inputs_->add(std::move(source));
// }
//
// void stream_transport::add_source(
//   caf::flow::observable<command_message> source) {
//   command_inputs_->add(std::move(source));
// }
//
// void stream_transport::add_sink(caf::flow::observer<data_message> sink) {
//   init_data_outputs();
//   data_outputs_.attach(sink);
// }
//
// void stream_transport::add_sink(caf::flow::observer<command_message> sink) {
//   init_command_outputs();
//   command_outputs_.attach(sink);
// }

caf::flow::observable<command_message>
stream_transport::select_local_commands(const filter_type& filter) {
  init_command_outputs();
  return command_outputs_
    .filter([filter](const command_message& item) {
      detail::prefix_matcher f;
      return f(filter, item);
    })
    .as_observable();
}

// -- initialization -----------------------------------------------------------

// Feeds input messages from a peer into the central merge.
class dispatch_step {
public:
  using input_type = node_message;

  using output_type = node_message;

  explicit dispatch_step(stream_transport* state, endpoint_id src) noexcept
    : state_(state), src_(src) {
    // nop
  }

  template <class Next, class... Steps>
  bool on_next(const input_type& msg, Next& next, Steps&... steps) {
    auto& [path, content] = msg.data();
    if (path.head().id() != state_->id()) {
      BROKER_ERROR(state_->id() <<
                   "received a" << get_type(content) << "message to"
                   << path.head().id() << "from" << src_ << "-> drop");
      return true;
    }
    return next.on_next(msg, steps...);
  }

  template <class Next, class... Steps>
  void on_complete(Next& next, Steps&... steps) {
    BROKER_DEBUG("peer" << src_ << "completed its flow");
    auto& peers = state_->peers_;
    if (auto i = peers.find(src_); i != peers.end() && !i->second.invalidated) {
      i->second.invalidated = true;
      update_status();
      caf::error default_reason;
      auto& addr = i->second.addr;
      if (addr.retry.count() > 0)
        caf::anon_send(state_->self(), atom::peer_v, addr);
      state_->peer_disconnected(src_, default_reason);
    }
    next.on_complete(steps...);
  }

  template <class Next, class... Steps>
  void on_error(const error& what, Next& next, Steps&... steps) {
    BROKER_DEBUG("peer" << src_ << "aborted its flow:" << what);
    auto& peers = state_->peers_;
    if (auto i = peers.find(src_); i != peers.end() && !i->second.invalidated) {
      i->second.invalidated = true;
      update_status();
      auto& addr = i->second.addr;
      if (addr.retry.count() > 0)
        caf::anon_send(state_->self(), atom::peer_v, addr);
      state_->peer_disconnected(src_, what);
    }
    next.on_error(what, steps...);
  }

private:
  void update_status() {
    auto status = peer_status::peered;
    if (state_->peer_statuses_->update(src_, status,
                                       peer_status::disconnected)) {
      BROKER_DEBUG(src_ << ":: peered -> disconnected");
    } else {
      BROKER_ERROR("invalid status for peer" << BROKER_ARG2("peer", src_)
                                             << BROKER_ARG(status)
                                             << "(expected status: peered)");
      state_->peer_statuses_->update(src_, status, peer_status::disconnected);
    }
  }

  stream_transport* state_;
  endpoint_id src_;
  std::vector<endpoint_id> receivers_cache_;
  std::vector<multipath> routes_cache_;
  std::vector<endpoint_id> unreachables_cache_;
};

// Converts a data or command message to a node message to feed locally produced
// messages into the central merge point.
template <class T>
class pack_and_dispatch_step {
public:
  using input_type = T;

  using output_type = node_message;

  explicit pack_and_dispatch_step(stream_transport* state) noexcept
    : state_(state) {
    // nop
  }

  pack_and_dispatch_step(const pack_and_dispatch_step& other) noexcept
    : state_(other.state_) {
    // nop
  }

  template <class Next, class... Steps>
  bool on_next(const T& msg, Next& next, Steps&... steps) {
    receivers_cache_.clear();
    const auto& dst = get_topic(msg);
    detail::prefix_matcher matches;
    for (const auto& [peer, filter] : state_->peer_filters())
      if (matches(filter.entry, dst))
        receivers_cache_.emplace_back(peer);
    BROKER_DEBUG("got" << receivers_cache_.size() << "receiver for" << msg);
    if (!receivers_cache_.empty()) {
      // Clear caches.
      routes_cache_.clear();
      unreachables_cache_.clear();
      alm::multipath::generate(receivers_cache_, state_->tbl(), routes_cache_,
                               unreachables_cache_);
      if (routes_cache_.empty())
        return true;
      auto packed = state_->pack(msg);
      for (auto& route : routes_cache_) {
        auto nmsg = node_message{std::move(route), packed};
        if (!next.on_next(nmsg, steps...))
          return false;
      }
      if (!unreachables_cache_.empty())
        BROKER_WARNING("cannot ship message: no path to any of"
                       << unreachables_cache_);
    }
    return true;
  }

  template <class Next, class... Steps>
  void on_complete(Next& next, Steps&... steps) {
    next.on_complete(steps...);
  }

  template <class Next, class... Steps>
  void on_error(const error& what, Next& next, Steps&... steps) {
    next.on_error(what, steps...);
  }

private:
  stream_transport* state_;
  std::vector<endpoint_id> receivers_cache_;
  std::vector<multipath> routes_cache_;
  std::vector<endpoint_id> unreachables_cache_;
};

caf::behavior stream_transport::make_behavior() {
  // Create the merge nodes.
  auto init_merger = [this](auto& ptr) {
    ptr.emplace(self_);
    ptr->delay_error(true);
    ptr->shutdown_on_last_complete(false);
  };
  init_merger(data_inputs_);
  init_merger(command_inputs_);
  init_merger(central_merge_);
  // Connect incoming data messages to the central merge node.
  central_merge_->add(data_inputs_ //
                        ->as_observable()
                        .transform(pack_and_dispatch_step<data_message>{this}));
  // Connect incoming command messages to the central merge node.
  central_merge_->add(command_inputs_ //
                        ->as_observable()
                        .transform(pack_and_dispatch_step<command_message>{this}));
  // Consume routing updates for this peer from the central merge node and add
  // instrumentation.
  auto fam = self()->system().metrics().counter_family(
    "broker", "processed-elements", {"type"},
    "Number of processed stream elements.");
  using counter_ptr = caf::telemetry::int_counter*;
  // Indexes in this array are values of packed_message_type that start at 1.
  std::array<counter_ptr, 7> counters{{
    nullptr,
    fam->get_or_add({{"type", "data"}}),
    fam->get_or_add({{"type", "command"}}),
    fam->get_or_add({{"type", "filter-request"}}),
    fam->get_or_add({{"type", "filter-update"}}),
    fam->get_or_add({{"type", "path-discovery"}}),
    fam->get_or_add({{"type", "path-revocation"}}),
  }};
  central_merge_ //
    ->as_observable()
    .for_each([this, counters](const node_message& msg) {
      auto msg_type = get_type(msg);
      counters[static_cast<size_t>(msg_type)]->inc();
      const auto& hop = get_path(msg).head();
      if (hop.id() != id() || !hop.is_receiver())
        return;
      switch (msg_type) {
        case packed_message_type::filter_request: {
          endpoint_id node;
          caf::binary_deserializer src{nullptr, get_payload(msg)};
          if (src.apply(node)) {
            handle_filter_request(node);
          } else {
            BROKER_ERROR(
              "received malformed filter request:" << src.get_error());
          }
          break;
        }
        case packed_message_type::filter_update: {
          endpoint_id node;
          lamport_timestamp ts;
          filter_type new_filter;
          caf::binary_deserializer src{nullptr, get_payload(msg)};
          if (src.apply(node) && src.apply(ts) && src.apply(new_filter)) {
            handle_filter_update(node, ts, new_filter);
          } else {
            BROKER_ERROR(
              "received malformed filter update:" << src.get_error());
          }
          break;
        }
        case packed_message_type::path_discovery: {
          endpoint_id_list path;
          vector_timestamp ts;
          caf::binary_deserializer src{nullptr, get_payload(msg)};
          if (src.apply(path) && src.apply(ts)) {
            handle_path_discovery(path, ts);
          } else {
            BROKER_ERROR(
              "received malformed path discovery:" << src.get_error());
          }
          break;
        }
        case packed_message_type::path_revocation: {
          endpoint_id_list path;
          vector_timestamp ts;
          endpoint_id lost_peer;
          caf::binary_deserializer src{nullptr, get_payload(msg)};
          if (src.apply(path) && src.apply(ts) && src.apply(lost_peer)) {
            handle_path_revocation(path, ts, lost_peer);
          } else {
            BROKER_ERROR(
              "received malformed path revocation:" << src.get_error());
          }
          break;
        }
        default:
          break;
      }
    });
  // Create behavior for the actor.
  using caf::net::socket_id;
  using detail::lift;
  caf::message_handler base;
  if (connector_adapter_)
    base = connector_adapter_->message_handlers();
  return base
    .or_else(
      [this](atom::publish, atom::local, data_message& msg) {
        publish_locally(msg);
      },
      [=](atom::listen, const std::string& addr, uint16_t port) {
        auto rp = self_->make_response_promise();
        if (!connector_adapter_) {
          rp.deliver(make_error(ec::no_connector_available));
          return;
        }
        connector_adapter_->async_listen(
          addr, port,
          [rp](uint16_t actual_port) mutable {
            rp.deliver(atom::listen_v, atom::ok_v, actual_port);
          },
          [rp](const caf::error& what) mutable { rp.deliver(what); });
      },
      [=](atom::peer, const network_info& addr) {
        auto rp = self_->make_response_promise();
        try_connect(addr, rp);
        return rp;
      },
      [this](atom::unpeer, const network_info& peer_addr) {
        unpeer(peer_addr);
      },
      [this](atom::unpeer, endpoint_id peer_id) { //
        unpeer(peer_id);
      },
      [this](atom::peer, endpoint_id peer, const network_info& addr,
             alm::lamport_timestamp ts, const filter_type& filter,
             detail::node_consumer_res in_res,
             detail::node_producer_res out_res) -> caf::result<void> {
        if (auto err = init_new_peer(peer, addr, ts, filter, in_res, out_res))
          return err;
        else
          return caf::unit;
      },
      [this](filter_type& filter, detail::data_producer_res snk) {
        subscribe(filter);
        init_data_outputs();
        data_outputs_
          .filter([filter{std::move(filter)}](const data_message& item) {
            detail::prefix_matcher f;
            return f(filter, item);
          })
          .subscribe(std::move(snk));
      },
      [this](std::shared_ptr<filter_type> fptr, detail::data_producer_res snk) {
        // Here, we accept a shared_ptr to the filter instead of an actual
        // object. This allows the subscriber to manipulate the filter later via
        // actions (to make sure there are is unsynchronized read/write on the
        // filter).
        subscribe(*fptr);
        init_data_outputs();
        data_outputs_
          .filter([fptr{std::move(fptr)}](const data_message& item) {
            detail::prefix_matcher f;
            return f(*fptr, item);
          })
          .subscribe(std::move(snk));
      },
      [this](std::shared_ptr<filter_type>& fptr, topic& x, bool add,
             std::shared_ptr<std::promise<void>>& sync) {
        // Here, we assume that the filter is bound to some output flow.
        auto e = fptr->end();
        auto i = std::find(fptr->begin(), e, x);
        if (add) {
          if (i == e) {
            fptr->emplace_back(std::move(x));
            subscribe(*fptr);
          }
        } else {
          if (i != e)
            fptr->erase(i);
        }
        if (sync)
          sync->set_value();
      },
      [this](detail::data_consumer_res src) {
        auto sub
          = data_inputs_->add(self()->make_observable().from_resource(src));
        subscriptions_.emplace_back(std::move(sub));
      },
      [this](atom::join, filter_type& filter) mutable {
        auto hdl = caf::actor_cast<caf::actor>(self()->current_sender());
        if (!hdl)
          return;
        subscribe(filter);
        init_data_outputs();
        auto sub = data_outputs_
                     .filter([filter](const data_message& item) {
                       detail::prefix_matcher f;
                       return f(filter, item);
                     })
                     .for_each([this, hdl](const data_message& msg) {
                       self()->send(hdl, msg);
                     });
        auto weak_self = self()->address();
        hdl->attach_functor([weak_self, sub]() mutable {
          auto strong_self = caf::actor_cast<caf::actor>(weak_self);
          if (!strong_self)
            return;
          auto cb = caf::make_action([sub]() mutable { sub.dispose(); });
          caf::anon_send(strong_self, std::move(cb));
        });
      })
    .or_else(super::make_behavior());
}

caf::error stream_transport::init_new_peer(endpoint_id peer,
                                           const network_info& addr,
                                           alm::lamport_timestamp ts,
                                           const filter_type& filter,
                                           detail::node_consumer_res in_res,
                                           detail::node_producer_res out_res) {
  BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(ts) << BROKER_ARG(filter));
  auto i = peers_.find(peer);
  if (i != peers_.end() && !i->second.invalidated)
    return make_error(ec::repeated_peering_handshake_request);
  auto& psm = *peer_statuses_;
  auto status = peer_status::peered;
  if (psm.insert(peer, status)) {
    BROKER_DEBUG(peer << ":: () -> peered");
  } else if (status == peer_status::connected
             && psm.update(peer, status, peer_status::peered)) {
    BROKER_DEBUG(peer << ":: connected -> peered");
  } else {
    BROKER_ERROR("invalid status for new peer" << BROKER_ARG(peer)
                                               << BROKER_ARG(status));
    return make_error(ec::invalid_status, to_string(status));
  }
  endpoint_id_list path{peer};
  auto& sys = self_->system();
  auto out = central_merge_->as_observable()
               .filter([peer](const node_message& msg) {
                 auto& msg_path = get_path(msg);
                 return msg_path.head().id() == peer
                        || msg_path.is_next_hop(peer);
               })
               .map([peer](const node_message& msg) {
                 auto& [msg_path, pm] = msg.data();
                 if (msg_path.head().id() == peer)
                   return msg;
                 else
                   return node_message{msg_path.select(peer), pm};
               })
               .subscribe(out_res);
  auto in = self_->make_observable().from_resource(in_res);
  central_merge_->add(in.transform(dispatch_step{this, peer}).as_observable());
  subscriptions_.emplace_back(in.as_disposable());
  if (i == peers_.end()) {
    peers_.emplace(peer, peer_state{std::move(in).as_disposable(),
                                    std::move(out), addr});
  } else {
    i->second.in = std::move(in).as_disposable();
    i->second.out = std::move(out);
    i->second.invalidated = false;
  }
  add_or_update_path(tbl_, peer, path, vector_timestamp{ts});
  handle_filter_update(peer, ts, filter);
  peer_connected(peer);
  trigger_path_discovery();
  return caf::none;
}

caf::error stream_transport::init_new_peer(endpoint_id peer,
                                           const network_info& addr,
                                           alm::lamport_timestamp ts,
                                           const filter_type& filter,
                                           detail::native_socket fd) {
  namespace cn = caf::net;
  BROKER_TRACE(BROKER_ARG(peer)
               << BROKER_ARG(ts) << BROKER_ARG(filter) << BROKER_ARG(fd));
  using caf::async::make_bounded_buffer_resource;
  using cn::make_socket_manager;
  auto sock = cn::stream_socket{fd};
  if (peers_.count(peer) != 0) {
    BROKER_DEBUG("already connected to" << peer);
    cn::close(sock);
    return make_error(ec::repeated_peering_handshake_request);
  }
  auto [con1, prod1] = make_bounded_buffer_resource<node_message>();
  auto [con2, prod2] = make_bounded_buffer_resource<node_message>();
  if (auto err = init_new_peer(peer, addr, ts, filter, con1, prod2)) {
    BROKER_ERROR("failed to initialize peering state" << BROKER_ARG(peer)
                                                      << BROKER_ARG(err));
    cn::close(sock);
    return err;
  }
  auto& sys = self_->system();
  auto mpx = sys.network_manager().mpx_ptr();
  auto mgr = make_socket_manager<detail::protocol, cn::length_prefix_framing,
                                 cn::stream_transport>(sock, mpx, id());
  mgr->top_layer().connect_flows(mgr.get(), con2, prod1);
  if (auto err = mgr->init(content(sys.config())); err) {
    BROKER_ERROR("failed to initialize a peering connection:" << err);
    if (auto i = peers_.find(peer); i != peers_.end()) {
      auto& st = i->second;
      st.in.dispose();
      st.out.dispose();
      peers_.erase(i);
    }
    return err;
  } else {
    BROKER_DEBUG("successfully added peer" << peer << "on socket" << sock.id);
    return caf::none;
  }
}

void stream_transport::unpeer(const endpoint_id& peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  if (auto i = peers_.find(peer_id); i != peers_.end())
    unpeer(i);
  else
    cannot_remove_peer(peer_id);
}

void stream_transport::unpeer(const network_info& peer_addr) {
  BROKER_TRACE(BROKER_ARG(peer_addr));
  if (auto i = find_peer(peer_addr); i != peers_.end())
    unpeer(i);
  else
    cannot_remove_peer(peer_addr);
}

void stream_transport::unpeer(peer_state_map::iterator i) {
  BROKER_ASSERT(i != peers_.end());
  auto& st = i->second;
  if (st.invalidated) {
    BROKER_DEBUG(i->first << "already invalidated");
    return;
  }
  BROKER_DEBUG("drop state for " << i->first);
  st.invalidated = true;
  st.in.dispose();
  st.out.dispose();
  auto id = i->first;
  peer_removed(id);
  peers_.erase(i);
  cleanup(id);
}

void stream_transport::try_connect(const network_info& addr,
                                   caf::response_promise rp) {
  BROKER_TRACE(BROKER_ARG(addr));
  if (!connector_adapter_) {
    rp.deliver(make_error(ec::no_connector_available));
    return;
  }
  connector_adapter_->async_connect(
    addr,
    [this, rp](endpoint_id peer, const network_info& addr,
               alm::lamport_timestamp ts, const filter_type& filter,
               detail::native_socket fd) mutable {
      BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(addr) << BROKER_ARG(ts)
                                    << BROKER_ARG(filter) << BROKER_ARG(fd));
      if (auto err = init_new_peer(peer, addr, ts, filter, fd);
          err && err != ec::repeated_peering_handshake_request)
        rp.deliver(std::move(err));
      else
        rp.deliver(atom::peer_v, atom::ok_v, peer);
    },
    [this, rp](endpoint_id peer, const network_info& addr) mutable {
      BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(addr));
      if (auto i = peers_.find(peer); i != peers_.end()) {
        // Override the address if this one has a retry field. This makes
        // sure we "prefer" a user-defined address over addresses we read
        // from sockets for incoming peerings.
        if (addr.has_retry_time() && !i->second.addr.has_retry_time())
          i->second.addr = addr;
        rp.deliver(atom::peer_v, atom::ok_v, peer);
      } else {
        // Race on the state. May happen if the remote peer already
        // started a handshake and the connector thus drops this request
        // as redundant. We should already have the connect event queued,
        // so we just enqueue this request again and the second time
        // we should find it in the cache.
        using namespace std::literals;
        self_->run_delayed(1ms, [this, peer, addr, rp]() mutable {
          BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(addr));
          if (auto i = peers_.find(peer); i != peers_.end()) {
            if (addr.has_retry_time() && !i->second.addr.has_retry_time())
              i->second.addr = addr;
            rp.deliver(atom::peer_v, atom::ok_v, peer);
          } else {
            try_connect(addr, rp);
          }
        });
      }
    },
    [this, rp, addr](const caf::error& what) mutable {
      BROKER_TRACE(BROKER_ARG(what));
      rp.deliver(std::move(what));
      peer_unavailable(addr);
    });
}

// -- utility ------------------------------------------------------------------

stream_transport::peer_state_map::iterator
stream_transport::find_peer(const network_info& addr) noexcept {
  auto has_addr = [&addr](const auto& kvp) { return kvp.second.addr == addr; };
  return std::find_if(peers_.begin(), peers_.end(), has_addr);
}

} // namespace broker::alm
