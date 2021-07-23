#include "broker/alm/stream_transport.hh"

#include <caf/async/publishing_queue.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/net/length_prefix_framing.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/socket_manager.hpp>
#include <caf/net/stream_transport.hpp>
#include <caf/scheduled_actor/flow.hpp>

#include "broker/detail/flow_controller_callback.hh"
#include "broker/detail/overload.hh"
#include "broker/detail/protocol.hh"

namespace broker::alm {

// -- constructors, destructors, and assignment operators ----------------------

stream_transport::stream_transport(caf::event_based_actor* self)
  : super(self), reserved_(std::string{topic::reserved}) {
  // nop
}

stream_transport::stream_transport(caf::event_based_actor* self,
                                   detail::connector_ptr conn)
  : super(self), reserved_(std::string{topic::reserved}) {
  if (conn) {
    auto f = [this](endpoint_id remote_id, alm::lamport_timestamp ts,
                    const filter_type& filter, caf::net::stream_socket fd) {
      std::ignore = init_new_peer(remote_id, ts, filter, fd);
    };
    connector_adapter_.reset(
      new detail::connector_adapter(self, std::move(conn), f, filter_));
  }
}

// -- overrides for peer::publish ----------------------------------------------

namespace {

template <class BufferedObservable, class Msg>
void push_impl(caf::scheduled_actor* self, BufferedObservable& impl,
               const Msg& what) {
  impl.append_to_buf(what);
  impl.try_push();
}

} // namespace

void stream_transport::publish(endpoint_id dst, atom::subscribe,
                               const endpoint_id_list& path,
                               const vector_timestamp& ts,
                               const filter_type& new_filter) {
  BROKER_TRACE(BROKER_ARG(dst)
               << BROKER_ARG(path) << BROKER_ARG(ts) << BROKER_ARG(new_filter));
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(path)         //
                             && sink.apply(ts)        //
                             && sink.apply(new_filter);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::routing_update, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  push_impl(self(), *central_merge_,
            node_message{alm::multipath{dst, true}, std::move(pm)});
}

void stream_transport::publish(endpoint_id dst, atom::revoke,
                               const endpoint_id_list& path,
                               const vector_timestamp& ts,
                               const endpoint_id& lost_peer,
                               const filter_type& new_filter) {
  BROKER_TRACE(BROKER_ARG(dst)
               << BROKER_ARG(path) << BROKER_ARG(ts) << BROKER_ARG(lost_peer)
               << BROKER_ARG(new_filter));
  buf_.clear();
  caf::binary_serializer sink{nullptr, buf_};
  [[maybe_unused]] bool ok = sink.apply(path)         //
                             && sink.apply(ts)        //
                             && sink.apply(lost_peer) //
                             && sink.apply(new_filter);
  BROKER_ASSERT(ok);
  auto data = reinterpret_cast<const std::byte*>(buf_.data());
  auto pm = packed_message{packed_message_type::path_revocation, reserved_,
                           std::vector<std::byte>{data, data + buf_.size()}};
  push_impl(self(), *central_merge_,
            node_message{alm::multipath{dst, true}, std::move(pm)});
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

void stream_transport::dispatch(const node_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  // TODO: implement me
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
  auto cancel_inputs = [](auto&... ptr) { (ptr->cancel_inputs(), ...); };
  cancel_inputs(data_inputs_, command_inputs_);
  connector_adapter_.reset();
  for (auto& kvp : peers_)
    kvp.second.first.dispose();
}

// -- overrides for flow_controller --------------------------------------------

void stream_transport::init_data_outputs() {
  if (!data_outputs_) {
    data_outputs_ //
      = central_merge_->as_observable()
          .filter([this_node{id_}](const node_message& msg) {
            const auto& next_hop = get_path(msg).head();
            return next_hop.id() == this_node //
                   && next_hop.is_receiver()  //
                   && get_type(msg) == packed_message_type::data;
          })
          .map([](const node_message& msg) {
            caf::binary_deserializer src{nullptr, get_payload(msg)};
            data content;
            std::ignore = src.apply(content);
            return data_message{get_topic(msg), std::move(content)};
          })
          .as_observable();
  }
}

void stream_transport::init_command_outputs() {
  if (!command_outputs_) {
    command_outputs_ //
      = central_merge_->as_observable()
          .filter([this_node{id_}](const node_message& msg) {
            const auto& next_hop = get_path(msg).head();
            return next_hop.id() == this_node //
                   && next_hop.is_receiver()  //
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

caf::scheduled_actor* stream_transport::ctx() {
  return self_;
}

void stream_transport::add_source(caf::flow::observable<data_message> source) {
  data_inputs_->add(std::move(source));
}

void stream_transport::add_source(
  caf::flow::observable<command_message> source) {
  command_inputs_->add(std::move(source));
}

void stream_transport::add_sink(caf::flow::observer<data_message> sink) {
  init_data_outputs();
  data_outputs_.attach(sink);
}

void stream_transport::add_sink(caf::flow::observer<command_message> sink) {
  init_command_outputs();
  command_outputs_.attach(sink);
}

caf::async::publisher<data_message>
stream_transport::select_local_data(const filter_type& filter) {
  init_data_outputs();
  auto out = data_outputs_
               .filter([filter](const data_message& item) {
                 detail::prefix_matcher f;
                 return f(filter, item);
               })
               .as_observable();
  return self_->to_async_publisher(out);
}

caf::async::publisher<command_message>
stream_transport::select_local_commands(const filter_type& filter) {
  init_command_outputs();
  auto out = command_outputs_
               .filter([filter](const command_message& item) {
                 detail::prefix_matcher f;
                 return f(filter, item);
               })
               .as_observable();
  return self_->to_async_publisher(out);
}

void stream_transport::add_filter(const filter_type& filter) {
  subscribe(filter);
}

// -- initialization -----------------------------------------------------------

namespace {

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
    auto& path = get_path(msg);
    auto& content = caf::get<1>(msg);
    if (path.head().id() != state_->id()) {
      BROKER_ERROR("received a message for another node: drop");
      return true;
    }
    if (path.head().is_receiver()) {
      if (!next.on_next(msg, steps...))
        return false;
    }
    return path.for_each_node_while([&](multipath node) {
      auto sub = make_node_message(std::move(node), content);
      return next.on_next(sub, steps...);
    });
  }

  template <class Next, class... Steps>
  void on_complete(Next& next, Steps&... steps) {
    BROKER_DEBUG("peer" << src_ << "completed its flow");
    caf::error default_reason;
    state_->peer_disconnected(src_, default_reason);
    next.on_complete(steps...);
  }

  template <class Next, class... Steps>
  void on_error(const error& what, Next& next, Steps&... steps) {
    BROKER_DEBUG("peer" << src_ << "aborted its flow:" << what);
    state_->peer_disconnected(src_, what);
    next.on_error(what, steps...);
  }

private:
  stream_transport* state_;
  endpoint_id src_;
  std::vector<endpoint_id> receivers_cache_;
  std::vector<multipath> routes_cache_;
  std::vector<endpoint_id> unreachables_cache_;
};

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
      if (matches(filter, dst))
        receivers_cache_.emplace_back(peer);
    BROKER_DEBUG("got" << receivers_cache_.size() << "receiver for" << msg);
    if (!receivers_cache_.empty()) {
      // Clear caches.
      routes_cache_.clear();
      unreachables_cache_.clear();
      buf_.clear();
      // Compute paths.
      alm::multipath::generate(receivers_cache_, state_->tbl(), routes_cache_,
                               unreachables_cache_);
      if (routes_cache_.empty())
        return true;
      // Serialize payload.
      caf::binary_serializer sink{nullptr, buf_};
      std::ignore = sink.apply(get<1>(msg));
      auto first = reinterpret_cast<std::byte*>(buf_.data());
      auto last = first + buf_.size();
      auto payload = std::vector<std::byte>(first, last);
      auto packed = packed_message{packed_message_type_v<T>, dst,
                                   std::move(payload)};
      // Emit packed messages.
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
  caf::byte_buffer buf_;
};

} // namespace

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
  // Consume routing updates for this peer from the central merge node.
  central_merge_ //
    ->as_observable()
    .for_each([this](const node_message& msg) {
      if (get_path(msg).head().id() != id()
          || !get_path(msg).head().is_receiver())
        return;
      auto pmt = get_type(msg);
      if (pmt == packed_message_type::routing_update) {
        endpoint_id_list path;
        vector_timestamp ts;
        filter_type new_filter;
        caf::binary_deserializer src{nullptr, get_payload(msg)};
        bool ok = src.apply(path)  //
                  && src.apply(ts) //
                  && src.apply(new_filter);
        if (ok) {
          handle_filter_update(path, ts, new_filter);
        } else {
          BROKER_ERROR("received malformed routing update:" << src.get_error());
        }
      } else if (pmt == packed_message_type::path_revocation) {
        endpoint_id_list path;
        vector_timestamp ts;
        endpoint_id lost_peer;
        filter_type new_filter;
        caf::binary_deserializer src{nullptr, get_payload(msg)};
        bool ok = src.apply(path)         //
                  && src.apply(ts)        //
                  && src.apply(lost_peer) //
                  && src.apply(new_filter);
        if (ok) {
          handle_path_revocation(path, ts, lost_peer, new_filter);
        } else {
          BROKER_ERROR(
            "received malformed path revocation:" << src.get_error());
        }
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
      // // Special handlers for bypassing streams and/or forwarding.
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
        if (!connector_adapter_) {
          rp.deliver(make_error(ec::no_connector_available));
          return;
        }
        connector_adapter_->async_connect(
          addr,
          [this, rp](endpoint_id peer, alm::lamport_timestamp ts,
                     const filter_type& filter,
                     caf::net::stream_socket fd) mutable {
            if (auto err = init_new_peer(peer, ts, filter, fd))
              rp.deliver(std::move(err));
            else
              rp.deliver(atom::peer_v, atom::ok_v, peer);
          },
          [this, rp](const caf::error& what) mutable {
            rp.deliver(std::move(what));
          });
      },
      [=](atom::unpeer, const network_info&) {
        // TODO: implement me
      },
      [=](atom::unpeer, const network_info&) {
        // TODO: implement me
      },
      [this](atom::unpeer, const endpoint_id& peer_id) {
        // TODO: implement me
      },
      [this](const detail::flow_controller_callback_ptr& f) {
        (*f)(this);
      })
    .or_else(super::make_behavior());
}

// -- utility ------------------------------------------------------------------

caf::error stream_transport::init_new_peer(endpoint_id peer,
                                           alm::lamport_timestamp ts,
                                           const filter_type& filter,
                                           connect_flows_fun connect_flows) {
  BROKER_TRACE(BROKER_ARG(peer) << BROKER_ARG(ts) << BROKER_ARG(filter));
  if (peers_.count(peer) != 0)
    return make_error(ec::repeated_peering_handshake_request);
  endpoint_id_list path{peer};
  auto& sys = self_->system();
  auto out = central_merge_ //
               ->as_observable()
               .filter([peer](const node_message& msg) {
                 return get_path(msg).head().id() == peer;
               })
               .as_observable();
  auto async_out = self_->to_async_publisher(out);
  auto in = self_->observe(connect_flows(std::move(async_out)));
  central_merge_->add(in.transform(dispatch_step{this, peer}));
  peers_.emplace(peer, std::make_pair(in.as_disposable(), out.as_disposable()));
  auto update_res = handle_update(path, vector_timestamp{ts}, filter);
  if (auto& new_peers = update_res.first; !new_peers.empty())
    for (auto& id : new_peers)
      peer_discovered(id);
  peer_connected(peer);
  flood_subscriptions();
  return caf::none;
}

caf::error stream_transport::init_new_peer(endpoint_id peer,
                                           alm::lamport_timestamp ts,
                                           const filter_type& filter,
                                           caf::net::stream_socket sock) {
  BROKER_TRACE(BROKER_ARG(peer)
               << BROKER_ARG(ts) << BROKER_ARG(filter) << BROKER_ARG(sock));
  using caf::async::make_publishing_queue;
  using caf::net::make_socket_manager;
  if (peers_.count(peer) != 0) {
    caf::net::close(sock);
    return make_error(ec::repeated_peering_handshake_request);
  }
  auto& sys = self_->system();
  auto mpx = sys.network_manager().mpx_ptr();
  auto mgr
    = make_socket_manager<detail::protocol, caf::net::length_prefix_framing,
                          caf::net::stream_transport>(sock, mpx, id());
  connect_flows_fun fn = [mgr](node_message_publisher out) {
    return mgr->top_layer().connect_flows(mgr.get(), std::move(out));
  };
  if (auto err = init_new_peer(peer, ts, filter, std::move(fn))) {
    return err;
  } else if (err = mgr->init(content(sys.config())); err) {
    BROKER_ERROR("failed to initialize a peering connection:" << err);
    if (auto i = peers_.find(peer); i != peers_.end()) {
      auto& [in, out] = i->second;
      in.dispose();
      out.dispose();
      peers_.erase(i);
    }
    return err;
  } else {
    BROKER_DEBUG("successfully added peer" << peer << "on socket" << sock);
    return caf::none;
  }
}

void stream_transport::unpeer(const endpoint_id& peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  // TODO: implement me
}

} // namespace broker::alm
