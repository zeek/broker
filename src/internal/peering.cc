#include "broker/internal/peering.hh"

#include "broker/data.hh"
#include "broker/internal/killswitch.hh"
#include "broker/internal/type_id.hh"
#include "broker/topic.hh"

#include <caf/binary_serializer.hpp>
#include <caf/scheduled_actor/flow.hpp>

namespace broker::internal {

namespace {

// ASCII sequence 'BYE' followed by our 64-bit bye ID.
constexpr size_t bye_token_size = 11;

class affix_generator {
public:
  using output_type = node_message;

  affix_generator(peering_ptr ptr) : ptr_(std::move(ptr)) {}

  template <class Step, class... Steps>
  void pull(size_t n, Step& step, Steps&... steps) {
    while (n > 0) {
      switch (emitted_) {
        case 0: {
          if (!step.on_next(first(), steps...))
            return;
          emitted_ = 1;
          break;
        }
        case 1: {
          if (!step.on_next(second(), steps...))
            return;
          emitted_ = 2;
          ptr_ = nullptr;
          break;
        }
        default: {
          step.on_complete(steps...);
          return;
        }
      }
      --n;
    }
  }

  template <class Info, sc S>
  node_message make_status_msg(Info&& ep, sc_constant<S> code,
                               const char* msg) const {
    auto val = status::make(code, std::forward<Info>(ep), msg);
    auto content = get_as<data>(val);
    caf::byte_buffer buf;
    caf::binary_serializer snk{nullptr, buf};
    std::ignore = snk.apply(content);
    // TODO: this conversion is going to become superfluous with CAF 0.19.
    auto first = reinterpret_cast<std::byte*>(buf.data());
    std::vector<std::byte> bytes{first, first + buf.size()};
    auto pmsg = make_packed_message(packed_message_type::data, defaults::ttl,
                                    topic{std::string{topic::statuses_str}},
                                    std::move(bytes));
    return make_node_message(ptr_->id(), ptr_->id(), std::move(pmsg));
  }

  virtual node_message first() = 0;

  virtual node_message second() = 0;

protected:
  peering_ptr ptr_;

private:
  size_t emitted_ = 0;
};

class prefix_generator : public affix_generator {
public:
  using super = affix_generator;

  using super::super;

  node_message first() override {
    return make_status_msg(endpoint_info{ptr_->peer_id()},
                           sc_constant<sc::endpoint_discovered>(),
                           "found a new peer in the network");
  }

  node_message second() override {
    return make_status_msg(endpoint_info{ptr_->peer_id(), ptr_->addr()},
                           sc_constant<sc::peer_added>(),
                           "handshake successful");
  }
};

class suffix_generator : public affix_generator {
public:
  using super = affix_generator;

  using super::super;

  node_message first() override {
    if (ptr_->removed()) {
      return make_status_msg(endpoint_info{ptr_->peer_id(), ptr_->addr()},
                             sc_constant<sc::peer_removed>(),
                             "removed connection to remote peer");
    } else {
      return make_status_msg(endpoint_info{ptr_->peer_id(), ptr_->addr()},
                             sc_constant<sc::peer_lost>(),
                             "lost connection to remote peer");
    }
  }

  node_message second() override {
    return make_status_msg(endpoint_info{ptr_->peer_id()},
                           sc_constant<sc::endpoint_unreachable>(),
                           "lost the last path");
  }
};

} // namespace

void peering::on_bye_ack() {
  in_.dispose();
  out_.dispose();
  bye_timeout_.dispose();
}

void peering::force_disconnect() {
  assert(removed_);
  on_bye_ack();
}

void peering::schedule_bye_timeout(caf::scheduled_actor* self) {
  bye_timeout_.dispose();
  bye_timeout_ =
    self->run_delayed(defaults::unpeer_timeout,
                      [ptr = shared_from_this()] { ptr->force_disconnect(); });
}

std::vector<std::byte> peering::make_bye_token() {
  std::vector<std::byte> result;
  result.resize(bye_token_size);
  const auto* prefix = "BYE";
  const auto* suffix = &bye_id_;
  memcpy(result.data(), prefix, 3);
  memcpy(result.data() + 3, suffix, 8);
  return result;
}

node_message peering::make_bye_message() {
  auto packed = make_packed_message(packed_message_type::ping, defaults::ttl,
                                    topic{std::string{topic::reserved}},
                                    make_bye_token());
  return make_node_message(id_, peer_id_, std::move(packed));
}

caf::flow::observable<node_message>
peering::setup(caf::scheduled_actor* self, node_consumer_res in_res,
               node_producer_res out_res,
               caf::flow::observable<node_message> src) {
  // Construct the BYE message that we emit at the end.
  bye_id_ = self->new_u64_id();
  auto bye_packed_msg = make_packed_message(packed_message_type::ping, //
                                            defaults::ttl,
                                            topic{std::string{topic::reserved}},
                                            make_bye_token());
  auto bye_msg = make_node_message(id_, peer_id_, std::move(bye_packed_msg));
  // Inject our kill switch to allow us to cancel this peering later on.
  src //
    .compose(add_flow_scope_t{output_stats_})
    .compose(inject_killswitch_t{&out_})
    .subscribe(std::move(out_res));
  // Read inputs and surround them with connect/disconnect status messages.
  return self //
    ->make_observable()
    .from_generator(prefix_generator{shared_from_this()})
    .concat( //
      self->make_observable()
        .from_resource(std::move(in_res))
        .on_error_complete()
        .compose(add_flow_scope_t{input_stats_})
        .compose(inject_killswitch_t{&in_})
        .do_on_next([ptr = shared_from_this(), token = make_bye_token()](
                      const node_message& msg) mutable {
          // When unpeering, we send a BYE ping message. When
          // receiving the corresponding pong message, we can safely
          // discard the input (this flow).
          if (!ptr || get_type(msg) != packed_message_type::pong)
            return;
          if (auto& payload = get_payload(msg); payload == token) {
            ptr->on_bye_ack();
            ptr = nullptr;
          }
        }),
      self //
        ->make_observable()
        .from_generator(suffix_generator{shared_from_this()}));
}

void peering::remove(caf::scheduled_actor* self,
                     caf::flow::item_publisher<node_message>& snk,
                     bool with_timeout) {
  if (removed_)
    return;
  // Tag as about-to-be-removed and schedule a timeout.
  removed_ = true;
  if (with_timeout)
    schedule_bye_timeout(self);
  // TODO: ideally, we would guarantee that Broker sends all pending messages to
  //       the peer before shutting down the connection. By pushing to
  //       `unsafe_inputs`, we only have this guarantee for messages published
  //       via asynchronous message. That's how Zeek publishes it data at the
  //       moment, but it means that we can still cut off the connection before
  //       currently buffered messages on other sources were shipped. We do have
  //       the BYE handshake at the end and close the connection only after
  //       seeing the ACK, so pending data may still "slip by" after the BYE.
  //       That's not reliable, though.
  snk.push(make_bye_message());
}

bool peering::is_subscribed_to(const topic& what) const {
  detail::prefix_matcher f;
  return f(*filter_, what);
}

} // namespace broker::internal
