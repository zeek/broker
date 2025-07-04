#include "broker/internal/peering.hh"

#include "broker/data.hh"
#include "broker/data_envelope.hh"
#include "broker/format/bin.hh"
#include "broker/internal/killswitch.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"
#include "broker/ping_envelope.hh"
#include "broker/topic.hh"

#include <caf/scheduled_actor/flow.hpp>

using namespace std::literals;

namespace broker::internal {

namespace {

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
    return make_data_message(ptr_->id(), ptr_->id(),
                             topic{std::string{topic::statuses_str}}, content);
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
      auto msg = "removed connection to remote peer"s;
      if (const auto& reason = ptr_->removed_reason(); !reason.empty()) {
        msg += " (";
        msg += reason;
        msg += ')';
      }
      return make_status_msg(endpoint_info{ptr_->peer_id(), ptr_->addr()},
                             sc_constant<sc::peer_removed>(), msg.c_str());
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

void peering::force_disconnect(std::string reason) {
  if (!removed_) {
    removed_ = true;
    removed_reason_ = std::move(reason);
  }
  on_bye_ack();
}

void peering::schedule_bye_timeout(caf::scheduled_actor* self) {
  bye_timeout_.dispose();
  bye_timeout_ =
    self->run_delayed(defaults::unpeer_timeout, [ptr = shared_from_this()] {
      ptr->force_disconnect("timeout during graceful disconnect");
    });
}

void peering::assign_bye_token(std::array<std::byte, bye_token_size>& buf) {
  const auto* prefix = "BYE";
  const auto* suffix = &bye_id_;
  memcpy(buf.data(), prefix, 3);
  memcpy(buf.data() + 3, suffix, 8);
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
  std::array<std::byte, bye_token_size> token;
  assign_bye_token(token);
  return make_ping_message(id_, peer_id_, token.data(), token.size());
}

std::pair<caf::flow::observable<node_message>,
          caf::flow::observable<node_message>>
peering::setup(caf::scheduled_actor* self,
               caf::flow::observable<node_message> in,
               caf::flow::observable<node_message> src) {
  // Construct the BYE message that we emit at the end.
  bye_id_ = self->new_u64_id();
  auto bye_msg = make_bye_message();
  // Inject our kill switch to allow us to cancel this peering later on.
  auto out = src //
               .compose(add_flow_scope_t{output_stats_})
               .compose(inject_killswitch_t{&out_});
  // Read inputs and surround them with connect/disconnect status messages.
  auto new_in =
    self //
      ->make_observable()
      .from_generator(prefix_generator{shared_from_this()})
      .concat( //
        in.on_error_complete()
          .compose(add_flow_scope_t{input_stats_})
          .compose(inject_killswitch_t{&in_})
          .do_on_next([ptr = shared_from_this(), token = make_bye_token()](
                        const node_message& msg) mutable {
            // When unpeering, we send a BYE ping message. When
            // receiving the corresponding pong message, we can safely
            // discard the input (this flow).
            if (!ptr || get_type(msg) != packed_message_type::pong)
              return;
            if (auto [payload_bytes, payload_size] = msg->raw_bytes();
                std::equal(payload_bytes, payload_bytes + payload_size,
                           token.begin(), token.end())) {
              log::core::debug("final-pong-received",
                               "received final PONG message during unpeering");
              ptr->on_bye_ack();
              ptr = nullptr;
            }
          }),
        self //
          ->make_observable()
          .from_generator(suffix_generator{shared_from_this()}));
  return {new_in.as_observable(), out.as_observable()};
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
