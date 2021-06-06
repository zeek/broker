#pragma once

#include <caf/uuid.hpp>

namespace broker::detail {

struct originator_tag_t{};

constexpr auto originator_tag = originator_tag_t{};

struct responder_tag_t{};

constexpr auto responder_tag = responder_tag_t{};

class protocol {
public:
  using input_tag = caf::tag::message_oriented;

  enum state_t {
    await_syn,
    await_syn_ack,
    await_ack,
    await_msg,
  };

  protocol(originator_tag_t, caf::uuid this_peer)
    : state_(await_syn_ack), this_peer_(this_peer) {
    // nop
  }

  protocol(responder_tag, caf::uuid this_peer)
    : state_(await_syn), this_peer_(this_peer) {
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
    adapter_->flush();
    if (reason == caf::sec::socket_disconnected
        || reason == caf::sec::discarded)
      adapter_->on_complete();
    else
      adapter_->on_error(reason);
  }

  template <class LowerLayerPtr>
  void after_reading(LowerLayerPtr) {
    adapter_->flush();
  }

  template <class LowerLayerPtr>
  ptrdiff_t consume(LowerLayerPtr down, caf::byte_span buf) {
    using bnm = broker_network_message_type;
    caf::binary_deserializer src{nullptr, buf};
    bnm tag;
    if (!src.apply(tag))
      return -1;
    switch (tag) {
      case bnm::data:
      case bnm::command:
        if (!handle_msg(down, src, static_cast<packed_message_type>(tag)))
          return -1;
        break;
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
    state_ = await_msg;
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
    state_ = await_msg;
    return initialize_flows(down);
  }

  template <class LowerLayerPtr>
  bool handle_msg(LowerLayerPtr down, caf::binary_deserializer& src,
                  packed_message_type tag) {
    if (state_ != await_msg) {
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
    auto bytes = std::as_bytes(src.remainder());
    auto first = reinterpret_cast<std::byte*>(bytes.begin());
    auto last = reinterpret_cast<std::byte*>(bytes.last());
    auto payload = std::vector<std::byte>{first, last};
    // Push data down the pipeline.
    auto msg = make_cow_tuple(tag, std::move(path), msg_topic,
                              std::move(payload));
    if (peer_messages_->push(std::move(msg)) == 0)
      down->suspend_reading();
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
  caf::uuid this_peer_;

  /// Points to the manager that runs this protocol stack.
  caf::net::socket_manager* mgr_ = nullptr;

  /// Forwards outgoing messages to the peer. We write whatever we receive from
  /// this channel to the socket.
  caf::net::subscriber_adapter_ptr<peer_message> controller_messages_;

  /// After receiving messages from the socket, we publish peer messages to this
  /// queue for processing by the controller.
  caf::net::publisher_adapter_ptr<peer_message> peer_messages_;
};

} // namespace broker::detail
