#pragma once

#include <caf/net/observer_adapter.hpp>
#include <caf/net/publisher_adapter.hpp>
#include <caf/tag/message_oriented.hpp>
#include <caf/uuid.hpp>

#include "broker/alm/multipath.hh"
#include "broker/message.hh"

namespace broker::detail {

class protocol {
public:
  using input_tag = caf::tag::message_oriented;

  explicit protocol(caf::uuid this_peer) : this_peer_(this_peer) {
    // nop
  }

  caf::async::publisher<node_message>
  connect_flows(caf::net::socket_manager* mgr,
                caf::async::publisher<node_message> in) {
    using caf::make_counted;
    // Connect outgoing items to the .
    using controller_adapter_type = caf::net::observer_adapter<node_message>;
    controller_messages_ = make_counted<controller_adapter_type>(mgr);
    in.subscribe(controller_messages_->as_observer());
    // Connect incoming traffic to the observer.
    using peer_adapter_type = caf::net::publisher_adapter<node_message>;
    peer_messages_ = make_counted<peer_adapter_type>(mgr, 64, 8);
    return peer_messages_->as_publisher();
  }

  template <class LowerLayerPtr>
  caf::error
  init(caf::net::socket_manager* mgr, LowerLayerPtr&&, const caf::settings&) {
    mgr_ = mgr;
    return caf::none;
  }

  template <class LowerLayerPtr>
  bool prepare_send(LowerLayerPtr down) {
    while (down->can_send_more()) {
      auto [val, done, err] = controller_messages_->poll();
      if (val) {
        if (!write(down, *val)) {
          down->abort_reason(make_error(ec::invalid_message));
          return false;
        }
      } else if (done) {
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
  void abort(LowerLayerPtr, const caf::error& reason) {
    peer_messages_->flush();
    if (reason == caf::sec::socket_disconnected
        || reason == caf::sec::discarded)
      peer_messages_->on_complete();
    else
      peer_messages_->on_error(reason);
  }

  template <class LowerLayerPtr>
  void after_reading(LowerLayerPtr) {
    peer_messages_->flush();
  }

  template <class LowerLayerPtr>
  ptrdiff_t consume(LowerLayerPtr down, caf::byte_span buf) {
    caf::binary_deserializer src{nullptr, buf};
    auto tag = alm_message_type{0};
    if (!src.apply(tag))
      return -1;
    switch (tag) {
      case alm_message_type::data:
      case alm_message_type::command:
        if (!handle_msg(down, src, static_cast<packed_message_type>(tag)))
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
  bool handle_msg(LowerLayerPtr down, caf::binary_deserializer& src,
                  packed_message_type tag) {
    auto fail = [&down] {
      BROKER_ERROR("got malformed data message");
      down->abort_reason(make_error(ec::invalid_message));
      return false;
    };
    // Extract path.
    alm::multipath path;
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
    auto bytes = caf::as_bytes(src.remainder());
    auto first = reinterpret_cast<const std::byte*>(bytes.begin());
    auto last = reinterpret_cast<const std::byte*>(bytes.end());
    auto payload = std::vector<std::byte>{first, last};
    // Push data down the pipeline.
    auto packed = packed_message{tag, msg_topic, std::move(payload)};
    auto msg = node_message{std::move(path), std::move(packed)};
    if (peer_messages_->push(std::move(msg)) == 0)
      down->suspend_reading();
    return true;
  }

  template <class LowerLayerPtr>
  bool write(LowerLayerPtr down, const node_message& msg) {
    auto&& [msg_path, msg_content] = msg.data();
    auto&& [msg_type, msg_topic, payload] = msg_content.data();
    down->begin_message();
    caf::binary_serializer sink{nullptr, down->message_buffer()};
    auto write_bytes = [&sink](caf::const_byte_span bytes) {
      sink.buf().insert(sink.buf().end(), bytes.begin(), bytes.end());
      return true;
    };
    auto write_topic = [&](const auto& x) {
      const auto& str = x.string();
      if (str.size() > 0xFFFF) {
        BROKER_ERROR("topic exceeds maximum size of 65535 characters");
        return false;
      }
      return sink.apply(static_cast<uint16_t>(str.size()))
             && write_bytes(caf::as_bytes(caf::make_span(str)));
    };
    return sink.apply(msg_type)                                   //
           && sink.apply(msg_path)                                //
           && write_topic(msg_topic)                              //
           && write_bytes(caf::as_bytes(caf::make_span(payload))) //
           && down->end_message();                                //
  }

  /// Stores the ID of the local Broker endpoint.
  caf::uuid this_peer_;

  /// Points to the manager that runs this protocol stack.
  caf::net::socket_manager* mgr_ = nullptr;

  /// Forwards outgoing messages to the peer. We write whatever we receive from
  /// this channel to the socket.
  caf::net::observer_adapter_ptr<node_message> controller_messages_;

  /// After receiving messages from the socket, we publish peer messages to this
  /// queue for processing by the controller.
  caf::net::publisher_adapter_ptr<node_message> peer_messages_;
};

} // namespace broker::detail
