#pragma once

#include <caf/net/consumer_adapter.hpp>
#include <caf/net/producer_adapter.hpp>
#include <caf/tag/message_oriented.hpp>
#include <caf/tag/no_auto_reading.hpp>
#include <caf/uuid.hpp>

#include "broker/alm/multipath.hh"
#include "broker/message.hh"

namespace broker::detail {

/// Implements the Broker message exchange protocol. Note: this only covers the
/// communication after the handshake. The handshake process is implemented by
/// the connector.
class protocol : public caf::tag::no_auto_reading {
public:
  using input_tag = caf::tag::message_oriented;

  using consumer_resource = caf::async::consumer_resource<node_message>;

  using consumer_buffer = consumer_resource::buffer_type;

  using consumer_adapter_ptr = caf::net::consumer_adapter_ptr<consumer_buffer>;

  using consumer_adapter = consumer_adapter_ptr::element_type;

  using producer_resource = caf::async::producer_resource<node_message>;

  using producer_buffer = producer_resource::buffer_type;

  using producer_adapter_ptr = caf::net::producer_adapter_ptr<producer_buffer>;

  using producer_adapter = producer_adapter_ptr::element_type;

  explicit protocol(caf::uuid this_peer) : this_peer_(this_peer) {
    // nop
  }

  void connect_flows(caf::net::socket_manager* mgr, consumer_resource in,
                     producer_resource out) {
    in_ = consumer_adapter::try_open(mgr, in);
    out_ = producer_adapter::try_open(mgr, out);
  }

  template <class LowerLayerPtr>
  caf::error
  init(caf::net::socket_manager* mgr, LowerLayerPtr&&, const caf::settings&) {
    mgr_ = mgr;
    if (!in_)
      return make_error(ec::cannot_open_resource,
                        "protocol instance failed to open consumer resource");
    else if (!out_)
      return make_error(ec::cannot_open_resource,
                        "protocol instance failed to open producer resource");
    else
      return caf::none;
  }

  template <class LowerLayerPtr>
  struct send_helper {
    protocol* proto;
    LowerLayerPtr down;
    bool aborted = false;
    size_t consumed = 0;

    send_helper(protocol* proto, LowerLayerPtr down)
      : proto(proto), down(down) {
      // nop
    }

    void on_next(caf::span<const node_message> items) {
      BROKER_ASSERT(items.size() == 1);
      for (const auto& item : items) {
        if (!proto->write(down, item)) {
          aborted = true;
          down->abort_reason(make_error(ec::invalid_message));
        }
      }
    }

    void on_complete() {
      // nop
    }

    void on_error(const caf::error&) {
      // nop
    }
  };

  template <class LowerLayerPtr>
  bool prepare_send(LowerLayerPtr down) {
    send_helper helper{this, down};
    while (down->can_send_more() && in_) {
      auto [ok, consumed] = in_->pull(caf::async::delay_errors, 1, helper);
      if (!ok) {
        in_ = nullptr;
      } else if (helper.aborted) {
        down->abort_reason(make_error(ec::invalid_message));
        in_->cancel();
        in_ = nullptr;
        return false;
      } else if (consumed == 0) {
        return true;
      }
    }
    return true;
  }

  template <class LowerLayerPtr>
  bool done_sending(LowerLayerPtr) {
    return !in_ || !in_->has_data();
  }

  template <class LowerLayerPtr>
  void abort(LowerLayerPtr, const caf::error& reason) {
    BROKER_TRACE(BROKER_ARG(reason));
    if (out_) {
      if (reason == caf::sec::socket_disconnected
          || reason == caf::sec::discarded)
        out_->close();
      else
        out_->abort(reason);
      out_ = nullptr;
    }
    if (in_) {
      in_->cancel();
      in_ = nullptr;
    }
  }

  template <class LowerLayerPtr>
  ptrdiff_t consume(LowerLayerPtr down, caf::byte_span buf) {
    caf::binary_deserializer src{nullptr, buf};
    auto tag = alm_message_type{0};
    if (!out_) {
      BROKER_WARNING("consume called after output buffer has been closed");
      down->abort_reason(
        make_error(ec::shutting_down, "output buffer has been closed"));
      return -1;
    }
    if (!src.apply(tag)) {
      BROKER_ERROR("failed to serialize tag");
      down->abort_reason(
        make_error(ec::invalid_tag, "failed to serialize tag"));
      return -1;
    }
    switch (tag) {
      case alm_message_type::data:
      case alm_message_type::command:
      case alm_message_type::routing_update:
      case alm_message_type::path_revocation:
        if (!handle_msg(down, src, static_cast<packed_message_type>(tag)))
          return -1;
        break;
      default:
        BROKER_ERROR("received unknown tag");
        down->abort_reason(make_error(ec::invalid_tag, "received unknown tag"));
        return -1;
    }
    return static_cast<ptrdiff_t>(buf.size());
  }

private:
  template <class LowerLayerPtr>
  bool handle_msg(LowerLayerPtr down, caf::binary_deserializer& src,
                  packed_message_type tag) {
    auto fail = [&down] {
      BROKER_ERROR("received malformed data");
      down->abort_reason(make_error(ec::invalid_message, "malformed data"));
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
    BROKER_DEBUG("got a message of type" << tag << "with a payload of"
                                         << payload.size() << "bytes and topic"
                                         << msg_topic << "on socket"
                                         << down->handle().id);
    auto packed = packed_message{tag, msg_topic, std::move(payload)};
    auto msg = node_message{std::move(path), std::move(packed)};
    if (out_->push(std::move(msg)) == 0)
      down->suspend_reading();
    return true;
  }

  template <class LowerLayerPtr>
  bool write(LowerLayerPtr down, const node_message& msg) {
    const auto& [msg_path, msg_content] = msg.data();
    const auto& [msg_type, msg_topic, payload] = msg_content.data();
    BROKER_DEBUG("write a node message of type" << msg_type << "to socket"
                                                << down->handle().id);
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

  /// Incoming node messages from the peer.
  consumer_adapter_ptr in_;

  /// Outgoing node messages to the peer.
  producer_adapter_ptr out_;
};

} // namespace broker::detail
