#pragma once

#include "broker/config.hh"
#include "broker/detail/inspect_enum.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/endpoint_id.hh"
#include "broker/fwd.hh"
#include "broker/intrusive_ptr.hh"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace broker {

/// Tags an envelope with the type of the serialized data. This enumeration is a
/// subset of @ref p2p_message_type.
enum class envelope_type : uint8_t {
  data = 1,
  command,
  routing_update,
  ping,
  pong,
};

/// @relates envelope_type
std::string to_string(envelope_type);

/// @relates envelope_type
bool from_string(std::string_view, envelope_type&);

/// @relates envelope_type
bool from_integer(uint8_t, envelope_type&);

/// @relates envelope_type
template <class Inspector>
bool inspect(Inspector& f, envelope_type& x) {
  return detail::inspect_enum(f, x);
}

/// Wraps a value of type @ref variant and associates it with a @ref topic.
class envelope {
public:
  envelope() noexcept : ref_count_(1) {}

  envelope(const envelope&) = delete;

  envelope& operator=(const envelope&) = delete;

  virtual ~envelope();

  /// Returns the type of the envelope.
  virtual envelope_type type() const noexcept = 0;

  /// Returns the time-to-live for the message in this envelope.
  virtual uint16_t ttl() const noexcept;

  /// Returns the sender of the message in this envelope or `nil` for "this
  /// node".
  virtual endpoint_id sender() const noexcept;

  /// Returns the receiver of the message in this envelope or `nil` for "all
  /// nodes".
  virtual endpoint_id receiver() const noexcept;

  /// Returns the topic for the data in this envelope.
  virtual std::string_view topic() const noexcept = 0;

  /// Returns the contained value in its serialized form.
  virtual std::pair<const std::byte*, size_t> raw_bytes() const noexcept = 0;

  /// Returns a new envelope with the given sender and receiver.
  virtual envelope_ptr with(endpoint_id new_sender,
                            endpoint_id new_receiver) const = 0;

  /// Returns a string representation of this envelope.
  virtual std::string stringify() const = 0;

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// write format.
  static expected<envelope_ptr> deserialize(const std::byte* data, size_t size);

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// JSON format.
  static expected<envelope_ptr> deserialize_json(const char* data, size_t size);

  /// @pre `type == envelope_type::data`
  data_envelope_ptr as_data() const;

  /// @pre `type == envelope_type::command`
  command_envelope_ptr as_command() const;

  /// @pre `type == envelope_type::routing_update`
  routing_update_envelope_ptr as_routing_update() const;

  /// @pre `type == envelope_type::ping`
  ping_envelope_ptr as_ping() const;

  /// @pre `type == envelope_type::pong`
  pong_envelope_ptr as_pong() const;

  /// Increments the reference count.
  void ref() const noexcept {
    ++ref_count_;
  }

  /// Decrements the reference count and destroys this object if the count
  /// reaches zero.
  void unref() const noexcept {
    if (--ref_count_ == 0)
      delete this;
  }

  /// Base type for decorators that wrap another envelope to override sender and
  /// receiver.
  template <class Decorated>
  class decorator : public Decorated {
  public:
    using decorated_ptr = intrusive_ptr<const Decorated>;

    decorator(decorated_ptr decorated, endpoint_id sender, endpoint_id receiver)
      : decorated_(std::move(decorated)), sender_(sender), receiver_(receiver) {
      // nop
    }

    endpoint_id sender() const noexcept override {
      return sender_;
    }

    endpoint_id receiver() const noexcept override {
      return receiver_;
    }

    std::string_view topic() const noexcept override {
      return decorated_->topic();
    }

    std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
      return decorated_->raw_bytes();
    }

  protected:
    decorated_ptr decorated_;
    endpoint_id sender_;
    endpoint_id receiver_;
  };

  template <class T>
  using mbr_allocator = detail::monotonic_buffer_resource::allocator<T>;

  template <class Base>
  class deserialized : public Base {
  public:
    deserialized(const endpoint_id& sender, const endpoint_id& receiver,
                 uint16_t ttl, std::string_view topic_str,
                 const std::byte* payload, size_t payload_size)
      : sender_(sender),
        receiver_(receiver),
        ttl_(ttl),
        topic_size_(topic_str.size()),
        payload_size_(payload_size) {
      // Note: we need to copy the topic and the data into our memory resource.
      // The pointers passed to the constructor are only valid for the duration
      // of the call.
      mbr_allocator<char> str_allocator{&buf_};
      topic_ = str_allocator.allocate(topic_str.size() + 1);
      memcpy(topic_, topic_str.data(), topic_str.size());
      topic_[topic_str.size()] = '\0';
      mbr_allocator<std::byte> byte_allocator{&buf_};
      payload_ = byte_allocator.allocate(payload_size);
      memcpy(payload_, payload, payload_size);
    }

    uint16_t ttl() const noexcept override {
      return ttl_;
    }

    endpoint_id sender() const noexcept override {
      return sender_;
    }

    endpoint_id receiver() const noexcept override {
      return receiver_;
    }

    std::string_view topic() const noexcept override {
      return {topic_, topic_size_};
    }

    std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
      return {payload_, payload_size_};
    }

    detail::monotonic_buffer_resource& buf() {
      return buf_;
    }

  private:
    endpoint_id sender_;

    endpoint_id receiver_;

    uint16_t ttl_;

    char* topic_;

    size_t topic_size_;

    std::byte* payload_;

    size_t payload_size_;

    detail::monotonic_buffer_resource buf_;
  };

private:
  using ref_count_t = std::atomic<size_t>;

  alignas(BROKER_CONSTRUCTIVE_INTERFERENCE_SIZE) mutable ref_count_t ref_count_;
};

/// A shared pointer to an @ref envelope.
/// @relates envelope
using envelope_ptr = intrusive_ptr<const envelope>;

/// @relates envelope
inline std::string to_string(const envelope& x) {
  return x.stringify();
}

/// @relates envelope
inline std::string to_string(const envelope_ptr& x) {
  if (x)
    return x->stringify();
  else
    return "<null>";
}

} // namespace broker
