#pragma once

#include "broker/config.hh"
#include "broker/detail/inspect_enum.hh"
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

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// write format.
  static expected<envelope_ptr> deserialize(const std::byte* data, size_t size);

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

private:
  using ref_count_t = std::atomic<size_t>;

  alignas(BROKER_CONSTRUCTIVE_INTERFERENCE_SIZE) mutable ref_count_t ref_count_;
};

/// A shared pointer to an @ref envelope.
/// @relates envelope
using envelope_ptr = intrusive_ptr<envelope>;

/// Wraps a value of type @ref variant and associates it with a @ref topic.
class data_envelope : public envelope {
public:
  envelope_type type() const noexcept final;

  /// Returns the contained value.
  /// @pre `root != nullptr`
  virtual variant value() noexcept = 0;

  /// Checks whether `val` is the root value.
  virtual bool is_root(const variant_data* val) const noexcept = 0;

  /// Creates a new data envolope from the given @ref topic and @ref data.
  static data_envelope_ptr make(broker::topic t, const data& d);

  /// Creates a new data envolope from the given @ref topic and @ref data.
  static data_envelope_ptr make(broker::topic t, variant d);

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// write format.
  static expected<envelope_ptr> deserialize(const std::byte* data, size_t size);

protected:
  /// Parses the data returned from @ref raw_bytes.
  variant_data* do_parse(detail::monotonic_buffer_resource& buf, error& err);
};

/// A shared pointer to a @ref data_envelope.
/// @relates data_envelope
using data_envelope_ptr = intrusive_ptr<data_envelope>;

/// Wraps an @ref internal_command and associates it with a @ref topic.
class command_envelope : public envelope {
public:
  envelope_type type() const noexcept final;

  /// Returns the contained command.
  virtual internal_command& value() const noexcept = 0;
};

/// A shared pointer to a @ref command_envelope.
/// @relates command_envelope
using command_envelope_ptr = intrusive_ptr<command_envelope>;

/// Represents a ping message.
class ping_envelope : public envelope {
public:
  envelope_type type() const noexcept final;
};

/// A shared pointer to a @ref ping_envelope.
/// @relates ping_envelope
using ping_envelope_ptr = intrusive_ptr<ping_envelope>;

/// Represents a pong message.
class pong_envelope : public envelope {
public:
  envelope_type type() const noexcept final;
};

/// A shared pointer to a @ref pong_envelope.
/// @relates pong_envelope
using pong_envelope_ptr = intrusive_ptr<pong_envelope>;

} // namespace broker
