#pragma once

#include "broker/envelope.hh"

namespace broker {

/// Represents a ping message.
class ping_envelope : public envelope {
public:
  envelope_type type() const noexcept final;

  std::string_view topic() const noexcept override;

  envelope_ptr with(endpoint_id new_sender,
                    endpoint_id new_receiver) const final;

  std::string stringify() const override;

  /// Creates a new ping envelope from the given arguments.
  static ping_envelope_ptr make(const endpoint_id& sender,
                                const endpoint_id& receiver,
                                const std::byte* payload, size_t payload_size);

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// write format.
  static expected<envelope_ptr>
  deserialize(const endpoint_id& sender, const endpoint_id& receiver,
              uint16_t ttl, std::string_view topic_str,
              const std::byte* payload, size_t payload_size);
};

/// A shared pointer to a @ref ping_envelope.
/// @relates ping_envelope
using ping_envelope_ptr = intrusive_ptr<const ping_envelope>;

} // namespace broker
