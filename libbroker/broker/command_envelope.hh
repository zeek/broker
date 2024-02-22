#pragma once

#include "broker/envelope.hh"

namespace broker {

/// Wraps an @ref internal_command and associates it with a @ref topic.
class command_envelope : public envelope {
public:
  envelope_type type() const noexcept final;

  envelope_ptr with(endpoint_id new_sender,
                    endpoint_id new_receiver) const final;

  std::string stringify() const override;

  /// Returns the contained command.
  virtual const internal_command& value() const noexcept = 0;

  /// Creates a new command envelope from the arguments.
  static command_envelope_ptr make(broker::topic t, internal_command d);

  static command_envelope_ptr make(const endpoint_id& sender,
                                   const endpoint_id& receiver,
                                   std::string topic, internal_command d);

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// write format.
  static expected<envelope_ptr>
  deserialize(const endpoint_id& sender, const endpoint_id& receiver,
              uint16_t ttl, std::string_view topic_str,
              const std::byte* payload, size_t payload_size);
};

/// A shared pointer to a @ref command_envelope.
/// @relates command_envelope
using command_envelope_ptr = intrusive_ptr<const command_envelope>;

} // namespace broker
