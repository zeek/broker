#include "broker/command_envelope.hh"

#include "broker/endpoint_id.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

#include <caf/binary_deserializer.hpp>

namespace broker {

envelope_type command_envelope::type() const noexcept {
  return envelope_type::command;
}

envelope_ptr command_envelope::with(endpoint_id new_sender,
                                    endpoint_id new_receiver) {
  throw std::logic_error("command_envelope::with called");
}

command_envelope_ptr command_envelope::make(broker::topic t,
                                            internal_command d) {
  throw std::logic_error("command_envelope::make called");
}

command_envelope_ptr command_envelope::make(const endpoint_id& sender,
                                            const endpoint_id& receiver,
                                            std::string topic,
                                            internal_command d) {
  throw std::logic_error("command_envelope::make called");
}

namespace {

/// A @ref command_envelope for deserialized command.
class deserialized_command_envelope
  : public envelope::deserialized<command_envelope> {
public:
  using super = envelope::deserialized<command_envelope>;

  using super::super;

  const internal_command& value() const noexcept override{
    return value_;
  }

  error parse() {
    auto [data, data_size] = this->raw_bytes();
    caf::binary_deserializer src{nullptr, data, data_size};
    if (!src.apply(value_))
      return make_error(ec::invalid_data);
    return error{};
  }

private:
  internal_command value_;
};

} // namespace

expected<envelope_ptr> command_envelope::deserialize(
  const endpoint_id& sender, const endpoint_id& receiver, uint16_t ttl,
  std::string_view topic_str, const std::byte* payload, size_t payload_size) {
  using impl_t = deserialized_command_envelope;
  auto result = make_intrusive<impl_t>(sender, receiver, ttl, topic_str,
                                       payload, payload_size);
  if (auto err = result->parse())
    return err;
  return {std::move(result)};
}

} // namespace broker
