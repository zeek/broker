#include "broker/command_envelope.hh"

#include "broker/endpoint_id.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/deep_to_string.hpp>

using namespace std::literals;

namespace broker {

namespace {

/// Decorates another data envelope to override sender and receiver.
class command_envelope_decorator
  : public envelope::decorator<command_envelope> {
public:
  using super = envelope::decorator<command_envelope>;

  using super::super;

  const internal_command& value() const noexcept override {
    return decorated_->value();
  }
};

using command_envelope_decorator_ptr =
  intrusive_ptr<command_envelope_decorator>;

} // namespace

envelope_ptr command_envelope::with(endpoint_id new_sender,
                                    endpoint_id new_receiver) const {
  return command_envelope_decorator_ptr::make(intrusive_ptr{new_ref, this},
                                              new_sender, new_receiver);
}

std::string command_envelope::stringify() const {
  auto result = "command("s;
  result += topic();
  result += ", ";
  result += caf::deep_to_string(value());
  result += ')';
  return result;
}

namespace {

/// A @ref command_envelope for deserialized command.
class default_command_envelope : public command_envelope {
public:
  using super = command_envelope;

  default_command_envelope(const endpoint_id& sender,
                           const endpoint_id& receiver, std::string&& topic_str,
                           internal_command&& cmd)
    : sender_(sender),
      receiver_(receiver),
      topic_(std::move(topic_str)),
      value_(std::move(cmd)) {
    caf::binary_serializer sink{nullptr, buf_};
    if (!sink.apply(value_))
      throw std::logic_error("failed to serialize command");
  }

  default_command_envelope(std::string&& topic_str, internal_command&& cmd)
    : topic_(topic_str), value_(std::move(cmd)) {
    caf::binary_serializer sink{nullptr, buf_};
    if (!sink.apply(value_))
      throw std::logic_error("failed to serialize command");
  }

  endpoint_id sender() const noexcept override {
    return sender_;
  }

  endpoint_id receiver() const noexcept override {
    return receiver_;
  }

  const internal_command& value() const noexcept override {
    return value_;
  }

  std::string_view topic() const noexcept override {
    return topic_;
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {reinterpret_cast<const std::byte*>(buf_.data()), buf_.size()};
  }

private:
  endpoint_id sender_;
  endpoint_id receiver_;
  std::string topic_;
  internal_command value_;
  caf::byte_buffer buf_;
};

using default_command_envelope_ptr = intrusive_ptr<default_command_envelope>;

} // namespace

envelope_type command_envelope::type() const noexcept {
  return envelope_type::command;
}

command_envelope_ptr command_envelope::make(broker::topic t,
                                            internal_command d) {
  return default_command_envelope_ptr::make(std::move(t).move_string(),
                                            std::move(d));
}

command_envelope_ptr command_envelope::make(const endpoint_id& sender,
                                            const endpoint_id& receiver,
                                            std::string topic,
                                            internal_command d) {
  return default_command_envelope_ptr::make(sender, receiver, std::move(topic),
                                            std::move(d));
}

namespace {

/// A @ref command_envelope for deserialized command.
class deserialized_command_envelope
  : public envelope::deserialized<command_envelope> {
public:
  using super = envelope::deserialized<command_envelope>;

  using super::super;

  const internal_command& value() const noexcept override {
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

using deserialized_command_envelope_ptr =
  intrusive_ptr<deserialized_command_envelope>;

} // namespace

expected<envelope_ptr> command_envelope::deserialize(
  const endpoint_id& sender, const endpoint_id& receiver, uint16_t ttl,
  std::string_view topic_str, const std::byte* payload, size_t payload_size) {
  auto result = deserialized_command_envelope_ptr::make(sender, receiver, ttl,
                                                        topic_str, payload,
                                                        payload_size);
  if (auto err = result->parse())
    return err;
  return {std::move(result)};
}

void convert(const command_envelope_ptr& cmd, std::string& str) {
  if (!cmd) {
    str = "null";
    return;
  }
  str = cmd->stringify();
}

} // namespace broker
