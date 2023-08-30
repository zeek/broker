#include "broker/pong_envelope.hh"

#include "broker/endpoint_id.hh"
#include "broker/expected.hh"
#include "broker/ping_envelope.hh"
#include "broker/topic.hh"

namespace broker {

namespace {

class default_pong_envelope : public pong_envelope {
public:
  default_pong_envelope(endpoint_id sender, endpoint_id receiver,
                         const std::byte* payload,
                        size_t payload_size)
    : sender_(sender),
      receiver_(receiver),
      payload_size_(payload_size) {
    payload_ = std::make_unique<std::byte[]>(payload_size);
    memcpy(payload_.get(), payload, payload_size);
  }

  endpoint_id sender() const noexcept override {
    return sender_;
  }

  endpoint_id receiver() const noexcept override {
    return receiver_;
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {payload_.get(), payload_size_};
  }

private:
  endpoint_id sender_;
  endpoint_id receiver_;
  std::unique_ptr<std::byte[]> payload_;
  size_t payload_size_;
};

} // namespace

envelope_type pong_envelope::type() const noexcept {
  return envelope_type::pong;
}

std::string_view pong_envelope::topic() const noexcept {
  return broker::topic::reserved;
}

envelope_ptr pong_envelope::with(endpoint_id new_sender,
                                 endpoint_id new_receiver) {
  using decorator_t = envelope::decorator<pong_envelope>;
  return make_intrusive<decorator_t>(intrusive_ptr{new_ref, this}, new_sender,
                                     new_receiver);
}

pong_envelope_ptr pong_envelope::make(const endpoint_id& sender,
                                      const endpoint_id& receiver,
                                      const std::byte* payload,
                                      size_t payload_size) {
  return make_intrusive<default_pong_envelope>(sender, receiver, payload,
                                               payload_size);
}

pong_envelope_ptr pong_envelope::make(const ping_envelope_ptr& ping) {
  auto [payload, payload_size] = ping->raw_bytes();
  return make(ping->receiver(), ping->sender(), payload, payload_size);
}

expected<envelope_ptr> pong_envelope::deserialize(
  const endpoint_id& sender, const endpoint_id& receiver, uint16_t ttl,
  std::string_view topic_str, const std::byte* payload, size_t payload_size) {
  using impl_t = envelope::deserialized<pong_envelope>;
  return make_intrusive<impl_t>(sender, receiver, ttl, topic_str, payload,
                                payload_size);
}

} // namespace broker
