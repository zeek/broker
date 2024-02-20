#include "broker/ping_envelope.hh"

#include "broker/endpoint_id.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/topic.hh"

#include <caf/detail/append_hex.hpp>

using namespace std::literals;

namespace broker {

namespace {

class default_ping_envelope : public ping_envelope {
public:
  default_ping_envelope(endpoint_id sender, endpoint_id receiver,
                        const std::byte* payload, size_t payload_size)
    : sender_(sender), receiver_(receiver), payload_size_(payload_size) {
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

using default_ping_envelope_ptr = intrusive_ptr<default_ping_envelope>;

} // namespace

envelope_type ping_envelope::type() const noexcept {
  return envelope_type::ping;
}

std::string_view ping_envelope::topic() const noexcept {
  return broker::topic::reserved;
}

envelope_ptr ping_envelope::with(endpoint_id new_sender,
                                 endpoint_id new_receiver) const {
  using decorator_ptr = intrusive_ptr<envelope::decorator<ping_envelope>>;
  return decorator_ptr::make(intrusive_ptr{new_ref, this}, new_sender,
                             new_receiver);
}

std::string ping_envelope::stringify() const {
  auto result = "ping("s;
  auto [bytes, num_bytes] = raw_bytes();
  caf::detail::append_hex(result, bytes, num_bytes);
  result += ')';
  return result;
}

ping_envelope_ptr ping_envelope::make(const endpoint_id& sender,
                                      const endpoint_id& receiver,
                                      const std::byte* payload,
                                      size_t payload_size) {
  return default_ping_envelope_ptr::make(sender, receiver, payload,
                                         payload_size);
}

expected<envelope_ptr> ping_envelope::deserialize(
  const endpoint_id& sender, const endpoint_id& receiver, uint16_t ttl,
  std::string_view topic_str, const std::byte* payload, size_t payload_size) {
  using impl_ptr = intrusive_ptr<envelope::deserialized<ping_envelope>>;
  return impl_ptr::make(sender, receiver, ttl, topic_str, payload,
                        payload_size);
}

} // namespace broker
