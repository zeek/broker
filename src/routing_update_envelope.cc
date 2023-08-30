#include "broker/routing_update_envelope.hh"

#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/format/bin.hh"

#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>

namespace binfmt = broker::format::bin::v1;

namespace broker {

std::string_view routing_update_iterator::operator*() const {
  auto len  = size_t{0};
  auto ptr = pos_;
  if (!binfmt::read_varbyte(ptr, end_, len))
    throw std::logic_error{"invalid routing update"};
  if (ptr + len > end_)
    throw std::logic_error{"invalid routing update"};
  return {reinterpret_cast<const char*>(ptr), len};
}

routing_update_iterator& routing_update_iterator::operator++() {
  auto skip = size_t{0};
  if (!binfmt::read_varbyte(pos_, end_, skip))
    throw std::logic_error{"invalid routing update"};
  pos_ += skip;
  return *this;
}

envelope_type routing_update_envelope::type() const noexcept {
  return envelope_type::routing_update;
}

std::string_view routing_update_envelope::topic() const noexcept {
  return broker::topic::reserved;
}

envelope_ptr routing_update_envelope::with(endpoint_id new_sender,
                                           endpoint_id new_receiver) {
  using decorator_t = envelope::decorator<routing_update_envelope>;
  return make_intrusive<decorator_t>(intrusive_ptr{new_ref, this}, new_sender,
                                     new_receiver);
}

size_t routing_update_envelope::filter_size() const noexcept {
  auto [data, data_size] = raw_bytes();
  auto result = size_t{0};
  auto ptr = binfmt::const_byte_pointer{data};
  binfmt::read_varbyte(ptr, data + data_size, result);
  return result;
}

routing_update_iterator routing_update_envelope::begin() const noexcept {
  // The first entry of the filter is after the varbyte-encoded filter size.
  auto [data, data_size] = raw_bytes();
  auto unused = size_t{0};
  auto ptr = binfmt::const_byte_pointer{data};
  binfmt::read_varbyte(ptr, data + data_size, unused);
  return routing_update_iterator{ptr, data + data_size};
}

namespace {

class default_routing_update_envelope : public routing_update_envelope {
public:
  using byte_buffer = std::vector<std::byte>;

  explicit default_routing_update_envelope(byte_buffer payload)
    : payload_(std::move(payload)) {
    // nop
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {payload_.data(), payload_.size()};
  }

private:
  /// The filter data. Before the serialized data, we have a jump table that
  /// allows us to skip over entries for filter_at() calls. The jump table has
  /// `size_` entries, with each entry having four bytes (uint32_t).
  std::vector<std::byte> payload_;
};

} // namespace

routing_update_envelope_ptr
routing_update_envelope::make(const std::vector<broker::topic>& entries) {
  std::vector<std::byte> bytes;
  bytes.reserve(64);
  auto iter = std::back_inserter(bytes);
  binfmt::write_varbyte(entries.size(), iter);
  for (auto& entry : entries) {
    const auto& str = entry.string();
    binfmt::write_varbyte(str.size(), iter);
    for (auto c : str)
      *iter++ = static_cast<std::byte>(c);
  }
  return make_intrusive<default_routing_update_envelope>(std::move(bytes));
}

namespace {

/// A @ref routing_update_envelope for deserialized routing_update.
class deserialized_routing_update_envelope
  : public envelope::deserialized<routing_update_envelope> {
public:
  using super = envelope::deserialized<routing_update_envelope>;

  using super::super;

  error parse() {
    // TODO: sanity check the filter data.
    return error{};
  }
};

} // namespace

expected<envelope_ptr> routing_update_envelope::deserialize(
  const endpoint_id& sender, const endpoint_id& receiver, uint16_t ttl,
  std::string_view topic_str, const std::byte* payload, size_t payload_size) {
  using impl_t = deserialized_routing_update_envelope;
  auto result = make_intrusive<impl_t>(sender, receiver, ttl, topic_str,
                                       payload, payload_size);
  if (auto err = result->parse())
    return err;
  return {std::move(result)};
}

} // namespace broker
