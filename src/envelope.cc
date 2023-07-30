#include "broker/envelope.hh"

#include "broker/defaults.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/format/bin.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/topic.hh"
#include "broker/variant.hh"
#include "broker/variant_data.hh"

#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

namespace broker {

std::string to_string(envelope_type x) {
  // Same strings since packed_message is a subset of p2p_message.
  return to_string(static_cast<p2p_message_type>(x));
}

bool from_string(std::string_view str, envelope_type& x) {
  auto tmp = p2p_message_type{0};
  if (from_string(str, tmp) && static_cast<uint8_t>(tmp) <= 5) {
    x = static_cast<envelope_type>(tmp);
    return true;
  } else {
    return false;
  }
}

bool from_integer(uint8_t val, envelope_type& x) {
  if (val <= 0x04) {
    auto tmp = p2p_message_type{0};
    if (from_integer(val, tmp)) {
      x = static_cast<envelope_type>(tmp);
      return true;
    }
  }
  return false;
}

namespace {

template <class T>
using mbr_allocator = broker::detail::monotonic_buffer_resource::allocator<T>;

using const_byte_pointer = const std::byte*;

} // namespace

envelope::~envelope() {
  // nop
}

uint16_t envelope::ttl() const noexcept {
  return defaults::ttl;
}

endpoint_id envelope::sender() const noexcept {
  return endpoint_id::nil();
}

endpoint_id envelope::receiver() const noexcept {
  return endpoint_id::nil();
}

expected<envelope_ptr> envelope::deserialize(const std::byte* data,
                                             size_t size) {
  // Format is as follows:
  // -  16 bytes: sender
  // -  16 bytes: receiver
  // -   1 byte : message type
  // -   2 bytes: TTL
  // -   2 bytes: topic length T
  // -   T bytes: topic
  // - remainder: payload (type-specific)
  if (size < 37)
    return make_error(ec::invalid_data, "message too short");
  switch (static_cast<envelope_type>(data[32])) {
    default:
      return make_error(ec::invalid_data, "invalid message type");
    case envelope_type::data:
      return data_envelope::deserialize(data, size);
  }
}

envelope_type data_envelope::type() const noexcept {
  return envelope_type::data;
}

envelope_type command_envelope::type() const noexcept {
  return envelope_type::command;
}

envelope_type ping_envelope::type() const noexcept {
  return envelope_type::ping;
}

envelope_type pong_envelope::type() const noexcept {
  return envelope_type::pong;
}

namespace {

/// A @ref data_envelope for deserialized data.
class deserialized_data_envelope : public data_envelope {
public:
  deserialized_data_envelope(endpoint_id sender, endpoint_id receiver,
                             uint16_t ttl, const char* topic_str,
                             size_t topic_size, const std::byte* data,
                             size_t data_size)
    : sender_(sender),
      receiver_(receiver),
      ttl_(ttl),
      topic_size_(topic_size),
      data_size_(data_size) {
    // Note: we need to copy the topic and the data into our memory resource.
    // The pointers passed to the constructor are only valid for the duration of
    // the call.
    mbr_allocator<char> str_allocator{&buf_};
    topic_ = str_allocator.allocate(topic_size + 1);
    memcpy(topic_, topic_str, topic_size);
    topic_[topic_size] = '\0';
    mbr_allocator<std::byte> byte_allocator{&buf_};
    data_ = byte_allocator.allocate(data_size);
    memcpy(data_, data, data_size);
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

  variant value() noexcept override {
    return {root_, {new_ref, this}};
  }

  std::string_view topic() const noexcept override {
    return {topic_, topic_size_};
  }

  bool is_root(const variant_data* val) const noexcept override {
    return val == root_;
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {data_, data_size_};
  }

  error parse() {
    error result;
    root_ = do_parse(buf_, result);
    return result;
  }

private:
  variant_data* root_ = nullptr;

  endpoint_id sender_;

  endpoint_id receiver_;

  uint16_t ttl_;

  char* topic_;

  size_t topic_size_;

  std::byte* data_;

  size_t data_size_;

  detail::monotonic_buffer_resource buf_;
};

} // namespace

expected<envelope_ptr> data_envelope::deserialize(const std::byte* data,
                                                  size_t size) {
  // Format: see envelope::deserialize.
  if (size < 37)
    return make_error(ec::invalid_data, "message too short");
  auto advance = [&](size_t n) {
    data += n;
    size -= n;
  };
  // Extract the sender.
  auto sender = endpoint_id::from_bytes(data);
  advance(16);
  // Extract the receiver.
  auto receiver = endpoint_id::from_bytes(data);
  advance(16);
  // Check the type.
  if (static_cast<envelope_type>(*data) != envelope_type::data)
    return make_error(ec::invalid_data, "expected a data message");
  advance(1);
  // Extract the TTL.
  auto ttl = uint16_t{0};
  std::memcpy(&ttl, data, sizeof(ttl));
  ttl = format::bin::v1::from_network_order(ttl);
  advance(2);
  // Extract the topic.
  auto topic_size = uint16_t{0};
  std::memcpy(&topic_size, data + 2, sizeof(topic_size));
  advance(2);
  if (topic_size > size)
    return make_error(ec::invalid_data, "invalid topic size");
  auto topic_data = reinterpret_cast<const char*>(data);
  advance(topic_size);
  // Sanity check: we need at least 1 byte for the payload.
  if (size == 0)
    return make_error(ec::invalid_data, "missing payload");
  // Construct the envelope.
  using impl_t = deserialized_data_envelope;
  auto result = make_intrusive<impl_t>(sender, receiver, ttl, topic_data,
                                       topic_size, data, size);
  // Parse the payload.
  if (auto err = result->parse())
    return err;
  // Done.
  return {std::move(result)};
}

variant_data* data_envelope::do_parse(detail::monotonic_buffer_resource& buf,
                                      error& err) {
  auto [bytes, size] = raw_bytes();
  if (bytes == nullptr || size == 0) {
    err = make_error(ec::deserialization_failed, "cannot parse null data");
    return nullptr;
  }
  // Create the root object.
  variant_data* root;
  {
    mbr_allocator<variant_data> allocator{&buf};
    root = new (allocator.allocate(1)) variant_data();
  }
  // Parse the data. This is a shallow parse, which is why we need to copy the
  // bytes into the buffer resource first.
  auto end = bytes + size;
  auto [ok, pos] = root->parse_shallow(buf, bytes, end);
  if (ok && pos == end)
    return root;
  err = make_error(ec::deserialization_failed, "failed to parse data");
  return nullptr;
}

namespace {

/// The default implementation for @ref data_envelope that wraps a byte buffer
/// and a topic..
class default_data_envelope : public data_envelope {
public:
  default_data_envelope(std::string topic_str, caf::byte_buffer bytes)
    : topic_(std::move(topic_str)), bytes_(std::move(bytes)) {
    // nop
  }

  variant value() noexcept override {
    return {root_, {new_ref, this}};
  }

  std::string_view topic() const noexcept override {
    return topic_;
  }

  bool is_root(const variant_data* val) const noexcept override {
    return val == root_;
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {reinterpret_cast<const std::byte*>(bytes_.data()), bytes_.size()};
  }

  error parse() {
    error result;
    root_ = do_parse(buf_, result);
    return result;
  }

private:
  variant_data* root_ = nullptr;
  std::string topic_;
  caf::byte_buffer bytes_;
  detail::monotonic_buffer_resource buf_;
};

} // namespace

data_envelope_ptr data_envelope::make(broker::topic t, const data& d) {
  caf::byte_buffer buf;
  caf::binary_serializer sink{nullptr, buf};
#ifndef NDEBUG
  if (auto ok = sink.apply(d); !ok) {
    auto errstr = caf::to_string(sink.get_error());
    fprintf(stderr, "broker::envelope::make failed to serialize data: %s\n",
            errstr.c_str());
    abort();
  }
#else
  std::ignore = sink.apply(d);
#endif
  auto res = make_intrusive<default_data_envelope>(std::move(t).move_string(),
                                                   std::move(buf));
#ifndef NDEBUG
  if (auto err = res->parse()) {
    auto errstr = to_string(err);
    fprintf(stderr, "broker::envelope::make generated malformed data: %s\n",
            errstr.c_str());
    abort();
  }
#else
  std::ignore = res->parse();
#endif
  return res;
}

namespace {

/// Wraps a data view and a topic.
class data_envelope_wrapper : public data_envelope {
public:
  data_envelope_wrapper(std::string topic_str, variant val)
    : topic_(std::move(topic_str)), val_(std::move(val)) {
    // nop
  }

  variant value() noexcept override {
    return val_;
  }

  std::string_view topic() const noexcept override {
    return topic_;
  }

  bool is_root(const variant_data* val) const noexcept override {
    return val == val_.raw() && val_.is_root();
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    if (val_.is_root())
      val_.shared_envelope()->raw_bytes();
    return {nullptr, 0};
  }

private:
  std::string topic_;
  variant val_;
};

} // namespace

data_envelope_ptr data_envelope::make(broker::topic t, variant d) {
  return make_intrusive<data_envelope_wrapper>(std::move(t).move_string(),
                                               std::move(d));
}

} // namespace broker
