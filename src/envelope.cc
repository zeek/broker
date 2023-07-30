#include "broker/envelope.hh"

#include "broker/defaults.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/error.hh"
#include "broker/internal/type_id.hh"
#include "broker/topic.hh"
#include "broker/variant.hh"
#include "broker/variant_data.hh"

#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

namespace broker {

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

  variant value() const noexcept override {
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

  variant value() const noexcept override {
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
