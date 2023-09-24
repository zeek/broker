#include "broker/builder.hh"

#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/envelope.hh"
#include "broker/error.hh"
#include "broker/topic.hh"

namespace broker {

namespace {

template <class T>
using mbr_allocator = detail::monotonic_buffer_resource::allocator<T>;

class builder_envelope : public data_envelope {
public:
  builder_envelope(std::string_view topic_str, std::vector<std::byte> bytes,
                   size_t offset)
    : topic_size_(topic_str.size()), bytes_(std::move(bytes)), offset_(offset) {
    mbr_allocator<char> str_allocator{&buf_};
    topic_ = str_allocator.allocate(topic_str.size() + 1);
    memcpy(topic_, topic_str.data(), topic_str.size());
    topic_[topic_str.size()] = '\0';
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
    return {bytes_.data() + offset_, bytes_.size() - offset_};
  }

  error parse() {
    error result;
    root_ = do_parse(buf_, result);
    return result;
  }

private:
  char* topic_;
  size_t topic_size_;
  variant_data* root_ = nullptr;
  std::vector<std::byte> bytes_;
  detail::monotonic_buffer_resource buf_;
  size_t offset_;
};

data_envelope_ptr make_builder_envelope(std::string_view topic_str,
                                        std::vector<std::byte> bytes,
                                        size_t offset) {
  auto res = make_intrusive<builder_envelope>(topic_str, std::move(bytes),
                                              offset);
#ifndef NDEBUG
  if (auto err = res->parse()) {
    auto errstr = to_string(err);
    fprintf(stderr, "make_builder_envelope received malformed data: %s\n",
            errstr.c_str());
    abort();
  }
#else
  std::ignore = res->parse();
#endif
  return res;
}

} // namespace

} // namespace broker

namespace broker::detail {

template <class Builder>
data_envelope_ptr builder_access::build(Builder& src, std::string_view str) {
  auto offset = format::bin::v1::encode_sequence_end(src.tag(), src.size_,
                                                     src.bytes_);
  return make_builder_envelope(str, std::move(src.bytes_), offset);
}

} // namespace broker::detail

namespace broker {

set_builder::set_builder() {
  format::bin::v1::encode_sequence_begin(bytes_);
}

std::pair<const std::byte*, size_t> set_builder::bytes() {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  return {bytes_.data() + offset, bytes_.size() - offset};
}

data_envelope_ptr set_builder::build_envelope(std::string_view topic_str) && {
  return detail::builder_access::build(*this, topic_str);
}

variant set_builder::build() && {
  return detail::builder_access::build(*this, topic::reserved)->value();
}

table_builder::table_builder() {
  format::bin::v1::encode_sequence_begin(bytes_);
}

std::pair<const std::byte*, size_t> table_builder::bytes() {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  return {bytes_.data() + offset, bytes_.size() - offset};
}

data_envelope_ptr table_builder::build_envelope(std::string_view topic_str) && {
  return detail::builder_access::build(*this, topic_str);
}

variant table_builder::build() && {
  return detail::builder_access::build(*this, topic::reserved)->value();
}

list_builder::list_builder() {
  format::bin::v1::encode_sequence_begin(bytes_);
}

std::pair<const std::byte*, size_t> list_builder::bytes() {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  return {bytes_.data() + offset, bytes_.size() - offset};
}

data_envelope_ptr list_builder::build_envelope(std::string_view topic_str) && {
  return detail::builder_access::build(*this, topic_str);
}

variant list_builder::build() && {
  return detail::builder_access::build(*this, topic::reserved)->value();
}

void list_builder::reset() {
  size_ = 0;
  bytes_ = builder_buffer{};
  format::bin::v1::encode_sequence_begin(bytes_);
}

} // namespace broker
