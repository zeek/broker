#include "broker/builder.hh"

#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/envelope.hh"
#include "broker/error.hh"
#include "broker/topic.hh"

namespace broker {

namespace {

class builder_envelope : public data_envelope {
public:
  builder_envelope(std::vector<std::byte> bytes, size_t offset)
    : bytes_(std::move(bytes)), offset_(offset) {
    // nop
  }

  variant value() const noexcept override {
    return {root_, {new_ref, this}};
  }

  std::string_view topic() const noexcept override {
    return {};
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
  variant_data* root_ = nullptr;
  std::vector<std::byte> bytes_;
  detail::monotonic_buffer_resource buf_;
  size_t offset_;
};

data_envelope_ptr make_builder_envelope(std::vector<std::byte> bytes,
                                        size_t offset) {
  auto res = make_intrusive<builder_envelope>(std::move(bytes), offset);
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

set_builder::set_builder() {
  format::bin::v1::encode_sequence_begin(bytes_);
}

std::pair<const std::byte*, size_t> set_builder::bytes() {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  return {bytes_.data() + offset, bytes_.size() - offset};
}

variant set_builder::build() && {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  auto enevlope = make_builder_envelope(std::move(bytes_), offset);
  return enevlope->value();
}

table_builder::table_builder() {
  format::bin::v1::encode_sequence_begin(bytes_);
}

std::pair<const std::byte*, size_t> table_builder::bytes() {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  return {bytes_.data() + offset, bytes_.size() - offset};
}

variant table_builder::build() && {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  auto enevlope = make_builder_envelope(std::move(bytes_), offset);
  return enevlope->value();
}

list_builder::list_builder() {
  format::bin::v1::encode_sequence_begin(bytes_);
}

std::pair<const std::byte*, size_t> list_builder::bytes() {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  return {bytes_.data() + offset, bytes_.size() - offset};
}

variant list_builder::build() && {
  auto offset = format::bin::v1::encode_sequence_end(tag(), size_, bytes_);
  auto enevlope = make_builder_envelope(std::move(bytes_), offset);
  return enevlope->value();
}

} // namespace broker
