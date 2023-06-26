#include "broker/builder.hh"

#include "broker/data.hh"
#include "broker/topic.hh"

#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

#include <new>

namespace broker {

namespace {

class builder_envelope : public data_envelope {
public:
  builder_envelope(std::vector<std::byte> bytes, size_t offset)
    : bytes_(std::move(bytes)), offset_(offset) {
    // nop
  }

  data_view get_data() const noexcept override {
    return {root_, shared_from_this()};
  }

  const topic& get_topic() const noexcept override {
    return topic_;
  }

  bool is_root(const detail::data_view_value* val) const noexcept override {
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
  detail::data_view_value* root_ = nullptr;
  topic topic_;
  std::vector<std::byte> bytes_;
  detail::monotonic_buffer_resource buf_;
  size_t offset_;
};

data_envelope_ptr make_builder_envelope(std::vector<std::byte> bytes,
                                        size_t offset) {
  auto res = std::make_shared<builder_envelope>(std::move(bytes), offset);
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
  detail::bbf::encode_sequence_begin(bytes_);
}

data_view set_builder::build() && {
  auto offset = detail::bbf::encode_sequence_end(tag(), size_, bytes_);
  auto enevlope = make_builder_envelope(std::move(bytes_), offset);
  return enevlope->get_data();
}

table_builder::table_builder() {
  detail::bbf::encode_sequence_begin(bytes_);
}

data_view table_builder::build() && {
  auto offset = detail::bbf::encode_sequence_end(tag(), size_, bytes_);
  auto enevlope = make_builder_envelope(std::move(bytes_), offset);
  return enevlope->get_data();
}

vector_builder::vector_builder() {
  detail::bbf::encode_sequence_begin(bytes_);
}

data_view vector_builder::build() && {
  auto offset = detail::bbf::encode_sequence_end(tag(), size_, bytes_);
  auto enevlope = make_builder_envelope(std::move(bytes_), offset);
  return enevlope->get_data();
}

} // namespace broker
