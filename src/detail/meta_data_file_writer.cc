#include "broker/detail/meta_data_file_writer.hh"

#include <caf/error.hpp>
#include <caf/sec.hpp>

#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker {
namespace detail {

meta_data_file_writer::meta_data_file_writer()
  : sink_(nullptr, buf_), writer_(sink_), flush_threshold_(1024) {
  // nop
}

meta_data_file_writer::~meta_data_file_writer() {
  flush();
}

caf::error meta_data_file_writer::open(std::string file_name) {
  if (auto err = flush()) {
    // Log the error, but ignore it otherwise.
    CAF_LOG_ERROR("flushing previous file failed:" << err);
  }
  f_.open(file_name, std::ofstream::binary);
  if (!f_.is_open())
    return make_error(ec::cannot_open_file, file_name);
  auto magic = format::magic;
  auto version = format::version;
  char header[sizeof(magic) + sizeof(version)];
  memcpy(header, &magic, sizeof(magic));
  memcpy(header + sizeof(magic), &version, sizeof(version));
  if (!f_.write(header, sizeof(header))) {
    BROKER_ERROR("unable to write to file:" << file_name);
    f_.close();
    return make_error(ec::cannot_write_file, file_name);
  }
  if (!f_.flush()) {
    BROKER_ERROR("unable to write to file (flush failed):" << file_name);
    f_.close();
    return make_error(ec::cannot_write_file, file_name);
  }
  file_name_ = std::move(file_name);
  return caf::none;
}

caf::error meta_data_file_writer::flush() {
  if (!f_.is_open() || buf_.empty())
    return caf::none;
  if (!f_.write(buf_.data(), buf_.size()))
    return make_error(ec::cannot_write_file, file_name_);
  buf_.clear();
  return caf::none;
}

caf::error meta_data_file_writer::write(const data_message& x) {
  uint16_t tid;
  if (auto err = topic_id(get_topic(x), tid))
    return err;
  auto entry = format::entry_type::data_message;
  if (auto err = sink_(entry, tid))
    return err;
  if (auto err = writer_(get_data(x)))
    return err;
  if (buf_.size() >= flush_threshold())
    return flush();
  return caf::none;
}

caf::error meta_data_file_writer::topic_id(const topic& x, uint16_t& id) {
  auto e = topic_table_.end();
  auto i = std::find(topic_table_.begin(), e, x);
  if (i == e) {
    // Write the new topic to file first.
    auto entry = format::entry_type::new_topic;
    if (auto err = sink_(entry, x.string()))
      return err;
    id = static_cast<uint16_t>(topic_table_.size());
    topic_table_.emplace_back(x);
    return caf::none;
  }
  id = static_cast<uint16_t>(std::distance(topic_table_.begin(), i));
  return caf::none;
}

bool meta_data_file_writer::operator!() const{
  return !f_;
}

meta_data_file_writer::operator bool() const {
  return static_cast<bool>(f_);
}

meta_data_file_writer_ptr make_meta_data_file_writer(const std::string& fname) {
  meta_data_file_writer_ptr result{new meta_data_file_writer};
  if (result->open(fname) != caf::none)
    return nullptr;
  return result;
}

meta_data_file_writer& operator<<(meta_data_file_writer& out,
                                  const data_message& x) {
  out.write(x);
  return out;
}

} // namespace detail
} // namespace broker
