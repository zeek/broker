#include "broker/detail/meta_data_file_writer.hh"

#include <caf/error.hpp>
#include <caf/sec.hpp>

#include "broker/logger.hh"

namespace broker {
namespace detail {

meta_data_file_writer::meta_data_file_writer()
  : sink_(nullptr, buf_), writer_(sink_), flush_threshold_(1024) {
  // nop
}

meta_data_file_writer::~meta_data_file_writer() {
  flush();
}

caf::error meta_data_file_writer::open(const std::string& file_name) {
  flush();
  f_.open(file_name, std::ofstream::binary);
  if (!f_.is_open())
    return caf::sec::cannot_open_file;
  auto magic = format::magic;
  auto version = format::version;
  char header[sizeof(magic) + sizeof(version)];
  memcpy(header, &magic, sizeof(magic));
  memcpy(header + sizeof(magic), &version, sizeof(version));
  if (!f_.write(header, sizeof(header))) {
    BROKER_ERROR("unable to write to file:" << file_name);
    f_.close();
    return caf::sec::cannot_open_file;
  }
  if (!f_.flush()) {
    BROKER_ERROR("unable to write to file (flush failed):" << file_name);
    f_.close();
    return caf::sec::cannot_open_file;
  }
  return caf::none;
}

void meta_data_file_writer::flush() {
  if (!f_.is_open() || buf_.empty())
    return;
  f_.write(buf_.data(), buf_.size());
  buf_.clear();
}

void meta_data_file_writer::write(const data& x) {
  writer_(x);
  if (buf_.size() >= flush_threshold())
    flush();
}

meta_data_file_writer_ptr make_meta_data_file_writer(const std::string& fname) {
  meta_data_file_writer_ptr result{new meta_data_file_writer};
  if (result->open(fname) != caf::none)
    return nullptr;
  return result;
}

meta_data_file_writer& operator<<(meta_data_file_writer& out, const data& x) {
  out.write(x);
  return out;
}

} // namespace detail
} // namespace broker
