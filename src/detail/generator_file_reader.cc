#include "broker/detail/generator_file_reader.hh"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>

#include <caf/detail/scope_guard.hpp>
#include <caf/error.hpp>
#include <caf/none.hpp>

#include "broker/detail/meta_data_file_writer.hh"
#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker {
namespace detail {

generator_file_reader::generator_file_reader(int fd, void* addr,
                                             size_t file_size)
  : fd_(fd),
    addr_(addr),
    file_size_(file_size),
    source_(nullptr, reinterpret_cast<char*>(addr), file_size),
    generator_(source_) {
  // We've already verified the file header in make_generator_file_reader.
  source_.skip(sizeof(meta_data_file_writer::format::magic)
               + sizeof(meta_data_file_writer::format::version));
}

generator_file_reader::~generator_file_reader() {
  munmap(addr_, file_size_);
  close(fd_);
}

bool generator_file_reader::at_end() const {
  return source_.remaining() == 0;
}

caf::error generator_file_reader::read(value_type& x) {
  if (at_end())
    return ec::end_of_file;
  using entry_type = meta_data_file_writer::format::entry_type;
  // Read until we got a data_message, a command_message, or an error.
  for (;;) {
    uint8_t entry = 0;
    if (auto err = source_(entry))
      return err;
    switch (static_cast<entry_type>(entry)) {
      case entry_type::new_topic:{
        std::string str;
        if (auto err = source_(str))
          return err;
        topic_table_.emplace_back(str);
        break;
      }
      case entry_type::data_message: {
        uint16_t topic_id;
        data value;
        if (auto err = source_(topic_id))
          return err;
        if (topic_id >= topic_table_.size())
          return ec::invalid_topic_key;
        if (auto err = generator_(value))
          return err;
        x = make_data_message(topic_table_[topic_id], std::move(value));
        return caf::none;
      }
      case entry_type::command_message: {
        // TODO: implement me
        throw std::runtime_error("unimplemented");
      }
    }
  }
}

generator_file_reader_ptr make_generator_file_reader(const std::string& fname) {
  // Get a file handle for the file.
  auto fd = open(fname.c_str(), O_RDONLY);
  if (fd == -1) {
    BROKER_ERROR("unable to open file:" << fname);
    return nullptr;
  }
  auto guard1 = caf::detail::make_scope_guard([&] { close(fd); });
  // Read the file size.
  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    BROKER_ERROR("unable to read file size (fstat failed):" << fname);
    return nullptr;
  }
  // Read and verify file size.
  auto file_size = static_cast<size_t>(sb.st_size);
  if (file_size < sizeof(meta_data_file_writer::format::header_size)) {
    BROKER_ERROR("cannot read file header (file too small):" << fname);
    return nullptr;
  }
  // Memory map file.
  auto addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (addr == nullptr){
    BROKER_ERROR("unable to open file (mmap failed):" << fname);
    return nullptr;
  }
  auto guard2 = caf::detail::make_scope_guard([&] { munmap(addr, file_size); });
  // Verify file header (magic number + version).
  uint32_t magic = 0;
  uint8_t version = 0;
  memcpy(&magic, addr, sizeof(magic));
  memcpy(&version, reinterpret_cast<char*>(addr) + sizeof(magic),
         sizeof(version));
  if (magic != meta_data_file_writer::format::magic) {
    BROKER_ERROR("unexpected file header (magic mismatch):" << fname);
    return nullptr;
  }
  if (version != meta_data_file_writer::format::version) {
    BROKER_ERROR("unexpected file header (version mismatch):" << fname);
    return nullptr;
  }
  // Done.
  auto ptr = new generator_file_reader(fd, addr, file_size);
  guard1.disable();
  guard2.disable();
  return generator_file_reader_ptr{ptr};
}

} // namespace detail
} // namespace broker
