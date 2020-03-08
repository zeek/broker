#include "broker/detail/generator_file_reader.hh"

#include <cstdio>
#include <cstdlib>

#include <caf/byte.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/error.hpp>
#include <caf/none.hpp>

#include "broker/config.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/generator_file_writer.hh"
#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

#ifdef BROKER_WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif // WIN32_LEAN_AND_MEAN

#ifndef NOMINMAX
#define NOMINMAX
#endif // NOMINMAX

#include <Windows.h>

namespace {

std::pair<HANDLE, bool> open_file(const char* fname) {
  auto hdl = CreateFile(fname, GENERIC_READ, 0, nullptr, OPEN_EXISTING,
                        FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
  return {hdl, hdl != nullptr};
}

std::pair<size_t, bool> file_size(HANDLE fd) {
  LARGE_INTEGER result;
  if (!GetFileSizeEx(fd, &result))
    return {0, false};
  return {static_cast<size_t>(result.QuadPart), true};
}

void close_file(HANDLE fd) {
  CloseHandle(fd);
}

void* memory_map_file(HANDLE fd, size_t) {
  return CreateFileMapping(fd, nullptr, PAGE_READONLY, 0, 0, nullptr);
}

void unmap_file(void* addr, size_t) {
  UnmapViewOfFile(addr);
}

void* make_file_view(void* mapper, size_t file_size) {
  return MapViewOfFile(mapper, FILE_MAP_READ, 0, 0, file_size);
}

} // namespace

#else // BROKER_WINDOWS

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

std::pair<int, bool> open_file(const char* fname) {
  auto result = open(fname, O_RDONLY);
  return {result, result != -1};
}

std::pair<size_t, bool> file_size(int fd) {
  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    return {0, false};
  }
  return {static_cast<size_t>(sb.st_size), true};
}

void close_file(int fd) {
  close(fd);
}

void* memory_map_file(int fd, size_t file_size) {
  return mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
}

void unmap_file(void* addr, size_t file_size) {
  munmap(addr, file_size);
}

void* make_file_view(void* addr, size_t) {
  // On POSIX, mmap() returns the mapped region directly.
  return addr;
}

} // namespace

#endif // BROKER_WINDOWS

namespace broker::detail {

generator_file_reader::generator_file_reader(file_handle fd,
                                             mapper_handle mapper,
                                             mapped_pointer addr,
                                             size_t file_size)
  : fd_(fd),
    mapper_(mapper),
    addr_(addr),
    file_size_(file_size),
    source_(nullptr,
            caf::make_span(reinterpret_cast<caf::byte*>(addr), file_size)),
    generator_(source_) {
  // We've already verified the file header in make_generator_file_reader.
  source_.skip(sizeof(generator_file_writer::format::magic)
               + sizeof(generator_file_writer::format::version));
}

generator_file_reader::~generator_file_reader() {
  unmap_file(addr_, file_size_);
  close_file(fd_);
}

bool generator_file_reader::at_end() const {
  return source_.remaining() == 0;
}

void generator_file_reader::rewind() {
  BROKER_ASSERT(at_end());
  sealed_ = true;
  source_.reset({reinterpret_cast<caf::byte*>(addr_), file_size_});
  source_.skip(sizeof(generator_file_writer::format::magic)
               + sizeof(generator_file_writer::format::version));
}

caf::error generator_file_reader::read(value_type& x) {
  if (at_end())
    return ec::end_of_file;
  using entry_type = generator_file_writer::format::entry_type;
  // Read until we got a data_message, a command_message, or an error.
  for (;;) {
    entry_type entry{};
    BROKER_TRY(source_(entry));
    switch (entry) {
      case entry_type::new_topic: {
        std::string str;
        BROKER_TRY(source_(str));
        if (!sealed_)
          topic_table_.emplace_back(str);
        break;
      }
      case entry_type::data_message: {
        uint16_t topic_id;
        BROKER_TRY(source_(topic_id));
        if (topic_id >= topic_table_.size())
          return ec::invalid_topic_key;
        data value;
        BROKER_TRY(generator_(value));
        x = make_data_message(topic_table_[topic_id], std::move(value));
        if (!sealed_)
          ++data_entries_;
        return caf::none;
      }
      case entry_type::command_message: {
        uint16_t topic_id;
        BROKER_TRY(source_(topic_id));
        if (topic_id >= topic_table_.size())
          return ec::invalid_topic_key;
        internal_command cmd;
        BROKER_TRY(generator_(cmd));
        x = make_command_message(topic_table_[topic_id], std::move(cmd));
        if (!sealed_)
          ++command_entries_;
        return caf::none;
      }
    }
  }
}

caf::error generator_file_reader::skip() {
  if (at_end())
    return ec::end_of_file;
  value_type tmp;
  return read(tmp);
}

caf::error generator_file_reader::skip_to_end() {
  while (!at_end())
    if (auto err = skip())
      return err;
  return caf::none;
}

generator_file_reader_ptr make_generator_file_reader(const std::string& fname) {
  // Get a file handle for the file.
  auto [fd, fd_ok] = open_file(fname.c_str());
  if (!fd_ok) {
    BROKER_ERROR("unable to open file:" << fname);
    return nullptr;
  }
  auto guard1 = caf::detail::make_scope_guard([fd = fd] { close_file(fd); });
  // Read the file size.
  auto [fsize, fsize_ok] = file_size(fd);
  if (!fsize_ok) {
    BROKER_ERROR("unable to read file size (fstat failed):" << fname);
    return nullptr;
  }
  // Read and verify file size.
  if (fsize < generator_file_writer::format::header_size) {
    BROKER_ERROR("cannot read file header (file too small):" << fname);
    return nullptr;
  }
  // Memory map file.
  auto mapper = memory_map_file(fd, fsize);
  if (mapper == nullptr) {
    BROKER_ERROR("unable to open file (mmap failed):" << fname);
    return nullptr;
  }
  auto cleanup = [mapper, fsize = fsize] { unmap_file(mapper, fsize); };
  auto guard2 = caf::detail::make_scope_guard(cleanup);
  // Create a view into the mapped file.
  auto addr = make_file_view(mapper, fsize);
  if (addr == nullptr) {
    BROKER_ERROR("unable to create view into the mapped file:" << fname);
    return nullptr;
  }
  // Verify file header (magic number + version).
  uint32_t magic = 0;
  uint8_t version = 0;
  memcpy(&magic, addr, sizeof(magic));
  memcpy(&version, reinterpret_cast<char*>(addr) + sizeof(magic),
         sizeof(version));
  if (magic != generator_file_writer::format::magic) {
    BROKER_ERROR("unexpected file header (magic mismatch):" << fname);
    return nullptr;
  }
  if (version != generator_file_writer::format::version) {
    BROKER_ERROR("unexpected file header (version mismatch):" << fname);
    return nullptr;
  }
  // Done.
  auto ptr = new generator_file_reader(fd, mapper, addr, fsize);
  guard1.disable();
  guard2.disable();
  return generator_file_reader_ptr{ptr};
}

} // namespace broker::detail
