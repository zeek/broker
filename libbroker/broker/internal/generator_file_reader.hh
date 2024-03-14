#pragma once

#include <cstddef>
#include <cstdio>
#include <functional>
#include <memory>
#include <variant>

#include <caf/binary_deserializer.hpp>

#include "broker/config.hh"
#include "broker/detail/native_socket.hh"
#include "broker/fwd.hh"
#include "broker/internal/data_generator.hh"
#include "broker/topic.hh"

namespace broker::internal {

class generator_file_reader {
public:
  using value_type = std::variant<data_message, command_message>;

  using mapper_handle = void*;

  using mapped_pointer = void*;

#ifdef BROKER_WINDOWS
  using file_handle_type = void*;
#else
  using file_handle_type = detail::native_socket;
#endif

  using read_raw_callback =
    std::function<bool(value_type*, caf::span<const caf::byte>)>;

  generator_file_reader(file_handle_type fd, mapper_handle mapper,
                        mapped_pointer addr, size_t file_size);

  generator_file_reader(generator_file_reader&&) = delete;

  generator_file_reader(const generator_file_reader&) = delete;

  generator_file_reader& operator=(generator_file_reader&&) = delete;

  generator_file_reader& operator=(const generator_file_reader&) = delete;

  ~generator_file_reader();

  bool at_end() const;

  /// @pre `at_end()`
  void rewind();

  caf::error read(value_type& x);

  /// Reads from the input until an error occurs, reaching the end of the input,
  /// or the callback returns `false`.
  caf::error read_raw(read_raw_callback f);

  caf::error skip();

  caf::error skip_to_end();

  const std::vector<topic>& topics() const noexcept {
    return topic_table_;
  }

  size_t entries() const noexcept {
    return data_entries_ + command_entries_;
  }

  size_t data_entries() const noexcept {
    return data_entries_;
  }

  size_t command_entries() const noexcept {
    return command_entries_;
  }

private:
  file_handle_type fd_;
  mapper_handle mapper_;
  mapped_pointer addr_;
  size_t file_size_;
  caf::binary_deserializer source_;
  data_generator generator_;
  std::vector<topic> topic_table_;
  size_t data_entries_ = 0;
  size_t command_entries_ = 0;
  bool sealed_ = false;
};

using generator_file_reader_ptr = std::unique_ptr<generator_file_reader>;

generator_file_reader_ptr make_generator_file_reader(const std::string& fname);

} // namespace broker::internal
