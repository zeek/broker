#ifndef BROKER_DETAIL_GENERATOR_FILE_READER_HH
#define BROKER_DETAIL_GENERATOR_FILE_READER_HH

#include <memory>
#include <cstddef>

#include <caf/binary_deserializer.hpp>

#include "broker/detail/data_generator.hh"
#include "broker/fwd.hh"

namespace broker {
namespace detail {

class generator_file_reader {
public:
  generator_file_reader(int fd, void* addr, size_t file_size);

  generator_file_reader(generator_file_reader&&) = delete;

  generator_file_reader(const generator_file_reader&) = delete;

  generator_file_reader& operator=(generator_file_reader&&) = delete;

  generator_file_reader& operator=(const generator_file_reader&) = delete;

  ~generator_file_reader();

  bool at_end() const;

  bool read(data& x);

private:
  int fd_;
  void* addr_;
  size_t file_size_;
  caf::binary_deserializer source_;
  data_generator generator_;
};

using generator_file_reader_ptr = std::unique_ptr<generator_file_reader>;

generator_file_reader_ptr make_generator_file_reader(const std::string& fname);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_GENERATOR_FILE_READER_HH
