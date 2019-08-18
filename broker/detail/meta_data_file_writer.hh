#ifndef BROKER_DETAIL_META_DATA_FILE_WRITER_HH
#define BROKER_DETAIL_META_DATA_FILE_WRITER_HH

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <caf/binary_serializer.hpp>
#include <caf/fwd.hpp>

#include "broker/detail/meta_data_writer.hh"
#include "broker/fwd.hh"

namespace broker {
namespace detail {

class meta_data_file_writer {
public:
  struct format {
    static constexpr uint32_t magic = 0x2EECC0DE;

    static constexpr uint8_t version = 1;

    static constexpr size_t header_size = sizeof(magic) + sizeof(version);
  };

  meta_data_file_writer();

  meta_data_file_writer(meta_data_file_writer&&) = delete;

  meta_data_file_writer(const meta_data_file_writer&) = delete;

  meta_data_file_writer& operator=(meta_data_file_writer&&) = delete;

  meta_data_file_writer& operator=(const meta_data_file_writer&) = delete;

  ~meta_data_file_writer();

  caf::error open(const std::string& file_name);

  void write(const data& x);

  void flush();

  size_t flush_threshold() const noexcept {
    return flush_threshold_;
  }

  void flush_threshold(size_t x) noexcept {
    flush_threshold_ = x;
  }

private:
  std::vector<char> buf_;
  caf::binary_serializer sink_;
  meta_data_writer writer_;
  std::ofstream f_;
  size_t flush_threshold_;
};

using meta_data_file_writer_ptr = std::unique_ptr<meta_data_file_writer>;

meta_data_file_writer_ptr make_meta_data_file_writer(const std::string& fname);

meta_data_file_writer& operator<<(meta_data_file_writer& out, const data& x);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_META_DATA_FILE_WRITER_HH
