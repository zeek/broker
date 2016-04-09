#include <algorithm>
#include <cassert>

#include "persist.hh"

static constexpr bool is_big_endian() {
#ifdef BROKER_BIG_ENDIAN
  return true;
#else
  return false;
#endif
}

namespace broker {
namespace util {
namespace persist {

save_archive::save_archive(std::string arg_serial)
  : serial_{std::move(arg_serial)} {
}

void save_archive::reset(std::string arg_serial) {
  serial_ = std::move(arg_serial);
  version_map_.clear();
}

std::string save_archive::get() {
  auto rval = std::move(serial_);
  reset();
  return rval;
}

void save_archive::save_bytes(const uint8_t* bytes, size_t size) {
  serial_.reserve(serial_.size() + size);
  std::copy(bytes, bytes + size, std::back_inserter(serial_));
}

void save_archive::save_bytes_reverse(const uint8_t* bytes, size_t size) {
  serial_.reserve(serial_.size() + size);
  std::reverse_copy(bytes, bytes + size, std::back_inserter(serial_));
}

void save_archive::save_value(const uint8_t* bytes, size_t size) {
  if (is_big_endian())
    save_bytes_reverse(bytes, size);
  else
    save_bytes(bytes, size);
}

uint32_t save_archive::register_class(const std::type_info& ti, uint32_t ver) {
  auto res = version_map_.emplace(std::type_index(ti), ver);
  if (res.second)
    save(*this, ver);
  return ver;
}

save_archive& save_binary(save_archive& ar, const void* bytes, size_t size) {
  auto num_bytes = static_cast<uint64_t>(size);
  save(ar, num_bytes);
  ar.save_bytes(reinterpret_cast<const uint8_t*>(bytes), size);
  return ar;
}

save_archive& save_sequence(save_archive& ar, size_t size) {
  auto num_items = static_cast<uint64_t>(size);
  save(ar, num_items);
  return ar;
}


load_archive::load_archive(const void* bytes, size_t num_bytes)
  : serial_bytes_{bytes},
    num_bytes_{num_bytes} {
}

void load_archive::reset(const void* bytes, size_t num_bytes) {
  serial_bytes_ = bytes;
  num_bytes_ = num_bytes;
  position_ = 0;
  version_map_.clear();
}

load_archive& load_binary(load_archive& ar, std::string* rval) {
  auto size = ar.load_value<uint64_t>();
  assert(ar.position_ + size <= ar.num_bytes_);
  rval->reserve(rval->size() + size);
  auto src = reinterpret_cast<const uint8_t*>(ar.serial_bytes_);
  src += ar.position_;
  std::copy(src, src + size, std::back_inserter(*rval));
  ar.position_ += size;
  return ar;
}

uint64_t load_sequence(load_archive& ar) {
  uint64_t rval;
  load(ar, &rval);
  return rval;
}

void load_archive::load_value(uint8_t* dst, size_t size) {
  assert(position_ + size <= num_bytes_);
  auto src = reinterpret_cast<const uint8_t*>(serial_bytes_);
  src += position_;
  if (is_big_endian())
    std::reverse_copy(src, src + size, dst);
  else
    std::copy(src, src + size, dst);
  position_ += size;
}

uint32_t load_archive::register_class(const std::type_info& ti) {
  auto it = version_map_.find(std::type_index(ti));
  if (it == version_map_.end()) {
    auto rval = load_value<uint32_t>();
    version_map_[std::type_index(ti)] = rval;
    return rval;
  } else
    return it->second;
}

} // namespace persist
} // namespace util
} // namespace broker
