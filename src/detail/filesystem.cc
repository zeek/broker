#include <sys/stat.h>

#include "broker/detail/filesystem.hh"

namespace broker {
namespace detail {

bool exists(const path& p) {
  struct stat st;
  return ::lstat(p.c_str(), &st) == 0;
}

bool remove(const path& p) {
  return ::unlink(p.c_str()) == 0;
}

} // namespace detail
} // namespace broker
