#include <sys/stat.h>
#include <sys/syslimits.h>
#include <ftw.h>

#include "broker/detail/filesystem.hh"

namespace broker {
namespace detail {

bool exists(const path& p) {
  struct stat st;
  return ::lstat(p.c_str(), &st) == 0;
}

bool remove(const path& p) {
  return remove_all(p); // lazy way out
}

namespace {

int rm(const char* path, const struct stat*, int, FTW*) {
  return ::remove(path);
}

} // namespace <anonymous>

bool remove_all(const path& p) {
  struct stat st;
  if (::lstat(p.c_str(), &st) != 0)
    return false;
  if (S_ISDIR(st.st_mode))
    return ::nftw(p.c_str(), rm, OPEN_MAX, FTW_DEPTH | FTW_PHYS) == 0;
  else 
    return ::remove(p.c_str()) == 0;
}

} // namespace detail
} // namespace broker
