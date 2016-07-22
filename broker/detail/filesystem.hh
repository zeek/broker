#ifndef BROKER_DETAIL_FILESYSTEM_HH
#define BROKER_DETAIL_FILESYSTEM_HH

#include <string>

namespace broker {
namespace detail {

using path = std::string;

/// Checks whether a given filename exists.
/// @param p The path to examine.
/// @returns `true` if the given path or file status corresponds to an existing
/// file or directory, `false` otherwise.
bool exists(const path& p);

/// Removes a file or empty directory.
/// @param p The path to remove.
/// @returns `true` if *p* was deleted, `false` if *p* did not exist.
bool remove(const path& p);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_FILE_SYSTEM_HH
