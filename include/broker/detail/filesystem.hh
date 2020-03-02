#pragma once

#include <string>
#include <vector>

#include "broker/config.hh"

#ifdef BROKER_HAS_STD_FILESYSTEM

#include <filesystem>

namespace broker::detail {

using path = std::filesystem::path;

using std::filesystem::exists;

using std::filesystem::is_directory;

inline bool is_file(const path& p) {
  return std::filesystem::is_regular_file(p);
}

inline bool mkdirs(const path& p) {
  return std::filesystem::create_directories(p);
}

inline path dirname(path p) {
  p.remove_filename();
  return p;
}

using std::filesystem::remove;

inline bool remove_all(const path& p) {
  std::error_code ec;
  std::filesystem::remove_all(p, ec);
  return static_cast<bool>(ec);
}

} // namespace broker::detail

#else // BROKER_HAS_STD_FILESYSTEM

namespace broker::detail {

using path = std::string;

/// Checks whether a given filename exists.
/// @param p The path to examine.
/// @returns `true` if the given path or file status corresponds to an existing
/// file or directory, `false` otherwise.
bool exists(const path& p);

/// Checks whether a given path exists and is a directory.
/// @param p The path to examine.
/// @returns `true` if the given path or file status corresponds to an existing
///          directory, `false` otherwise.
bool is_directory(const path& p);

/// Checks whether a given path exists and is a file.
/// @param p The path to examine.
/// @returns `true` if the given path or file status corresponds to an existing
///          file, `false` otherwise.
bool is_file(const path& p);

/// Like `mkdir -p`.
/// @param p The director to create.
/// @returns `false` if the given path cannot be created, else `true`.
bool mkdirs(const path& p);

/// Returns the parent directory of a path.
/// @param p The path whose directory you want.
/// @returns the parent directory of the path.
path dirname(const path& p);

/// Removes a file or empty directory.
/// @param p The path to remove.
/// @returns `true` iff *p* was deleted successfully.
bool remove(const path& p);

/// Deletes the contents of a path (if it is a directory) and the contents of
/// all its subdirectories, recursively, then deletes the path itself as if by
/// repeatedly applying the POSIX remove.
/// @param p The path to remove.
/// @returns `true` iff *p* was deleted successfully.
bool remove_all(const path& p);

} // namespace broker::detail

#endif // BROKER_HAS_STD_FILESYSTEM

namespace broker::detail {

/// Reads an entire file and returns its contents as list of lines.
/// @param p The path to read.
/// @param keep_empties Configures whether to drop empty lines.
/// @returns a `std::vector` containing one string for each line in the file.
std::vector<std::string> readlines(const path& p, bool keep_empties = true);

/// Reads an entire file and returns its contents as string.
/// @param p The path to read.
/// @returns a string containing the content of the file.
std::string read(const path& p);

/// Generates a path to a unique temporary file.
std::string make_temp_file_name();

} // namespace broker::detail
