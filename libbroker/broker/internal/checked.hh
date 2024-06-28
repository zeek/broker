#pragma once

#include <stdexcept>

namespace broker::internal {

// Helper function to check a nullable value and throw an exception if it is
// `nullptr` / `nullopt`. This function is used to simplify error handling in
// the constructor when initializing member functions.
template <class Nullable>
auto checked(Nullable what, const char* msg) {
  if (!what)
    throw std::logic_error(msg);
  return what;
}

template <class Nullable>
auto& checked_deref(Nullable& what, const char* msg) {
  if (!what)
    throw std::logic_error(msg);
  return *what;
}

} // namespace broker::internal
