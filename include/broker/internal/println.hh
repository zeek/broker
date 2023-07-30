#pragma once

#include "broker/convert.hh"

#include <caf/deep_to_string.hpp>
#include <caf/term.hpp>

#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>

namespace broker::internal {

std::mutex& println_mtx();

inline void do_print(std::ostream& ostr, const char* x) {
  ostr << x;
}

inline void do_print(std::ostream& ostr, std::string_view x) {
  ostr << x;
}

inline void do_print(std::ostream& ostr, const caf::term& x) {
  ostr << x;
}

template <class T>
void do_print(std::ostream& ostr, const T& x) {
  if constexpr (detail::has_convert_v<T, std::string>) {
    std::string tmp;
    convert(x, tmp);
    do_print(ostr, tmp);
  } else {
    auto tmp = caf::deep_to_string(x);
    do_print(ostr, tmp);
  }
}

template <class... Ts>
void do_print_all(std::ostream& ostr, const Ts&... xs) {
  (do_print(ostr, xs), ...);
}

} // namespace broker::internal

namespace broker::internal::out {

/// Prints a sequence of values to standard output.
template <class... Ts>
void println(Ts&&... xs) {
  std::unique_lock guard{println_mtx()};
  do_print_all(std::cout, std::forward<Ts>(xs)...);
  std::cout << caf::term::reset_endl;
}

} // namespace broker::internal::out

namespace broker::internal::err {

/// Prints a sequence of values to standard error.
template <class... Ts>
void println(Ts&&... xs) {
  std::unique_lock guard{println_mtx()};
  do_print_all(std::cerr, caf::term::red, std::forward<Ts>(xs)...);
  std::cerr << caf::term::reset_endl;
}

} // namespace broker::internal::err

namespace broker::internal::verbose {

bool enabled();

void enabled(bool value);

/// Prints a sequence of values to standard output if verbose logging is
/// enabled.
template <class... Ts>
void println(Ts&&... xs) {
  if (!enabled())
    return;
  std::unique_lock guard{println_mtx()};
  do_print_all(std::cout, caf::term::blue, std::chrono::system_clock::now(),
               ": ", std::forward<Ts>(xs)...);
  std::cout << caf::term::reset_endl;
}

} // namespace broker::internal::verbose
