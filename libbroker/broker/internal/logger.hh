#pragma once

#include <caf/logger.hpp>

#define BROKER_LOG(level, ...) CAF_LOG_IMPL("broker", level, __VA_ARGS__)

#define BROKER_TRACE(...)                                                      \
  BROKER_LOG(CAF_LOG_LEVEL_TRACE, "ENTRY" << __VA_ARGS__);                     \
  auto CAF_UNIFYN(broker_log_trace_guard_) = ::caf::detail::make_scope_guard(  \
    [=] { BROKER_LOG(CAF_LOG_LEVEL_TRACE, "EXIT"); })

#define BROKER_DEBUG(...) BROKER_LOG(CAF_LOG_LEVEL_DEBUG, __VA_ARGS__)

#define BROKER_INFO(...) BROKER_LOG(CAF_LOG_LEVEL_INFO, __VA_ARGS__)

#define BROKER_WARNING(...) BROKER_LOG(CAF_LOG_LEVEL_WARNING, __VA_ARGS__)

#define BROKER_ERROR(...) BROKER_LOG(CAF_LOG_LEVEL_ERROR, __VA_ARGS__)

#define BROKER_ARG CAF_ARG

#define BROKER_ARG2 CAF_ARG2

#define BROKER_ARG3 CAF_ARG3

#include "broker/convert.hh"
#include "broker/event.hh"
#include "broker/event_observer.hh"
#include "broker/logger.hh"

#include <type_traits>

namespace broker::internal {

// -- poor man's std::format replacement; remove once we can use C++20 ---------

template <class OutputIterator>
OutputIterator fmt_to(OutputIterator out, std::string_view fmt) {
  return std::copy(fmt.begin(), fmt.end(), out);
}

template <class OutputIterator, class T, class... Ts>
OutputIterator fmt_to(OutputIterator out, std::string_view fmt, const T& arg,
                      const Ts&... args) {
  if (fmt.empty())
    return out;
  if (fmt.size() == 1) {
    *out++ = fmt[0];
    return out;
  }
  auto index = size_t{0};
  auto ch = fmt[index];
  auto lookahead = fmt[index + 1];
  auto next = [&] {
    ch = lookahead;
    ++index;
    if (index + 1 < fmt.size())
      lookahead = fmt[index + 1];
    else
      lookahead = '\0';
  };
  while (index < fmt.size()) {
    switch (ch){
      // Must be "{}" (placeholder) or "{{" (escaped '{').
      case '{':
        if (lookahead == '{') {
          *out++ = '{';
          next(); // consume two characters
          break;
        }
        if (lookahead == '}') {
          if constexpr (std::is_arithmetic_v<T>) {
            auto str = std::to_string(arg);
            out = std::copy(str.begin(), str.end(), out);
          } else if constexpr (std::is_same_v<T, std::string>
                               || std::is_same_v<T, std::string_view>) {
            out = std::copy(arg.begin(), arg.end(), out);
          } else if constexpr (detail::has_convert_v<T, std::string>) {
            auto str = std::string{};
            convert(arg, str);
            out = std::copy(str.begin(), str.end(), out);
          } else if constexpr (std::is_same_v<T, caf::error>) {
            auto str = caf::to_string(arg);
            out = std::copy(str.begin(), str.end(), out);
          } else {
            static_assert(std::is_convertible_v<T, const char*>);
            for (auto cstr = arg; *cstr != '\0'; ++cstr)
              *out++ = *cstr;
          }
          return fmt_to(out, fmt.substr(index + 2), args...);
        }
        throw std::invalid_argument("invalid format string");
      // Must be "}}" (escaped '}'), because the use as placeholder was
      // handled in the previous case.
      case '}':
        if (lookahead == '}') {
          *out++ = '}';
          next(); // consume two characters
          break;
        }
        throw std::invalid_argument("invalid format string");
      // Other characters are copied verbatim.
      default:
        *out++ = ch;
        break;
    }
    next();
  }
  throw std::invalid_argument("format string ended unexpectedly");
}

template <class... Ts>
void do_log(event::severity_level level, event::component_type component,
            std::string_view identifier, std::string_view fmt_str,
            Ts&&... args) {
  auto lptr = logger();
  if (!lptr || !lptr->accepts(level, component)) {
    // Short-circuit if no observer is interested in the event.
    return;
  }
  auto msg = std::string{};
  msg.reserve(fmt_str.size() + sizeof...(Ts) * 8);
  fmt_to(std::back_inserter(msg), fmt_str, std::forward<Ts>(args)...);
  lptr->observe(std::make_shared<event>(event::severity_level::debug, component,
                                        identifier, std::move(msg)));
}

} // namespace broker::internal

/// Generates functions for logging messages with a specific component type.
#define BROKER_DECLARE_LOG_COMPONENT(name)                                     \
  namespace broker::internal::log::name {                                      \
  constexpr auto component = event::component_type::name;                      \
  template <class... Ts>                                                       \
  void critical(std::string_view identifier, std::string_view fmt_str,         \
                Ts&&... args) {                                                \
    do_log(event::severity_level::critical, component, identifier, fmt_str,    \
           std::forward<Ts>(args)...);                                         \
  }                                                                            \
  template <class... Ts>                                                       \
  void error(std::string_view identifier, std::string_view fmt_str,            \
             Ts&&... args) {                                                   \
    do_log(event::severity_level::error, component, identifier, fmt_str,       \
           std::forward<Ts>(args)...);                                         \
  }                                                                            \
  template <class... Ts>                                                       \
  void warning(std::string_view identifier, std::string_view fmt_str,          \
               Ts&&... args) {                                                 \
    do_log(event::severity_level::warning, component, identifier, fmt_str,     \
           std::forward<Ts>(args)...);                                         \
  }                                                                            \
  template <class... Ts>                                                       \
  void info(std::string_view identifier, std::string_view fmt_str,             \
            Ts&&... args) {                                                    \
    do_log(event::severity_level::info, component, identifier, fmt_str,        \
           std::forward<Ts>(args)...);                                         \
  }                                                                            \
  template <class... Ts>                                                       \
  void debug(std::string_view identifier, std::string_view fmt_str,            \
             Ts&&... args) {                                                   \
    do_log(event::severity_level::debug, component, identifier, fmt_str,       \
           std::forward<Ts>(args)...);                                         \
  }                                                                            \
  }

BROKER_DECLARE_LOG_COMPONENT(core)

#undef BROKER_DECLARE_LOG_COMPONENT
