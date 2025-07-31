#pragma once

#include "broker/event.hh"
#include "broker/event_observer.hh"

#include <format>
#include <string_view>

namespace broker {

/// Returns the global logger.
event_observer* logger() noexcept;

/// Sets the global logger.
/// @note This function is not thread-safe and should be called only once at
///       program startup.
void logger(event_observer_ptr ptr) noexcept;

/// Creates a new logger that prints to the console.
event_observer_ptr
make_console_logger(event::severity_level severity,
                    event::component_mask mask = event::default_component_mask);

/// Creates a new logger that prints to the console.
/// @param severity The minimum severity level to log as string. One of:
///                 "critical", "error", "warning", "info", or "debug".
/// @throws std::invalid_argument if `severity` is not a valid severity level.
event_observer_ptr
make_console_logger(std::string_view severity,
                    event::component_mask mask = event::default_component_mask);

/// Convenience function for setting a console logger.
inline void
set_console_logger(event::severity_level severity,
                   event::component_mask mask = event::default_component_mask) {
  logger(make_console_logger(severity, mask));
}

/// Convenience function for setting a console logger.
inline void
set_console_logger(std::string_view severity,
                   event::component_mask mask = event::default_component_mask) {
  logger(make_console_logger(severity, mask));
}

} // namespace broker

namespace broker::detail {

template <class... Ts>
void do_log(event::severity_level level, event::component_type component,
            std::string_view identifier, std::format_string<Ts...> fmt_str,
            Ts&&... args) {
  auto lptr = logger();
  if (!lptr || !lptr->accepts(level, component)) {
    // Short-circuit if no observer is interested in the event.
    return;
  }
  auto msg = std::format(fmt_str, std::forward<Ts>(args)...);
  auto ev = std::make_shared<event>(level, component, identifier,
                                    std::move(msg));
  lptr->observe(std::move(ev));
}

} // namespace broker::detail

/// Generates functions for logging messages with a specific component type.
#define BROKER_DECLARE_LOG_COMPONENT(name)                                     \
  namespace broker::log::name {                                                \
  constexpr auto component = event::component_type::name;                      \
  template <class... Ts>                                                       \
  void critical(std::string_view identifier,                                   \
                std::format_string<Ts...> fmt_str, Ts&&... args) {             \
    detail::do_log(event::severity_level::critical, component, identifier,     \
                   fmt_str, std::forward<Ts>(args)...);                        \
  }                                                                            \
  template <class... Ts>                                                       \
  void error(std::string_view identifier, std::format_string<Ts...> fmt_str,   \
             Ts&&... args) {                                                   \
    detail::do_log(event::severity_level::error, component, identifier,        \
                   fmt_str, std::forward<Ts>(args)...);                        \
  }                                                                            \
  template <class... Ts>                                                       \
  void warning(std::string_view identifier, std::format_string<Ts...> fmt_str, \
               Ts&&... args) {                                                 \
    detail::do_log(event::severity_level::warning, component, identifier,      \
                   fmt_str, std::forward<Ts>(args)...);                        \
  }                                                                            \
  template <class... Ts>                                                       \
  void info(std::string_view identifier, std::format_string<Ts...> fmt_str,    \
            Ts&&... args) {                                                    \
    detail::do_log(event::severity_level::info, component, identifier,         \
                   fmt_str, std::forward<Ts>(args)...);                        \
  }                                                                            \
  template <class... Ts>                                                       \
  void verbose(std::string_view identifier, std::format_string<Ts...> fmt_str, \
               Ts&&... args) {                                                 \
    detail::do_log(event::severity_level::verbose, component, identifier,      \
                   fmt_str, std::forward<Ts>(args)...);                        \
  }                                                                            \
  template <class... Ts>                                                       \
  void debug(std::string_view identifier, std::format_string<Ts...> fmt_str,   \
             Ts&&... args) {                                                   \
    detail::do_log(event::severity_level::debug, component, identifier,        \
                   fmt_str, std::forward<Ts>(args)...);                        \
  }                                                                            \
  }

BROKER_DECLARE_LOG_COMPONENT(core)

BROKER_DECLARE_LOG_COMPONENT(endpoint)

BROKER_DECLARE_LOG_COMPONENT(store)

BROKER_DECLARE_LOG_COMPONENT(network)

BROKER_DECLARE_LOG_COMPONENT(app)

#undef BROKER_DECLARE_LOG_COMPONENT
