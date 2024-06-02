#pragma once

#include "broker/event.hh"
#include "broker/event_observer.hh"

#include <string_view>

namespace broker {

/// Returns the global logger.
event_observer* logger() noexcept;

/// Sets the global logger.
/// @note This function is not thread-safe and should be called only once at
///       program startup.
void logger(event_observer_ptr ptr) noexcept;

/// Creates a new logger that prints to the console.
event_observer_ptr make_console_logger(event::severity_level severity);

/// Creates a new logger that prints to the console.
/// @param severity The minimum severity level to log as string. One of:
///                 "critical", "error", "warning", "info", or "debug".
/// @throws std::invalid_argument if `severity` is not a valid severity level.
event_observer_ptr make_console_logger(std::string_view severity);

/// Convenience function for setting a console logger.
inline void set_console_logger(event::severity_level severity) {
  logger(make_console_logger(severity));
}

/// Convenience function for setting a console logger.
inline void set_console_logger(std::string_view severity) {
  logger(make_console_logger(severity));
}

} // namespace broker
