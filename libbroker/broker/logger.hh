#pragma once

#include "broker/event_observer.hh"

namespace broker {

/// Returns the global logger.
event_observer* logger() noexcept;

/// Sets the global logger.
/// @note This function is not thread-safe and should be called only once at
///       program startup.
void logger(event_observer_ptr ptr) noexcept;

} // namespace broker
