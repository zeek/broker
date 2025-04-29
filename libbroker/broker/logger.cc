#include "broker/logger.hh"

#include "broker/time.hh"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>

#include <caf/term.hpp>

namespace broker {

namespace {

event_observer_ptr global_observer;

} // namespace

event_observer* logger() noexcept {
  return global_observer.get();
}

void logger(event_observer_ptr ptr) noexcept {
  global_observer = std::move(ptr);
}

} // namespace broker
