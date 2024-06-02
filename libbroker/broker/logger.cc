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

constexpr auto severity_color(event::severity_level level) {
  switch (level) {
    default: // critical and error
      return caf::term::red;
    case event::severity_level::warning:
      return caf::term::yellow;
    case event::severity_level::info:
      return caf::term::green;
    case event::severity_level::debug:
      return caf::term::blue;
  };
}

constexpr auto severity_name(event::severity_level level) {
  switch (level) {
    default:
      return "?????";
    case event::severity_level::critical:
      return "CRIT ";
    case event::severity_level::error:
      return "ERROR";
    case event::severity_level::warning:
      return "WARN ";
    case event::severity_level::info:
      return "INFO ";
    case event::severity_level::debug:
      return "DEBUG";
  };
}

class console_logger : public event_observer {
public:
  explicit console_logger(event::severity_level severity)
    : severity_(severity) {
    // nop
  }

  void observe(event_ptr what) override {
    // Split timestamp into seconds and milliseconds.
    namespace sc = std::chrono;
    auto sys_time = sc::time_point_cast<clock::duration>(what->timestamp);
    auto secs = sc::system_clock::to_time_t(sys_time);
    auto msecs = (what->timestamp.time_since_epoch().count() / 1'000) % 1'000;
    auto msecs_str = std::to_string(msecs);
    // Pad milliseconds to three digits.
    if (msecs < 10) {
      msecs_str.insert(msecs_str.begin(), 2, '0');
    } else if (msecs < 100) {
      msecs_str.insert(msecs_str.begin(), 1, '0');
    }
    // Critical section to avoid interleaved output.
    std::lock_guard guard{mtx_};
    std::cout << severity_color(what->severity)
              << std::put_time(std::localtime(&secs), "%F %T") << '.'
              << msecs_str << ' ' << severity_name(what->severity) << ' '
              << what->description << caf::term::reset << std::endl;
  }

  bool accepts(event::severity_level severity,
               event::component_type) const override {
    return severity >= severity_;
  }

private:
  std::mutex mtx_;
  event::severity_level severity_;
};

} // namespace

event_observer* logger() noexcept {
  return global_observer.get();
}

void logger(event_observer_ptr ptr) noexcept {
  global_observer = std::move(ptr);
}

event_observer_ptr make_console_logger(event::severity_level severity) {
  return std::make_shared<console_logger>(severity);
}

event_observer_ptr make_console_logger(std::string_view severity) {
  if (severity == "critical") {
    return make_console_logger(event::severity_level::critical);
  }
  if (severity == "error") {
    return make_console_logger(event::severity_level::error);
  }
  if (severity == "warning") {
    return make_console_logger(event::severity_level::warning);
  }
  if (severity == "info") {
    return make_console_logger(event::severity_level::info);
  }
  if (severity == "debug") {
    return make_console_logger(event::severity_level::debug);
  }
  throw std::invalid_argument("invalid severity level");
}

} // namespace broker
