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

constexpr caf::term severity_color(event::severity_level level) {
  switch (level) {
    default: // critical and error
      return caf::term::red;
    case event::severity_level::warning:
      return caf::term::yellow;
    case event::severity_level::info:
      return caf::term::green;
    case event::severity_level::verbose:
      return caf::term::blue;
    case event::severity_level::debug:
      return caf::term::cyan;
  };
}

constexpr const char* component_tag(event::component_type component) {
  switch (component) {
    default:
      return "[???]";
    case event::component_type::core:
      return "[core]";
    case event::component_type::endpoint:
      return "[endpoint]";
    case event::component_type::store:
      return "[store]";
    case event::component_type::network:
      return "[network]";
    case event::component_type::app:
      return "[app]";
  };
}

constexpr const char* severity_name(event::severity_level level) {
  switch (level) {
    default:
      return "?????";
    case event::severity_level::critical:
      return "CRITICAL";
    case event::severity_level::error:
      return "ERROR";
    case event::severity_level::warning:
      return "WARNING";
    case event::severity_level::info:
      return "INFO";
    case event::severity_level::verbose:
      return "VERBOSE";
    case event::severity_level::debug:
      return "DEBUG";
  };
}

class console_logger : public event_observer {
public:
  console_logger(event::severity_level severity, event::component_mask mask)
    : severity_(severity), mask_(mask) {
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
              << component_tag(what->component) << ' ' << what->description
              << caf::term::reset << std::endl;
  }

  bool accepts(event::severity_level severity,
               event::component_type component) const override {
    return severity <= severity_ && has_component(mask_, component);
  }

private:
  std::mutex mtx_;
  event::severity_level severity_;
  event::component_mask mask_;
};

} // namespace

event_observer* logger() noexcept {
  return global_observer.get();
}

void logger(event_observer_ptr ptr) noexcept {
  global_observer = std::move(ptr);
}

event_observer_ptr make_console_logger(event::severity_level severity,
                                       event::component_mask mask) {
  return std::make_shared<console_logger>(severity, mask);
}

event_observer_ptr make_console_logger(std::string_view severity,
                                       event::component_mask mask) {
  if (severity == "critical") {
    return make_console_logger(event::severity_level::critical, mask);
  }
  if (severity == "error") {
    return make_console_logger(event::severity_level::error, mask);
  }
  if (severity == "warning") {
    return make_console_logger(event::severity_level::warning, mask);
  }
  if (severity == "info") {
    return make_console_logger(event::severity_level::info, mask);
  }
  if (severity == "debug") {
    return make_console_logger(event::severity_level::debug, mask);
  }
  throw std::invalid_argument("invalid severity level");
}

} // namespace broker
