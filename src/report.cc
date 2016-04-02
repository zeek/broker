#include <mutex>

#include "broker/report.hh"
#include "broker/time_point.hh"

namespace broker {
namespace report {

extern std::mutex* mtx;
endpoint* manager;
message_queue* default_queue;

} // namespace report

static inline double now() {
  return time_point::now().value;
}

static std::string to_string(report::level lvl) {
  using report::level;
  switch (lvl) {
    case level::debug:
      return "debug";
    case level::info:
      return "info";
    case level::warn:
      return "warn";
    case level::error:
      return "error";
    default:
      return "<unknown>";
  }
}

int report::init(bool with_default_queue) {
  std::unique_lock<std::mutex> guard{*mtx};
  manager = new endpoint("broker.reports");
  if (with_default_queue)
    default_queue = new message_queue("broker.report.", *manager);
  return 0;
}

void report::done() {
  std::unique_lock<std::mutex> guard{*mtx};
  delete manager;
  manager = nullptr;
  delete default_queue;
  default_queue = nullptr;
}

void report::send(level lvl, topic subtopic, std::string msg) {
  std::unique_lock<std::mutex> guard{*mtx};
  if (!manager)
    return;
  topic full_topic = "broker.report." + to_string(lvl) + "." + subtopic;
  message full_message{now(), +lvl, std::move(subtopic), std::move(msg)};
  manager->send(std::move(full_topic), std::move(full_message));
}

void report::info(topic subtopic, std::string msg) {
  send(level::info, std::move(subtopic), std::move(msg));
}

void report::warn(topic subtopic, std::string msg) {
  send(level::warn, std::move(subtopic), std::move(msg));
}

void report::error(topic subtopic, std::string msg) {
  send(level::error, std::move(subtopic), std::move(msg));
}

} // namespace broker

// Begin C API
#include "broker/broker.h"

broker_endpoint* broker_report_manager() {
  return reinterpret_cast<broker_endpoint*>(broker::report::manager);
}

broker_message_queue* broker_report_default_queue() {
  return reinterpret_cast<broker_message_queue*>(broker::report::default_queue);
}

int broker_report_init(int with_default_queue) {
  return broker::report::init(with_default_queue);
}

void broker_report_done() {
  broker::report::done();
}

int broker_report_send(broker_report_level lvl, const broker_string* subtopic,
                       const broker_string* msg) {
  auto tt = reinterpret_cast<const std::string*>(subtopic);
  auto mm = reinterpret_cast<const std::string*>(msg);
  try {
    auto l = static_cast<broker::report::level>(lvl);
    broker::topic t(*tt);
    std::string m(*mm);
    broker::report::send(l, std::move(t), std::move(m));
  } catch (std::bad_alloc&) {
    return 0;
  }
  return 1;
}
