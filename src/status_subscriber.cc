#include "broker/status_subscriber.hh"

#include <limits>

#include <caf/event_based_actor.hpp>
#include <caf/send.hpp>

#include "broker/endpoint.hh"
#include "broker/internal/logger.hh"

#define BROKER_RETURN_CONVERTED_MSG()                                          \
  auto& t = get_topic(msg);                                                    \
  if (t == topic::errors_str) {                                                \
    if (auto value = to<error>(get_data(msg)))                                 \
      return value_type{std::move(*value)};                                    \
    BROKER_WARNING("received malformed error");                                \
  } else {                                                                     \
    if (auto value = to<status>(get_data(msg)))                                \
      return value_type{std::move(*value)};                                    \
    BROKER_WARNING("received malformed status");                               \
  }

#define BROKER_APPEND_CONVERTED_MSG()                                          \
  auto& t = get_topic(msg);                                                    \
  if (t == topic::errors_str) {                                                \
    if (auto value = to<error>(get_data(msg)))                                 \
      result.emplace_back(std::move(*value));                                  \
    else                                                                       \
      BROKER_WARNING("received malformed error");                              \
  } else {                                                                     \
    if (auto value = to<status>(get_data(msg)))                                \
      result.emplace_back(std::move(*value));                                  \
    else                                                                       \
      BROKER_WARNING("received malformed status");                             \
  }

using namespace caf;

namespace broker {

namespace {

std::vector<topic> make_status_topics(bool receive_statuses) {
  std::vector<topic> result;
  result.reserve(2);
  result.emplace_back(topic::errors());
  if (receive_statuses)
    result.emplace_back(topic::statuses());
  return result;
}

using value_type = status_subscriber::value_type;

} // namespace

status_subscriber::status_subscriber(subscriber impl) : impl_(std::move(impl)) {
  // nop
}

status_subscriber status_subscriber::make(endpoint& ep, bool receive_statuses,
                                          size_t queue_size) {
  return status_subscriber{
    subscriber::make(ep, make_status_topics(receive_statuses), queue_size)};
}

value_type status_subscriber::get(caf::timestamp timeout) {
  if (auto maybe_msg = impl_.get(timeout)) {
    auto msg = std::move(*maybe_msg);
    BROKER_RETURN_CONVERTED_MSG()
  }
  return nil;
}

std::vector<value_type> status_subscriber::get(size_t num,
                                               caf::timestamp timeout) {
  std::vector<value_type> result;
  auto msgs = impl_.get(num, timeout);
  for (auto& msg : msgs) {
    BROKER_APPEND_CONVERTED_MSG();
  }
  return result;
}

std::vector<value_type> status_subscriber::poll() {
  std::vector<value_type> result;
  auto msgs = impl_.poll();
  for (auto& msg : msgs) {
    BROKER_APPEND_CONVERTED_MSG();
  }
  return result;
}

void status_subscriber::append_converted(std::vector<value_type>& result,
                                         const data_message& msg){
  BROKER_APPEND_CONVERTED_MSG()}

value_type status_subscriber::convert(const data_message& msg) {
  BROKER_RETURN_CONVERTED_MSG()
  return nil;
}

void status_subscriber::reset() {
  return impl_.reset();
}

} // namespace broker
