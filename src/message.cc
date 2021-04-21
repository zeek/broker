#include "broker/message.hh"

#include <caf/deep_to_string.hpp>

namespace broker {

std::string to_string(const data_message& msg) {
  return caf::deep_to_string(msg.data());
}

std::string to_string(const command_message& msg) {
  return caf::deep_to_string(msg.data());
}

std::string to_string(const node_message& msg) {
  return caf::deep_to_string(msg.data());
}

} // namespace broker

namespace broker::detail{

namespace {

thread_local topic_cache_type topic_cache;

thread_local path_cache_type path_cache;

thread_local content_buf_type content_buf;

} // namespace

topic_cache_type& thread_local_topic_cache() {
  return topic_cache;
}

path_cache_type& thread_local_path_cache() {
  return path_cache;
}

content_buf_type& thread_local_content_buf() {
  return content_buf;
}

} // namespace broker::detail
