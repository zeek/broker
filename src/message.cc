#include "broker/message.hh"

#include <caf/deep_to_string.hpp>

namespace broker {

node_message make_node_message(node_message_content content,
                               const std::vector<endpoint_id>& path,
                               std::vector<endpoint_id> receivers) {
  BROKER_ASSERT(!path.empty());
  return node_message{std::move(content),
                      alm::multipath<endpoint_id>{path.begin(), path.end()},
                      std::move(receivers)};
}

node_message make_node_message(node_message_content content,
                               const std::vector<endpoint_id>& path,
                               endpoint_id receiver) {
  std::vector<endpoint_id> receivers;
  receivers.emplace_back(std::move(receiver));
  return node_message{std::move(content),
                      alm::multipath<endpoint_id>{path.begin(), path.end()},
                      std::move(receivers)};
}

bool addressed_to(const node_message& msg, const caf::strong_actor_ptr& ptr) {
  if (ptr) {
    auto& node = ptr->node();
    for (auto receiver : get_receivers(msg))
      if (receiver == node)
        return true;
  }
  return false;
}

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
