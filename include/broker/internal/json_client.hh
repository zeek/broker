#pragma once

#include "broker/endpoint_id.hh"
#include "broker/filter_type.hh"
#include "broker/internal/json_type_mapper.hh"
#include "broker/message.hh"
#include "broker/network_info.hh"

#include <caf/actor.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/fwd.hpp>
#include <caf/json_reader.hpp>
#include <caf/json_writer.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/scheduler/test_coordinator.hpp>

#include <string>
#include <string_view>

namespace broker::internal {

class json_client_state {
public:
  static inline const char* name = "broker.json-client";

  using in_t = caf::async::consumer_resource<caf::cow_string>;

  using out_t = caf::async::producer_resource<caf::cow_string>;

  json_client_state(caf::event_based_actor* selfptr, endpoint_id this_node,
                    caf::actor core, network_info addr, in_t in, out_t out);

  ~json_client_state();

  template <class T>
  std::string render(const T& x) {
    writer.reset();
    if (writer.apply(x)) {
      return to_string(writer.str());
    } else {
      return std::string{default_serialization_failed_error()};
    }
  }

  std::string render_error(std::string_view code, std::string_view context);

  std::string render_ack();

  void on_down_msg(const caf::down_msg& msg);

  caf::event_based_actor* self;
  endpoint_id id;
  caf::actor core;
  network_info addr;
  json_type_mapper mapper;
  caf::json_reader reader;
  caf::json_writer writer;
  std::vector<caf::disposable> subscriptions;
  caf::flow::item_publisher<caf::cow_string> ctrl_msgs;

  static std::string_view default_serialization_failed_error();

  void init(const filter_type& filter, const out_t& out,
            caf::async::consumer_resource<data_message> core_pull);
};

using json_client_actor = caf::stateful_actor<json_client_state>;

} // namespace broker::internal
