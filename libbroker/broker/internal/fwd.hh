#include "broker/fwd.hh"

#include <caf/async/fwd.hpp>

#include <memory>

namespace broker::internal {

enum class connector_event_id : uint64_t;

struct retry_state;

class central_dispatcher;
class flare_actor;
class pending_connection;
class unipath_manager;

using command_consumer_res = caf::async::consumer_resource<command_message>;
using command_producer_res = caf::async::producer_resource<command_message>;
using data_consumer_res = caf::async::consumer_resource<data_message>;
using data_producer_res = caf::async::producer_resource<data_message>;
using node_consumer_res = caf::async::consumer_resource<node_message>;
using node_producer_res = caf::async::producer_resource<node_message>;
using pending_connection_ptr = std::shared_ptr<pending_connection>;

} // namespace broker::internal
