#include "broker/detail/central_dispatcher.hh"

#include "broker/detail/overload.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker::detail {

central_dispatcher::~central_dispatcher() {
  // nop
}

void central_dispatcher::dispatch(const node_message_content& msg) {
  if (is_data_message(msg))
    dispatch(get_data_message(msg));
  else
    dispatch(get_command_message(msg));
}

} // namespace broker::detail
