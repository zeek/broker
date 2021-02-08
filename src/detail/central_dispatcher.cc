#include "broker/detail/central_dispatcher.hh"

#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker::detail {

central_dispatcher::central_dispatcher(caf::scheduled_actor* self)
  : self_(self) {
  // nop
}

void central_dispatcher::enqueue(const unipath_manager* source,
                                 item_scope scope,
                                 caf::span<const node_message> xs) {
  BROKER_DEBUG("central enqueue" << BROKER_ARG(scope)
                                 << BROKER_ARG2("xs.size", xs.size()));
  auto f = [&](auto& sink) { return !sink->enqueue(source, scope, xs); };
  sinks_.erase(std::remove_if(sinks_.begin(), sinks_.end(), f), sinks_.end());
}

void central_dispatcher::add(unipath_manager_ptr sink) {
  sinks_.emplace_back(std::move(sink));
}

} // namespace broker::detail
