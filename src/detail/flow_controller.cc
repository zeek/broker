#include "broker/detail/flow_controller.hh"

#include "broker/detail/shared_publisher_queue.hh"
#include "broker/detail/shared_publisher_queue_adapter.hh"
#include "broker/detail/shared_subscriber_queue.hh"
#include "broker/detail/shared_subscriber_queue_adapter.hh"

namespace broker::detail {

namespace {

// TODO: make these constant(s) configurable

/// Defines how many items are stored in the queue.
constexpr size_t queue_size = 64;

} // namespace <anonymous>

flow_controller::~flow_controller() {
  // nop
}

void flow_controller::connect(source_queue_promise_ptr<data_message> promise) {
  BROKER_ASSERT(promise != nullptr);
  using adapter_type = detail::shared_publisher_queue_adapter<data_message>;
  auto self = ctx();
  auto queue = caf::make_counted<source_queue<data_message>>(queue_size);
  auto adapter = caf::make_counted<adapter_type>(self, queue);
  queue->init(self->to_async_notifiable(adapter));
  add_source(adapter->as_observable());
  promise->set_value(std::move(queue));
}

void flow_controller::connect(sink_queue_promise_ptr<data_message> promise,
                              const filter_type& filter) {
  BROKER_ASSERT(promise != nullptr);
  using adapter_type = detail::shared_subscriber_queue_adapter<data_message>;
  auto self = ctx();
  auto queue = caf::make_counted<sink_queue<data_message>>(queue_size);
  add_filter(filter);
  queue->filter(filter);
  auto adapter = caf::make_counted<adapter_type>(self, queue,
                                                 std::move(promise));
  add_sink(adapter->as_observer());
}

void flow_controller::update_filter(sink_queue_ptr<data_message> queue,
                                    const filter_type& filter) {
  queue->filter(filter);
  add_filter(filter);
}

} // namespace broker::detail
