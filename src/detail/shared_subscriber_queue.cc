#include "broker/detail/shared_subscriber_queue.hh"

namespace broker {
namespace detail {

shared_subscriber_queue::~shared_subscriber_queue() {
  // nop
}

shared_subscriber_queue_ptr make_shared_subscriber_queue() {
  return caf::make_counted<shared_subscriber_queue>();
}

} // namespace detail
} // namespace broker
