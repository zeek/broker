#include "broker/subscriber.hh"

namespace broker {
namespace detail {

shared_queue::shared_queue() : pending(0) {
  // nop
}

shared_queue_ptr make_shared_queue() {
  return caf::make_counted<shared_queue>();
}

} // namespace detail
} // namespace broker
