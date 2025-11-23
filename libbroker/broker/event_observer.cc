#include "broker/event_observer.hh"

namespace broker {

event_observer::~event_observer() {}

void event_observer::on_peer_connect(const endpoint_id&, const network_info&) {}

void event_observer::on_peer_buffer_push(const endpoint_id&, size_t) {}

void event_observer::on_peer_buffer_pull(const endpoint_id&, size_t) {}

void event_observer::on_peer_disconnect(const endpoint_id&, const error&) {}

void event_observer::on_client_connect(const endpoint_id&,
                                       const network_info&) {}

void event_observer::on_client_buffer_push(const endpoint_id&, size_t) {}

void event_observer::on_client_buffer_pull(const endpoint_id&, size_t) {}

void event_observer::on_client_disconnect(const endpoint_id&, const error&) {}

} // namespace broker
