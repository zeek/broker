#include "broker/incoming_connection_status.hh"
// Begin C API
#include "broker/broker.h"

broker_incoming_connection_status_tag
broker_incoming_connection_status_get(const broker_incoming_connection_status* s)
	{
	auto ss = reinterpret_cast<const broker::incoming_connection_status*>(s);
	return static_cast<broker_incoming_connection_status_tag>(ss->status);
	}

const char*
broker_incoming_connection_status_peer_name(const broker_incoming_connection_status* s)
	{
	auto ss = reinterpret_cast<const broker::incoming_connection_status*>(s);
	return ss->peer_name.data();
	}
