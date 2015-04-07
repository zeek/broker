#include "broker/outgoing_connection_status.hh"
// Begin C API
#include "broker/broker.h"

const broker_peering*
broker_outgoing_connection_status_peering(const broker_outgoing_connection_status* s)
	{
	auto ss = reinterpret_cast<const broker::outgoing_connection_status*>(s);
	auto rval = &ss->relation;
	return reinterpret_cast<const broker_peering*>(rval);
	}

broker_outgoing_connection_status_tag
broker_outgoing_connection_status_get(const broker_outgoing_connection_status* s)
	{
	auto ss = reinterpret_cast<const broker::outgoing_connection_status*>(s);
	return static_cast<broker_outgoing_connection_status_tag>(ss->status);
	}

const char*
broker_outgoing_connection_status_peer_name(const broker_outgoing_connection_status* s)
	{
	auto ss = reinterpret_cast<const broker::outgoing_connection_status*>(s);
	return ss->peer_name.data();
	}
