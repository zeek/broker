#include "broker/store/response.hh"
// Begin C API
#include "broker/broker.h"

broker_store_query*
broker_store_response_get_query(broker_store_response* r)
	{
	auto rr = reinterpret_cast<broker::store::response*>(r);
	return reinterpret_cast<broker_store_query*>(&rr->request);
	}

broker_store_result*
broker_store_response_get_result(broker_store_response* r)
	{
	auto rr = reinterpret_cast<broker::store::response*>(r);
	return reinterpret_cast<broker_store_result*>(&rr->reply);
	}

void* broker_store_response_get_cookie(broker_store_response* r)
	{
	auto rr = reinterpret_cast<broker::store::response*>(r);
	return rr->cookie;
	}
