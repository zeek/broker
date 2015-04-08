#include "broker/store/result.hh"
// Begin C API
#include "broker/broker.h"

void broker_store_result_delete(broker_store_result* r)
	{
	delete reinterpret_cast<broker::store::result*>(r);
	}

broker_store_result_status
broker_store_result_get_status(const broker_store_result* r)
	{
	auto rr = reinterpret_cast<const broker::store::result*>(r);
	return static_cast<broker_store_result_status>(rr->stat);
	}

broker_store_result_tag
broker_store_result_which(const broker_store_result* r)
	{
	auto rr = reinterpret_cast<const broker::store::result*>(r);
	return static_cast<broker_store_result_tag>(broker::which(rr->value));
	}

int broker_store_result_bool(const broker_store_result* r)
	{
	auto rr = reinterpret_cast<const broker::store::result*>(r);
	return *broker::get<bool>(rr->value);
	}

uint64_t broker_store_result_count(const broker_store_result* r)
	{
	auto rr = reinterpret_cast<const broker::store::result*>(r);
	return *broker::get<uint64_t>(rr->value);
	}

const broker_data* broker_store_result_data(const broker_store_result* r)
	{
	auto rr = reinterpret_cast<const broker::store::result*>(r);
	return reinterpret_cast<const broker_data*>(
	            broker::get<broker::data>(rr->value));
	}

const broker_vector* broker_store_result_vector(const broker_store_result* r)
	{
	auto rr = reinterpret_cast<const broker::store::result*>(r);
	return reinterpret_cast<const broker_vector*>(
	            broker::get<broker::vector>(rr->value));
	}
