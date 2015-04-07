#include "broker/store/expiration_time.hh"
// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_store_expiration_time*
broker_store_expiration_time_create_relative(double interval, double time)
	{
	auto rval = new (nothrow) broker::store::expiration_time(interval, time);
	return reinterpret_cast<broker_store_expiration_time*>(rval);
	}

broker_store_expiration_time*
broker_store_expiration_time_create_absolute(double time)
	{
	auto rval = new (nothrow) broker::store::expiration_time(time);
	return reinterpret_cast<broker_store_expiration_time*>(rval);
	}

void broker_store_expiration_time_delete(broker_store_expiration_time* t)
	{
	delete reinterpret_cast<broker::store::expiration_time*>(t);
	}

broker_store_expiration_time_tag
broker_store_expiration_time_get_tag(const broker_store_expiration_time* t)
	{
	auto tt = reinterpret_cast<const broker::store::expiration_time*>(t);
	return static_cast<broker_store_expiration_time_tag>(tt->type);
	}

double* broker_store_expiration_time_get(broker_store_expiration_time* t)
	{
	auto tt = reinterpret_cast<broker::store::expiration_time*>(t);
	return &tt->expiry_time;
	}

double* broker_store_expiration_time_last_mod(broker_store_expiration_time* t)
	{
	auto tt = reinterpret_cast<broker::store::expiration_time*>(t);
	return &tt->modification_time;
	}

int broker_store_expiration_time_eq(const broker_store_expiration_time* a,
                                    const broker_store_expiration_time* b)
	{
	auto aa = reinterpret_cast<const broker::store::expiration_time*>(a);
	auto bb = reinterpret_cast<const broker::store::expiration_time*>(b);
	return *aa == *bb;
	}
