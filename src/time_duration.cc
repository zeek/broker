#include "broker/time_duration.hh"
// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_time_duration* broker_time_duration_create(double seconds)
	{
	return reinterpret_cast<broker_time_duration*>(
	            new (nothrow) broker::time_duration(seconds));
	}

void broker_time_duration_delete(broker_time_duration* t)
	{ delete reinterpret_cast<broker::time_duration*>(t); }

broker_time_duration* broker_time_duration_copy(const broker_time_duration* t)
	{
	auto tt = reinterpret_cast<const broker::time_duration*>(t);
	return reinterpret_cast<broker_time_duration*>(
	            new (nothrow) broker::time_duration(*tt));
	}

double broker_time_duration_value(const broker_time_duration* t)
	{ return reinterpret_cast<const broker::time_duration*>(t)->value; }

void broker_time_duration_set(broker_time_duration* t, double seconds)
	{
	auto tt = reinterpret_cast<broker::time_duration*>(t);
	*tt = broker::time_duration(seconds);
	}

int broker_time_duration_eq(const broker_time_duration* a,
                            const broker_time_duration* b)
	{
	auto aa = reinterpret_cast<const broker::time_duration*>(a);
	auto bb = reinterpret_cast<const broker::time_duration*>(b);
	return *aa == *bb;
	}

int broker_time_duration_lt(const broker_time_duration* a,
                            const broker_time_duration* b)
	{
	auto aa = reinterpret_cast<const broker::time_duration*>(a);
	auto bb = reinterpret_cast<const broker::time_duration*>(b);
	return *aa < *bb;
	}

size_t broker_time_duration_hash(const broker_time_duration* p)
	{
	auto tt = reinterpret_cast<const broker::time_duration*>(p);
	return std::hash<broker::time_duration>{}(*tt);
	}
