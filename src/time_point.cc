#include "broker/time_point.hh"
#include <chrono>

broker::time_point broker::time_point::now()
	{
	using namespace std::chrono;
	auto d = system_clock::now().time_since_epoch();
	return time_point{duration_cast<duration<double>>(d).count()};
	}

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_time_point* broker_time_point_create(double seconds_from_epoch)
	{
	return reinterpret_cast<broker_time_point*>(
	            new (nothrow) broker::time_point(seconds_from_epoch));
	}

void broker_time_point_delete(broker_time_point* t)
	{ delete reinterpret_cast<broker::time_point*>(t); }

broker_time_point* broker_time_point_copy(const broker_time_point* t)
	{
	auto tt = reinterpret_cast<const broker::time_point*>(t);
	return reinterpret_cast<broker_time_point*>(
	            new (nothrow) broker::time_point(*tt));
	}

broker_time_point* broker_time_point_now()
	{
	auto rval = new (nothrow) broker::time_point(broker::time_point::now());
	return reinterpret_cast<broker_time_point*>(rval);
	}

double broker_time_point_value(const broker_time_point* t)
	{ return reinterpret_cast<const broker::time_point*>(t)->value; }

void broker_time_point_set(broker_time_point* t, double seconds_from_epoch)
	{
	auto tt = reinterpret_cast<broker::time_point*>(t);
	*tt = broker::time_point(seconds_from_epoch);
	}

int broker_time_point_eq(const broker_time_point* a,
                         const broker_time_point* b)
	{
	auto aa = reinterpret_cast<const broker::time_point*>(a);
	auto bb = reinterpret_cast<const broker::time_point*>(b);
	return *aa == *bb;
	}

int broker_time_point_lt(const broker_time_point* a,
                         const broker_time_point* b)
	{
	auto aa = reinterpret_cast<const broker::time_point*>(a);
	auto bb = reinterpret_cast<const broker::time_point*>(b);
	return *aa < *bb;
	}

size_t broker_time_point_hash(const broker_time_point* p)
	{
	auto tt = reinterpret_cast<const broker::time_point*>(p);
	return std::hash<broker::time_point>{}(*tt);
	}
