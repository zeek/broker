#include "broker/enum_value.hh"
// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_enum_value* broker_enum_value_create(const char* name)
	{
	try
		{
		auto rval = new broker::enum_value(name);
		return reinterpret_cast<broker_enum_value*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

void broker_enum_value_delete(broker_enum_value* e)
	{ delete reinterpret_cast<broker::enum_value*>(e); }

broker_enum_value* broker_enum_value_copy(const broker_enum_value* e)
	{
	try
		{
		auto ee = reinterpret_cast<const broker::enum_value*>(e);
		auto rval = new broker::enum_value(*ee);
		return reinterpret_cast<broker_enum_value*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

const char* broker_enum_value_name(const broker_enum_value* e)
	{ return reinterpret_cast<const broker::enum_value*>(e)->name.data(); }

int broker_enum_value_set(broker_enum_value* e, const char* name)
	{
	try
		{
		auto ee = reinterpret_cast<broker::enum_value*>(e);
		ee->name = name;
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_enum_value_eq(const broker_enum_value* a,
                         const broker_enum_value* b)
	{
	auto aa = reinterpret_cast<const broker::enum_value*>(a);
	auto bb = reinterpret_cast<const broker::enum_value*>(b);
	return *aa == *bb;
	}

int broker_enum_value_lt(const broker_enum_value* a,
                         const broker_enum_value* b)
	{
	auto aa = reinterpret_cast<const broker::enum_value*>(a);
	auto bb = reinterpret_cast<const broker::enum_value*>(b);
	return *aa < *bb;
	}

size_t broker_enum_value_hash(const broker_enum_value* p)
	{
	auto pp = reinterpret_cast<const broker::enum_value*>(p);
	return std::hash<broker::enum_value>{}(*pp);
	}
