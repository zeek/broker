// Begin C API (simple wrapper of std::string)
#include "broker/broker.h"
#include <string>

broker_string* broker_string_create(const char* cstring)
	{
	try
		{ return reinterpret_cast<broker_string*>(new std::string(cstring)); }
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

void broker_string_delete(broker_string* s)
	{ delete reinterpret_cast<std::string*>(s); }

broker_string* broker_string_copy(const broker_string* s)
	{
	auto ss = reinterpret_cast<const std::string*>(s);

	try
		{ return reinterpret_cast<broker_string*>(new std::string(*ss)); }
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

broker_string* broker_string_from_data(const char* s, size_t len)
	{
	try
		{ return reinterpret_cast<broker_string*>(new std::string(s, len)); }
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

int broker_string_set_cstring(broker_string* s, const char* cstr)
	{
	try
		{
		*reinterpret_cast<std::string*>(s)= std::string(cstr);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_string_set_data(broker_string* s, const char* data, size_t len)
	{
	try
		{
		*reinterpret_cast<std::string*>(s)= std::string(data, len);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

const char* broker_string_data(const broker_string* s)
	{ return reinterpret_cast<const std::string*>(s)->data(); }

size_t broker_string_size(const broker_string* s)
	{ return reinterpret_cast<const std::string*>(s)->size(); }

int broker_string_eq(const broker_string* a, const broker_string* b)
	{
	auto aa = reinterpret_cast<const std::string*>(a);
	auto bb = reinterpret_cast<const std::string*>(b);
	return *aa == *bb;
	}

int broker_string_lt(const broker_string* a, const broker_string* b)
	{
	auto aa = reinterpret_cast<const std::string*>(a);
	auto bb = reinterpret_cast<const std::string*>(b);
	return *aa < *bb;
	}

size_t broker_string_hash(const broker_string* s)
	{
	auto ss = reinterpret_cast<const std::string*>(s);
	return std::hash<std::string>{}(*ss);
	}
