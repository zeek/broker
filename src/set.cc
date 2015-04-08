// Begin C API
#include "broker/broker.h"
#include "broker/data.hh"
using std::nothrow;

broker_set* broker_set_create()
	{ return reinterpret_cast<broker_set*>(new (nothrow) broker::set()); }

void broker_set_delete(broker_set* s)
	{ delete reinterpret_cast<broker::set*>(s); }

broker_set* broker_set_copy(const broker_set* s)
	{
	auto ss = reinterpret_cast<const broker::set*>(s);

	try
		{ return reinterpret_cast<broker_set*>(new broker::set(*ss)); }
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

void broker_set_clear(broker_set* s)
	{
	auto ss = reinterpret_cast<broker::set*>(s);
	ss->clear();
	}

size_t broker_set_size(const broker_set* s)
	{
	auto ss = reinterpret_cast<const broker::set*>(s);
	return ss->size();
	}

int broker_set_contains(const broker_set* s, const broker_data* key)
	{
	auto ss = reinterpret_cast<const broker::set*>(s);
	auto kk = reinterpret_cast<const broker::data*>(key);

	if ( ss->find(*kk) == ss->end() )
		return 0;

	return 1;
	}

int broker_set_insert(broker_set* s, const broker_data* key)
	{
	auto ss = reinterpret_cast<broker::set*>(s);
	auto kk = reinterpret_cast<const broker::data*>(key);

	try
		{ return ss->emplace(*kk).second; }
	catch ( std::bad_alloc& )
		{ return -1; }
	}

int broker_set_remove(broker_set* s, const broker_data* key)
	{
	auto ss = reinterpret_cast<broker::set*>(s);
	auto kk = reinterpret_cast<const broker::data*>(key);
	return ss->erase(*kk);
	}

int broker_set_eq(const broker_set* a, const broker_set* b)
	{
	auto aa = reinterpret_cast<const broker::set*>(a);
	auto bb = reinterpret_cast<const broker::set*>(b);
	return *aa == *bb;
	}

int broker_set_lt(const broker_set* a, const broker_set* b)
	{
	auto aa = reinterpret_cast<const broker::set*>(a);
	auto bb = reinterpret_cast<const broker::set*>(b);
	return *aa < *bb;
	}

size_t broker_set_hash(const broker_set* a)
	{
	auto aa = reinterpret_cast<const broker::set*>(a);
	return std::hash<broker::set>{}(*aa);
	}

broker_set_iterator* broker_set_iterator_create(broker_set* s)
	{
	auto ss = reinterpret_cast<broker::set*>(s);
	auto rval = new (nothrow) broker::set::iterator(ss->begin());
	return reinterpret_cast<broker_set_iterator*>(rval);
	}

void broker_set_iterator_delete(broker_set_iterator* it)
	{ delete reinterpret_cast<broker::set::iterator*>(it); }

int broker_set_iterator_at_last(broker_set* s, broker_set_iterator* it)
	{
	auto ss = reinterpret_cast<broker::set*>(s);
	auto ii = reinterpret_cast<broker::set::iterator*>(it);
	return *ii == ss->end();
	}

int broker_set_iterator_next(broker_set* s, broker_set_iterator* it)
	{
	auto ss = reinterpret_cast<broker::set*>(s);
	auto ii = reinterpret_cast<broker::set::iterator*>(it);
	++(*ii);
	return *ii == ss->end();
	}

const broker_data* broker_set_iterator_value(broker_set_iterator* it)
	{
	auto ii = reinterpret_cast<broker::set::iterator*>(it);
	return reinterpret_cast<const broker_data*>(&**ii);
	}

broker_set_const_iterator* broker_set_const_iterator_create(const broker_set* s)
	{
	auto ss = reinterpret_cast<const broker::set*>(s);
	auto rval = new (nothrow) broker::set::const_iterator(ss->begin());
	return reinterpret_cast<broker_set_const_iterator*>(rval);
	}

void broker_set_const_iterator_delete(broker_set_const_iterator* it)
	{ delete reinterpret_cast<broker::set::const_iterator*>(it); }

int broker_set_const_iterator_at_last(const broker_set* s,
                                      broker_set_const_iterator* it)
	{
	auto ss = reinterpret_cast<const broker::set*>(s);
	auto ii = reinterpret_cast<broker::set::const_iterator*>(it);
	return *ii == ss->end();
	}

int broker_set_const_iterator_next(const broker_set* s,
                                   broker_set_const_iterator* it)
	{
	auto ss = reinterpret_cast<const broker::set*>(s);
	auto ii = reinterpret_cast<broker::set::const_iterator*>(it);
	++(*ii);
	return *ii == ss->end();
	}

const broker_data*
broker_set_const_iterator_value(broker_set_const_iterator* it)
	{
	auto ii = reinterpret_cast<broker::set::const_iterator*>(it);
	return reinterpret_cast<const broker_data*>(&**ii);
	}
